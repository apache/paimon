/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.hive.HiveTypeUtils.toPaimonType;
import static org.apache.paimon.utils.FileUtils.COMMON_IO_FORK_JOIN_POOL;

/** Migrate hive table to paimon table. */
public class HiveMigrator implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(HiveMigrator.class);

    private static final Predicate<FileStatus> HIDDEN_PATH_FILTER =
            p -> !p.getPath().getName().startsWith("_") && !p.getPath().getName().startsWith(".");

    private static final String PAIMON_SUFFIX = "_paimon_";

    private final FileIO fileIO;
    private final HiveCatalog hiveCatalog;
    private final IMetaStoreClient client;
    private final String sourceDatabase;
    private final String sourceTable;
    private final String targetDatabase;
    private final String targetTable;
    private final Map<String, String> options;

    public HiveMigrator(
            HiveCatalog hiveCatalog,
            String sourceDatabase,
            String sourceTable,
            String targetDatabase,
            String targetTable,
            Map<String, String> options) {
        this.hiveCatalog = hiveCatalog;
        this.fileIO = hiveCatalog.fileIO();
        this.client = hiveCatalog.getHmsClient();
        this.sourceDatabase = sourceDatabase;
        this.sourceTable = sourceTable;
        this.targetDatabase = targetDatabase;
        this.targetTable = targetTable;
        this.options = options;
    }

    public static List<Migrator> databaseMigrators(
            HiveCatalog hiveCatalog, String sourceDatabase, Map<String, String> options) {
        IMetaStoreClient client = hiveCatalog.getHmsClient();
        try {
            return client.getAllTables(sourceDatabase).stream()
                    .map(
                            sourceTable ->
                                    new HiveMigrator(
                                            hiveCatalog,
                                            sourceDatabase,
                                            sourceTable,
                                            sourceDatabase,
                                            sourceTable + PAIMON_SUFFIX,
                                            options))
                    .collect(Collectors.toList());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void executeMigrate() throws Exception {
        if (!client.tableExists(sourceDatabase, sourceTable)) {
            throw new RuntimeException("Source hive table does not exist");
        }

        Table sourceHiveTable = client.getTable(sourceDatabase, sourceTable);
        Map<String, String> properties = new HashMap<>(sourceHiveTable.getParameters());
        checkPrimaryKey();

        // create paimon table if not exists
        Identifier identifier = Identifier.create(targetDatabase, targetTable);
        boolean alreadyExist = hiveCatalog.tableExists(identifier);
        if (!alreadyExist) {
            Schema schema =
                    from(
                            client.getSchema(sourceDatabase, sourceTable),
                            sourceHiveTable.getPartitionKeys(),
                            properties);
            hiveCatalog.createTable(identifier, schema, false);
        }

        try {
            FileStoreTable paimonTable = (FileStoreTable) hiveCatalog.getTable(identifier);
            checkPaimonTable(paimonTable);

            List<String> partitionsNames =
                    client.listPartitionNames(sourceDatabase, sourceTable, Short.MAX_VALUE);
            checkCompatible(sourceHiveTable, paimonTable);

            List<MigrateTask> tasks = new ArrayList<>();
            Map<Path, Path> rollBack = new ConcurrentHashMap<>();
            if (partitionsNames.isEmpty()) {
                tasks.add(
                        importUnPartitionedTableTask(
                                fileIO, sourceHiveTable, paimonTable, rollBack));
            } else {
                tasks.addAll(
                        importPartitionedTableTask(
                                client,
                                fileIO,
                                partitionsNames,
                                sourceHiveTable,
                                paimonTable,
                                rollBack));
            }

            List<Future<CommitMessage>> futures =
                    tasks.stream()
                            .map(COMMON_IO_FORK_JOIN_POOL::submit)
                            .collect(Collectors.toList());
            List<CommitMessage> commitMessages = new ArrayList<>();
            try {
                for (Future<CommitMessage> future : futures) {
                    commitMessages.add(future.get());
                }
            } catch (Exception e) {
                futures.forEach(f -> f.cancel(true));
                for (Future<?> future : futures) {
                    // wait all task cancelled or finished
                    while (!future.isDone()) {
                        //noinspection BusyWait
                        Thread.sleep(100);
                    }
                }
                // roll back all renamed path
                for (Map.Entry<Path, Path> entry : rollBack.entrySet()) {
                    Path newPath = entry.getKey();
                    Path origin = entry.getValue();
                    if (fileIO.exists(newPath)) {
                        fileIO.rename(newPath, origin);
                    }
                }

                throw new RuntimeException("Migrating failed because exception happens", e);
            }
            try (BatchTableCommit commit = paimonTable.newBatchWriteBuilder().newCommit()) {
                commit.commit(new ArrayList<>(commitMessages));
            }
        } catch (Exception e) {
            if (!alreadyExist) {
                hiveCatalog.dropTable(identifier, true);
            }
            throw new RuntimeException("Migrating failed", e);
        }

        // if all success, drop the origin table
        client.dropTable(sourceDatabase, sourceTable, true, true);
    }

    @Override
    public void renameTable(boolean ignoreIfNotExists) throws Exception {
        Identifier targetTableId = Identifier.create(targetDatabase, targetTable);
        Identifier sourceTableId = Identifier.create(sourceDatabase, sourceTable);
        LOG.info("Last step: rename {} to {}.", targetTableId, sourceTableId);
        hiveCatalog.renameTable(targetTableId, sourceTableId, ignoreIfNotExists);
    }

    private void checkPrimaryKey() throws Exception {
        PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(sourceDatabase, sourceTable);
        try {
            if (!client.getPrimaryKeys(primaryKeysRequest).isEmpty()) {
                throw new IllegalArgumentException("Can't migrate primary key table yet.");
            }
        } catch (Exception e) {
            LOG.warn(
                    "Your Hive version is low which not support get_primary_keys, skip primary key check firstly!");
        }
    }

    private void checkPaimonTable(FileStoreTable paimonTable) {
        if (paimonTable.primaryKeys().size() > 0) {
            throw new IllegalArgumentException(
                    "Hive migrator only support append only table target table");
        }

        if (paimonTable.store().bucketMode() != BucketMode.UNAWARE) {
            throw new IllegalArgumentException(
                    "Hive migrator only support unaware-bucket target table");
        }
    }

    public Schema from(
            List<FieldSchema> fields,
            List<FieldSchema> partitionFields,
            Map<String, String> hiveTableOptions) {
        HashMap<String, String> paimonOptions = new HashMap<>(this.options);
        paimonOptions.put(CoreOptions.BUCKET.key(), "-1");
        // for compatible with hive comment system
        if (hiveTableOptions.get("comment") != null) {
            paimonOptions.put("hive.comment", hiveTableOptions.get("comment"));
        }

        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .comment(hiveTableOptions.get("comment"))
                        .options(paimonOptions)
                        .partitionKeys(
                                partitionFields.stream()
                                        .map(FieldSchema::getName)
                                        .collect(Collectors.toList()));

        fields.forEach(
                field ->
                        schemaBuilder.column(
                                field.getName(),
                                toPaimonType(field.getType()),
                                field.getComment()));

        return schemaBuilder.build();
    }

    private List<MigrateTask> importPartitionedTableTask(
            IMetaStoreClient client,
            FileIO fileIO,
            List<String> partitionNames,
            Table sourceTable,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback)
            throws Exception {
        List<MigrateTask> migrateTasks = new ArrayList<>();
        List<BinaryWriter.ValueSetter> valueSetters = new ArrayList<>();

        RowType partitionRowType =
                paimonTable.schema().projectedLogicalRowType(paimonTable.schema().partitionKeys());

        partitionRowType
                .getFieldTypes()
                .forEach(type -> valueSetters.add(BinaryWriter.createValueSetter(type)));

        for (String partitionName : partitionNames) {
            Partition partition =
                    client.getPartition(
                            sourceTable.getDbName(), sourceTable.getTableName(), partitionName);
            Map<String, String> values = client.partitionNameToSpec(partitionName);
            String format = parseFormat(partition.getSd().getSerdeInfo().toString());
            String location = partition.getSd().getLocation();
            BinaryRow partitionRow =
                    FileMetaUtils.writePartitionValue(partitionRowType, values, valueSetters);
            Path path = paimonTable.store().pathFactory().bucketPath(partitionRow, 0);

            migrateTasks.add(
                    new MigrateTask(
                            fileIO, format, location, paimonTable, partitionRow, path, rollback));
        }
        return migrateTasks;
    }

    public MigrateTask importUnPartitionedTableTask(
            FileIO fileIO,
            Table sourceTable,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback) {
        String format = parseFormat(sourceTable.getSd().getSerdeInfo().toString());
        String location = sourceTable.getSd().getLocation();
        Path path = paimonTable.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
        return new MigrateTask(
                fileIO, format, location, paimonTable, BinaryRow.EMPTY_ROW, path, rollback);
    }

    private void checkCompatible(Table sourceHiveTable, FileStoreTable paimonTable) {
        List<FieldSchema> sourceFields = new ArrayList<>(sourceHiveTable.getPartitionKeys());
        List<DataField> targetFields =
                new ArrayList<>(
                        paimonTable
                                .schema()
                                .projectedLogicalRowType(paimonTable.partitionKeys())
                                .getFields());

        if (sourceFields.size() != targetFields.size()) {
            throw new RuntimeException(
                    "Source table partition keys not match target table partition keys.");
        }

        sourceFields.sort(Comparator.comparing(FieldSchema::getName));
        targetFields.sort(Comparator.comparing(DataField::name));

        for (int i = 0; i < sourceFields.size(); i++) {
            FieldSchema s = sourceFields.get(i);
            DataField t = targetFields.get(i);

            if (!s.getName().equals(t.name())
                    || !s.getType().equalsIgnoreCase(t.type().asSQLString())) {
                throw new RuntimeException(
                        "Source table partition keys not match target table partition keys, please checkCompatible.");
            }
        }
    }

    private String parseFormat(String serder) {
        if (serder.contains("avro")) {
            return "avro";
        } else if (serder.contains("parquet")) {
            return "parquet";
        } else if (serder.contains("orc")) {
            return "orc";
        } else {
            throw new UnsupportedOperationException("Unknown partition format: " + serder);
        }
    }

    /** One import task for one partition. */
    public static class MigrateTask implements Callable<CommitMessage> {

        private final FileIO fileIO;
        private final String format;
        private final String location;
        private final FileStoreTable paimonTable;
        private final BinaryRow partitionRow;
        private final Path newDir;
        private final Map<Path, Path> rollback;

        public MigrateTask(
                FileIO fileIO,
                String format,
                String location,
                FileStoreTable paimonTable,
                BinaryRow partitionRow,
                Path newDir,
                Map<Path, Path> rollback) {
            this.fileIO = fileIO;
            this.format = format;
            this.location = location;
            this.paimonTable = paimonTable;
            this.partitionRow = partitionRow;
            this.newDir = newDir;
            this.rollback = rollback;
        }

        @Override
        public CommitMessage call() throws Exception {
            if (!fileIO.exists(newDir)) {
                fileIO.mkdirs(newDir);
            }
            List<DataFileMeta> fileMetas =
                    FileMetaUtils.construct(
                            fileIO,
                            format,
                            location,
                            paimonTable,
                            HIDDEN_PATH_FILTER,
                            newDir,
                            rollback);
            return FileMetaUtils.commitFile(partitionRow, fileMetas);
        }
    }
}
