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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.migrate.FileMetaUtils;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.org.apache.avro.file.DataFileStream;
import org.apache.paimon.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.paimon.shade.org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** migrate iceberg table to paimon table. */
public class IcebergMigrator implements Migrator {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergMigrator.class);

    private final ThreadPoolExecutor executor;

    private final Catalog paimonCatalog;
    private final String paimonDatabaseName;
    private final String paimonTableName;
    private final CoreOptions coreOptions;

    private final String icebergDatabaseName;
    private final String icebergTableName;
    private final Options icebergOptions;

    private final IcebergMigrateMetadata icebergMigrateMetadata;
    // metadata path factory for iceberg metadata
    private final IcebergPathFactory icebergMetaPathFactory;
    // latest metadata file path
    private final String icebergLatestMetadataLocation;
    // metadata for newest iceberg snapshot
    private final IcebergMetadata icebergMetadata;

    private Boolean deleteOriginTable = true;

    public IcebergMigrator(
            Catalog paimonCatalog,
            String paimonDatabaseName,
            String paimonTableName,
            String icebergDatabaseName,
            String icebergTableName,
            Options icebergOptions,
            Integer parallelism,
            Map<String, String> options) {
        this.paimonCatalog = paimonCatalog;
        this.paimonDatabaseName = paimonDatabaseName;
        this.paimonTableName = paimonTableName;
        this.coreOptions = new CoreOptions(options);
        checkArgument(
                coreOptions.bucket() == -1,
                "Iceberg migrator only support unaware-bucket target table, bucket should be -1");
        checkArgument(
                !options.containsKey(CoreOptions.PRIMARY_KEY.key()),
                "Iceberg migrator does not support define primary key for target table.");

        this.icebergDatabaseName = icebergDatabaseName;
        this.icebergTableName = icebergTableName;
        this.icebergOptions = icebergOptions;

        Preconditions.checkArgument(
                icebergOptions.containsKey(IcebergOptions.METADATA_ICEBERG_STORAGE.key()),
                "'metadata.iceberg.storage' is required, please make sure it has been set.");

        IcebergMigrateMetadataFactory icebergMigrateMetadataFactory;
        try {
            icebergMigrateMetadataFactory =
                    FactoryUtil.discoverFactory(
                            IcebergMigrator.class.getClassLoader(),
                            IcebergMigrateMetadataFactory.class,
                            icebergOptions.get(IcebergOptions.METADATA_ICEBERG_STORAGE).toString()
                                    + "_migrate");
        } catch (FactoryException e) {
            throw new RuntimeException("create IcebergMigrateMetadataFactory failed.", e);
        }

        icebergMigrateMetadata =
                icebergMigrateMetadataFactory.create(
                        Identifier.create(icebergDatabaseName, icebergTableName), icebergOptions);

        this.icebergMetadata = icebergMigrateMetadata.icebergMetadata();
        this.icebergLatestMetadataLocation = icebergMigrateMetadata.icebergLatestMetadataLocation();
        this.icebergMetaPathFactory =
                new IcebergPathFactory(new Path(icebergLatestMetadataLocation).getParent());

        this.executor = createCachedThreadPool(parallelism, "ICEBERG_MIGRATOR");
    }

    @Override
    public void executeMigrate() throws Exception {
        List<TableSchema> paimonSchemas = icebergSchemasToPaimonSchemas(icebergMetadata);
        Preconditions.checkArgument(
                !paimonSchemas.isEmpty(),
                "paimon schemas transformed from iceberg table is empty.");
        Identifier paimonIdentifier = Identifier.create(paimonDatabaseName, paimonTableName);

        paimonCatalog.createDatabase(paimonDatabaseName, true);
        TableSchema firstSchema = paimonSchemas.get(0);
        Preconditions.checkArgument(firstSchema.id() == 0, "Unexpected, first schema id is not 0.");
        paimonCatalog.createTable(paimonIdentifier, firstSchema.toSchema(), false);

        try {
            FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);
            FileIO fileIO = paimonTable.fileIO();
            SchemaManager schemaManager = paimonTable.schemaManager();
            // commit all the iceberg schemas
            for (int i = 1; i < paimonSchemas.size(); i++) {
                LOG.info(
                        "commit new schema from iceberg, new schema id:{}",
                        paimonSchemas.get(i).id());
                schemaManager.commit(paimonSchemas.get(i));
            }

            IcebergManifestFile manifestFile =
                    IcebergManifestFile.create(paimonTable, icebergMetaPathFactory);
            IcebergManifestList manifestList =
                    IcebergManifestList.create(paimonTable, icebergMetaPathFactory);

            List<IcebergManifestFileMeta> icebergManifestFileMetas =
                    manifestList.read(icebergMetadata.currentSnapshot().manifestList());

            // check manifest file with 'DELETE' kind
            checkAndFilterManifestFiles(icebergManifestFileMetas);

            Map<Long, List<IcebergManifestEntry>> icebergEntries = new HashMap<>();
            for (IcebergManifestFileMeta icebergManifestFileMeta : icebergManifestFileMetas) {
                long schemaId =
                        getSchemaIdFromIcebergManifestFile(
                                new Path(icebergManifestFileMeta.manifestPath()), fileIO);
                List<IcebergManifestEntry> entries = manifestFile.read(icebergManifestFileMeta);
                icebergEntries
                        .computeIfAbsent(schemaId, v -> new ArrayList<>())
                        .addAll(
                                entries.stream()
                                        .filter(IcebergManifestEntry::isLive)
                                        .collect(Collectors.toList()));
            }

            List<IcebergDataFileMeta> icebergDataFileMetas = new ArrayList<>();
            // write schema id to IcebergDataFileMeta
            for (Map.Entry<Long, List<IcebergManifestEntry>> kv : icebergEntries.entrySet()) {
                icebergDataFileMetas.addAll(
                        kv.getValue().stream()
                                .map(entry -> entry.file().withSchemaId(kv.getKey()))
                                .collect(Collectors.toList()));
            }

            if (icebergDataFileMetas.isEmpty()) {
                LOG.info(
                        "No live iceberg data files in iceberg table for snapshot {}, iceberg table meta path is {}.",
                        icebergMetadata.currentSnapshotId(),
                        icebergLatestMetadataLocation);
                return;
            }
            // Again, check if delete File exists
            checkAndFilterDataFiles(icebergDataFileMetas);

            LOG.info(
                    "Begin to create Migrate Task, the number of iceberg data files is {}",
                    icebergDataFileMetas.size());

            List<MigrateTask> tasks = new ArrayList<>();
            Map<Path, Path> rollback = new ConcurrentHashMap<>();
            if (paimonTable.partitionKeys().isEmpty()) {
                tasks.add(importUnPartitionedTable(icebergDataFileMetas, paimonTable, rollback));
            } else {
                tasks.addAll(importPartitionedTable(icebergDataFileMetas, paimonTable, rollback));
            }

            List<Future<CommitMessage>> futures =
                    tasks.stream().map(executor::submit).collect(Collectors.toList());
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
                for (Map.Entry<Path, Path> entry : rollback.entrySet()) {
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
                LOG.info("paimon commit success! Iceberg data files have been migrated to paimon.");
            }
        } catch (Exception e) {
            paimonCatalog.dropTable(paimonIdentifier, true);
            throw new RuntimeException("Migrating failed", e);
        }

        // if all success, drop the origin table according the delete field
        if (deleteOriginTable) {
            icebergMigrateMetadata.deleteOriginTable();
        }
    }

    @Override
    public void deleteOriginTable(boolean delete) throws Exception {
        this.deleteOriginTable = delete;
    }

    @Override
    public void renameTable(boolean ignoreIfNotExists) throws Exception {
        Identifier targetTableId = Identifier.create(paimonDatabaseName, paimonTableName);
        Identifier sourceTableId = Identifier.create(icebergDatabaseName, icebergTableName);
        LOG.info("Last step: rename {} to {}.", targetTableId, sourceTableId);
        paimonCatalog.renameTable(targetTableId, sourceTableId, ignoreIfNotExists);
    }

    private List<TableSchema> icebergSchemasToPaimonSchemas(IcebergMetadata icebergMetadata) {
        Map<String, String> options =
                icebergMetadata.properties() == null
                        ? Collections.emptyMap()
                        : icebergMetadata.properties();
        return icebergMetadata.schemas().stream()
                .map(
                        icebergSchema -> {
                            LOG.info(
                                    "Convert iceberg schema to paimon schema, iceberg schema id: {}",
                                    icebergSchema.schemaId());
                            return TableSchema.create(
                                    icebergSchema.schemaId(),
                                    icebergSchemaToPaimonSchema(icebergSchema, options));
                        })
                .collect(Collectors.toList());
    }

    private Schema icebergSchemaToPaimonSchema(
            IcebergSchema icebergSchema, Map<String, String> options) {

        // get iceberg current partition spec
        int currentPartitionSpecId = icebergMetadata.defaultSpecId();
        IcebergPartitionSpec currentIcebergPartitionSpec =
                icebergMetadata.partitionSpecs().get(currentPartitionSpecId);

        List<DataField> dataFields =
                icebergSchema.fields().stream()
                        .map(IcebergDataField::toDatafield)
                        .collect(Collectors.toList());

        List<String> partitionKeys =
                currentIcebergPartitionSpec.fields().stream()
                        .map(IcebergPartitionField::name)
                        .collect(Collectors.toList());

        return new Schema(dataFields, partitionKeys, Collections.emptyList(), options, null);
    }

    private void checkAndFilterManifestFiles(
            List<IcebergManifestFileMeta> icebergManifestFileMetas) {

        for (IcebergManifestFileMeta meta : icebergManifestFileMetas) {
            Preconditions.checkArgument(
                    meta.content() != IcebergManifestFileMeta.Content.DELETES,
                    "IcebergMigrator don't support analyzing manifest file with 'DELETE' content.");
        }
    }

    private void checkAndFilterDataFiles(List<IcebergDataFileMeta> icebergDataFileMetas) {

        for (IcebergDataFileMeta meta : icebergDataFileMetas) {
            Preconditions.checkArgument(
                    meta.content() == IcebergDataFileMeta.Content.DATA,
                    "IcebergMigrator don't support analyzing iceberg delete file.");
        }
    }

    private long getSchemaIdFromIcebergManifestFile(Path manifestPath, FileIO fileIO) {

        try (DataFileStream<GenericRecord> dataFileStream =
                new DataFileStream<>(
                        fileIO.newInputStream(manifestPath), new GenericDatumReader<>())) {
            String schema = dataFileStream.getMetaString("schema");
            return JsonSerdeUtil.fromJson(schema, IcebergSchema.class).schemaId();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<DataFileMeta> construct(
            List<IcebergDataFileMeta> icebergDataFileMetas,
            FileIO fileIO,
            Table paimonTable,
            Path newDir,
            Map<Path, Path> rollback) {
        return icebergDataFileMetas.stream()
                .map(
                        icebergDataFileMeta ->
                                constructFileMeta(
                                        icebergDataFileMeta, fileIO, paimonTable, newDir, rollback))
                .collect(Collectors.toList());
    }

    private static DataFileMeta constructFileMeta(
            IcebergDataFileMeta icebergDataFileMeta,
            FileIO fileIO,
            Table table,
            Path dir,
            Map<Path, Path> rollback) {
        FileStatus status;
        try {
            status = fileIO.getFileStatus(new Path(icebergDataFileMeta.filePath()));
        } catch (IOException e) {
            throw new RuntimeException(
                    "error when get file status. file path is " + icebergDataFileMeta.filePath(),
                    e);
        }
        String format = icebergDataFileMeta.fileFormat();
        long schemaId = icebergDataFileMeta.schemaId();
        return FileMetaUtils.constructFileMeta(
                format, status, fileIO, table, dir, rollback, schemaId);
    }

    private MigrateTask importUnPartitionedTable(
            List<IcebergDataFileMeta> icebergDataFileMetas,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback) {
        BinaryRow partitionRow = BinaryRow.EMPTY_ROW;
        Path newDir = paimonTable.store().pathFactory().bucketPath(partitionRow, 0);

        return new MigrateTask(icebergDataFileMetas, paimonTable, partitionRow, newDir, rollback);
    }

    private List<MigrateTask> importPartitionedTable(
            List<IcebergDataFileMeta> icebergDataFileMetas,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback) {
        Map<BinaryRow, List<IcebergDataFileMeta>> dataInPartition =
                icebergDataFileMetas.stream()
                        .collect(Collectors.groupingBy(IcebergDataFileMeta::partition));
        List<MigrateTask> migrateTasks = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IcebergDataFileMeta>> entry : dataInPartition.entrySet()) {
            BinaryRow partitionRow = entry.getKey();
            Path newDir = paimonTable.store().pathFactory().bucketPath(partitionRow, 0);
            migrateTasks.add(
                    new MigrateTask(entry.getValue(), paimonTable, partitionRow, newDir, rollback));
        }
        return migrateTasks;
    }

    /** One import task for one partition. */
    public static class MigrateTask implements Callable<CommitMessage> {

        private final List<IcebergDataFileMeta> icebergDataFileMetas;
        private final FileStoreTable paimonTable;
        private final BinaryRow partitionRow;
        private final Path newDir;
        private final Map<Path, Path> rollback;

        public MigrateTask(
                List<IcebergDataFileMeta> icebergDataFileMetas,
                FileStoreTable paimonTable,
                BinaryRow partitionRow,
                Path newDir,
                Map<Path, Path> rollback) {
            this.icebergDataFileMetas = icebergDataFileMetas;
            this.paimonTable = paimonTable;
            this.partitionRow = partitionRow;
            this.newDir = newDir;
            this.rollback = rollback;
        }

        @Override
        public CommitMessage call() throws Exception {
            FileIO fileIO = paimonTable.fileIO();
            if (!fileIO.exists(newDir)) {
                fileIO.mkdirs(newDir);
            }
            List<DataFileMeta> fileMetas =
                    construct(icebergDataFileMetas, fileIO, paimonTable, newDir, rollback);
            return FileMetaUtils.createCommitMessage(
                    partitionRow, paimonTable.coreOptions().bucket(), fileMetas);
        }
    }
}
