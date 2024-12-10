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

package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;

import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** migrate hive table to paimon table. */
public class IcebergMigrator implements Migrator {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMigrator.class);

    private final ThreadPoolExecutor executor;

    private final Catalog paimonCatalog;
    private final FileIO paimonFileIO;
    private final String paimonDatabaseName;
    private final String paimonTableName;

    private final org.apache.iceberg.catalog.Catalog icebergCatalog;
    private final String icebergDatabaseName;
    private final String icebergTableName;

    // metadata path factory for iceberg metadata
    private IcebergPathFactory icebergMetaPathFactory;
    // iceberg table latest snapshot id
    private long icebergLatestSnapshotId;
    // iceberg latest snapshot metadata file name
    private String icebergLatestMetaDataFileName;
    // metadata for newest iceberg snapshot
    private final IcebergMetadata icebergMetadata;

    private boolean ignoreDelete = false;

    public IcebergMigrator(
            Catalog paimonCatalog,
            String paimonDatabaseName,
            String paimonTableName,
            org.apache.iceberg.catalog.Catalog icebergCatalog,
            String icebergDatabaseName,
            String icebergTableName,
            boolean ignoreDelete,
            Integer parallelism) {
        this.paimonCatalog = paimonCatalog;
        this.paimonFileIO = paimonCatalog.fileIO();
        this.paimonDatabaseName = paimonDatabaseName;
        this.paimonTableName = paimonTableName;

        this.icebergCatalog = icebergCatalog;
        this.icebergDatabaseName = icebergDatabaseName;
        this.icebergTableName = icebergTableName;

        initIcebergInfoForPaimon();
        this.icebergMetadata =
                IcebergMetadata.fromPath(
                        paimonFileIO,
                        icebergMetaPathFactory.toMetadataPath(icebergLatestMetaDataFileName));
        this.ignoreDelete = ignoreDelete;

        this.executor = createCachedThreadPool(parallelism, "ICEBERG_MIGRATOR");
    }

    public void initIcebergInfoForPaimon() {
        try {
            LOG.info("Try to load iceberg table to get the metadata path of latest snapshot.");
            org.apache.iceberg.Table icebergTable =
                    icebergCatalog.loadTable(
                            TableIdentifier.of(icebergDatabaseName, icebergTableName));
            org.apache.iceberg.TableMetadata currentMetadata =
                    ((org.apache.iceberg.BaseTable) icebergTable).operations().current();
            Path metadataLocation = new Path(currentMetadata.metadataFileLocation());
            LOG.info("iceberg latest snapshot metadata file location: {}", metadataLocation);
            this.icebergMetaPathFactory = new IcebergPathFactory(metadataLocation.getParent());
            this.icebergLatestMetaDataFileName = metadataLocation.getName();
            this.icebergLatestSnapshotId = currentMetadata.currentSnapshot().snapshotId();
            LOG.info("get iceberg metadata path successfully.");
        } catch (Exception e) {
            throw new RuntimeException(
                    "failed to get iceberg metadata path by loading iceberg table.");
        }
    }

    @Override
    public void executeMigrate() throws Exception {
        Schema paimonSchema = icebergSchemaToPaimonSchema(icebergMetadata);
        Identifier paimonIdentifier = Identifier.create(paimonDatabaseName, paimonTableName);

        paimonCatalog.createDatabase(paimonDatabaseName, false);
        paimonCatalog.createTable(paimonIdentifier, paimonSchema, false);

        try {
            FileStoreTable paimonTable = (FileStoreTable) paimonCatalog.getTable(paimonIdentifier);

            IcebergManifestFile manifestFile =
                    IcebergManifestFile.create(paimonTable, icebergMetaPathFactory);
            IcebergManifestList manifestList =
                    IcebergManifestList.create(paimonTable, icebergMetaPathFactory);

            List<IcebergManifestFileMeta> icebergManifestFileMetas =
                    manifestList.read(icebergMetadata.currentSnapshot().manifestList());

            // check manifest file with 'DELETE' kind
            icebergManifestFileMetas = checkAndFilterManifestFiles(icebergManifestFileMetas);

            // get all live iceberg entries
            List<IcebergManifestEntry> icebergEntries =
                    icebergManifestFileMetas.stream()
                            .flatMap(fileMeta -> manifestFile.read(fileMeta).stream())
                            .filter(IcebergManifestEntry::isLive)
                            .collect(Collectors.toList());
            if (icebergEntries.isEmpty()) {
                LOG.info(
                        "No live manifest entry in iceberg table for snapshot {}, iceberg table meta path is {}.",
                        icebergLatestSnapshotId,
                        icebergMetaPathFactory.toMetadataPath(icebergLatestSnapshotId));
                return;
            }

            List<IcebergDataFileMeta> icebergDataFileMetas =
                    icebergEntries.stream()
                            .map(IcebergManifestEntry::file)
                            .collect(Collectors.toList());

            // Again, check if the file is a Delete File
            icebergDataFileMetas = checkAndFilterDataFiles(icebergDataFileMetas);

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
                    if (paimonFileIO.exists(newPath)) {
                        paimonFileIO.rename(newPath, origin);
                    }
                }

                throw new RuntimeException("Migrating failed because exception happens", e);
            }
            try (BatchTableCommit commit = paimonTable.newBatchWriteBuilder().newCommit()) {
                commit.commit(new ArrayList<>(commitMessages));
                LOG.info("paimon commit success! Iceberg data files has been migrated to paimon.");
            }
        } catch (Exception e) {
            paimonCatalog.dropTable(paimonIdentifier, true);
            throw new RuntimeException("Migrating failed", e);
        }
    }

    @Override
    public void deleteOriginTable(boolean delete) throws Exception {}

    @Override
    public void renameTable(boolean ignoreIfNotExists) throws Exception {}

    public Schema icebergSchemaToPaimonSchema(IcebergMetadata icebergMetadata) {
        // get iceberg current schema
        IcebergSchema icebergSchema =
                icebergMetadata.schemas().get(icebergMetadata.currentSchemaId());

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

        return new Schema(
                dataFields, partitionKeys, Collections.emptyList(), Collections.emptyMap(), null);
    }

    private List<IcebergManifestFileMeta> checkAndFilterManifestFiles(
            List<IcebergManifestFileMeta> icebergManifestFileMetas) {
        if (!ignoreDelete) {
            for (IcebergManifestFileMeta meta : icebergManifestFileMetas) {
                if (meta.content() == IcebergManifestFileMeta.Content.DELETES) {
                    throw new RuntimeException(
                            "IcebergMigrator don't support analyzing manifest file with 'DELETE' content. "
                                    + "You can set 'ignore-delete' to ignore manifest file with 'DELETE' content.");
                }
            }
            return icebergManifestFileMetas;
        } else {
            return icebergManifestFileMetas.stream()
                    .filter(meta -> meta.content() != IcebergManifestFileMeta.Content.DELETES)
                    .collect(Collectors.toList());
        }
    }

    private List<IcebergDataFileMeta> checkAndFilterDataFiles(
            List<IcebergDataFileMeta> icebergDataFileMetas) {
        if (!ignoreDelete) {
            for (IcebergDataFileMeta meta : icebergDataFileMetas) {
                if (meta.content() != IcebergDataFileMeta.Content.DATA) {
                    throw new RuntimeException(
                            "IcebergMigrator don't support analyzing iceberg delete file. "
                                    + "You can set 'ignore-delete' to ignore iceberg delete files.");
                }
            }
            return icebergDataFileMetas;
        } else {
            return icebergDataFileMetas.stream()
                    .filter(meta -> meta.content() == IcebergDataFileMeta.Content.DATA)
                    .collect(Collectors.toList());
        }
    }

    private MigrateTask importUnPartitionedTable(
            List<IcebergDataFileMeta> icebergDataFileMetas,
            FileStoreTable paimonTable,
            Map<Path, Path> rollback) {
        BinaryRow partitionRow = BinaryRow.EMPTY_ROW;
        Path newDir = paimonTable.store().pathFactory().bucketPath(partitionRow, 0);

        return new MigrateTask(
                icebergDataFileMetas, paimonFileIO, paimonTable, partitionRow, newDir, rollback);
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
                    new MigrateTask(
                            entry.getValue(),
                            paimonFileIO,
                            paimonTable,
                            partitionRow,
                            newDir,
                            rollback));
        }
        return migrateTasks;
    }

    /** One import task for one partition. */
    public static class MigrateTask implements Callable<CommitMessage> {

        private final List<IcebergDataFileMeta> icebergDataFileMetas;
        private final FileIO fileIO;
        private final FileStoreTable paimonTable;
        private final BinaryRow partitionRow;
        private final Path newDir;
        private final Map<Path, Path> rollback;

        public MigrateTask(
                List<IcebergDataFileMeta> icebergDataFileMetas,
                FileIO fileIO,
                FileStoreTable paimonTable,
                BinaryRow partitionRow,
                Path newDir,
                Map<Path, Path> rollback) {
            this.icebergDataFileMetas = icebergDataFileMetas;
            this.fileIO = fileIO;
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
                            icebergDataFileMetas, fileIO, paimonTable, newDir, rollback);
            return FileMetaUtils.commitFile(partitionRow, fileMetas);
        }
    }
}
