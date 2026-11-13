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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergConversions;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.manifest.IcebergPartitionSummary;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergRef;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DataFilePathFactories;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * A {@link CommitCallback} to create Iceberg compatible metadata, so Iceberg readers can read
 * Paimon's {@link RawFile}.
 */
public class IcebergCommitCallback implements CommitCallback, TagCallback {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitCallback.class);

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private static final String PUFFIN_FORMAT = "puffin";

    // Snapshot summary metric keys
    private static final String SNAPSHOT_SUMMARY_ADDED_DATA_FILES = "added-data-files";
    private static final String SNAPSHOT_SUMMARY_ADDED_RECORDS = "added-records";
    private static final String SNAPSHOT_SUMMARY_ADDED_FILES_SIZE = "added-files-size";
    private static final String SNAPSHOT_SUMMARY_DELETED_DATA_FILES = "deleted-data-files";
    private static final String SNAPSHOT_SUMMARY_DELETED_RECORDS = "deleted-records";
    private static final String SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE = "removed-files-size";
    private static final String SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT =
            "changed-partition-count";
    static final String SNAPSHOT_SUMMARY_TOTAL_RECORDS = "total-records";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DATA_FILES = "total-data-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE = "total-files-size";
    private static final String SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES = "total-delete-files";
    private static final String SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    final FileStoreTable table;
    private final String commitUser;

    final IcebergPathFactory pathFactory;
    private final @Nullable IcebergMetadataCommitter metadataCommitter;

    private final FileStorePathFactory fileStorePathFactory;
    final IcebergManifestFile manifestFile;
    final IcebergManifestList manifestList;
    final int formatVersion;

    private final IndexFileHandler indexFileHandler;
    private final boolean needAddDvToIceberg;
    private final IcebergOptions.MissingBaseStrategy missingBaseStrategy;
    private final IcebergOptions.StorageType storageType;

    private ReplayHook replayHook = (point, value) -> {};

    /** See {@link #tableLock}. */
    @Nullable private SnapshotCommit tableLock;

    /**
     * The most files that a cleanup deletes while it holds the table lock. The lock keeps every
     * commit and rollback of the table out, so a long cleanup takes it in several turns.
     */
    private static final int CLEANUP_BATCH_SIZE = 50;

    private int cleanupBatchSize = CLEANUP_BATCH_SIZE;

    // -------------------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------------------

    public IcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        this.storageType = storageType;
        this.pathFactory = new IcebergPathFactory(catalogTableMetadataPath(table));

        IcebergMetadataCommitterFactory metadataCommitterFactory;
        try {
            metadataCommitterFactory =
                    FactoryUtil.discoverFactory(
                            IcebergCommitCallback.class.getClassLoader(),
                            IcebergMetadataCommitterFactory.class,
                            storageType.committerFactoryIdentifier());
        } catch (FactoryException e) {
            metadataCommitterFactory = null;
            // storage types without a committer have no factory by design, so a miss is expected
            if (storageType.requiresMetadataCommitter()) {
                LOG.warn(
                        "No IcebergMetadataCommitterFactory for '{}={}' found on the classpath, so "
                                + "table {} will not be synced to the external catalog (commits and "
                                + "metadata files are unaffected). Check that the module providing it "
                                + "is deployed and that its META-INF/services/{} entry survived "
                                + "shading. Cause: {}",
                        IcebergOptions.METADATA_ICEBERG_STORAGE.key(),
                        storageType,
                        table.fullName(),
                        Factory.class.getName(),
                        e.getMessage());
            }
        }
        this.metadataCommitter =
                metadataCommitterFactory == null ? null : metadataCommitterFactory.create(table);

        this.fileStorePathFactory = table.store().pathFactory();
        this.manifestFile = IcebergManifestFile.create(table, pathFactory);
        this.manifestList = IcebergManifestList.create(table, pathFactory);

        this.formatVersion =
                table.coreOptions().toConfiguration().get(IcebergOptions.FORMAT_VERSION);
        Preconditions.checkArgument(
                formatVersion == IcebergMetadata.FORMAT_VERSION_V2
                        || formatVersion == IcebergMetadata.FORMAT_VERSION_V3,
                "Unsupported iceberg format version! Only version 2 or version 3 is valid, but current version is ",
                formatVersion);

        this.indexFileHandler = table.store().newIndexFileHandler();
        this.needAddDvToIceberg = needAddDvToIceberg();
        this.missingBaseStrategy =
                table.coreOptions()
                        .toConfiguration()
                        .get(IcebergOptions.METADATA_MISSING_BASE_STRATEGY);
    }

    @VisibleForTesting
    void setReplayHook(ReplayHook replayHook) {
        this.replayHook = replayHook;
    }

    @VisibleForTesting
    void setCleanupBatchSize(int cleanupBatchSize) {
        Preconditions.checkArgument(cleanupBatchSize > 0, "A batch has at least one file.");
        this.cleanupBatchSize = cleanupBatchSize;
    }

    void replayPoint(ReplayPoint point, long value) {
        replayHook.at(point, value);
    }

    public static Path catalogTableMetadataPath(FileStoreTable table) {
        Path icebergDBPath = catalogDatabasePath(table);
        return new Path(icebergDBPath, String.format("%s/metadata", table.location().getName()));
    }

    public static Path catalogDatabasePath(FileStoreTable table) {
        Path dbPath = table.location().getParent();
        final String dbSuffix = ".db";

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);

        IcebergOptions.StorageLocation storageLocation =
                table.coreOptions()
                        .toConfiguration()
                        .getOptional(IcebergOptions.METADATA_ICEBERG_STORAGE_LOCATION)
                        .orElse(inferDefaultMetadataLocation(storageType));

        switch (storageLocation) {
            case TABLE_LOCATION:
                // Iceberg metadata is written beside the table, under the database's own location,
                // so no warehouse (<db>.db) layout is required. This lets the table register in any
                // catalog, including a database whose location is not a Paimon warehouse path (e.g.
                // an externally-provisioned / cross-account catalog database).
                return dbPath;
            case CATALOG_STORAGE:
                // Catalog-storage derives a warehouse-relative iceberg/<db>/ path by stripping the
                // ".db" suffix, so it only applies under the Paimon <db>.db warehouse layout.
                if (!dbPath.getName().endsWith(dbSuffix)) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Storage type %s with catalog-location Iceberg metadata requires a "
                                            + "Paimon warehouse database (a <db>.db location); set "
                                            + "metadata.iceberg.storage-location=table-location for a "
                                            + "database with a non-warehouse location.",
                                    storageType.name()));
                }
                String dbName =
                        dbPath.getName()
                                .substring(0, dbPath.getName().length() - dbSuffix.length());
                return new Path(dbPath.getParent(), String.format("iceberg/%s/", dbName));
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage location " + storageLocation.name());
        }
    }

    private static IcebergOptions.StorageLocation inferDefaultMetadataLocation(
            IcebergOptions.StorageType storageType) {
        switch (storageType) {
            case TABLE_LOCATION:
                return IcebergOptions.StorageLocation.TABLE_LOCATION;
            case HIVE_CATALOG:
            case HADOOP_CATALOG:
            case REST_CATALOG:
                return IcebergOptions.StorageLocation.CATALOG_STORAGE;
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage type: " + storageType.name());
        }
    }

    @Override
    public synchronized void close() throws Exception {
        SnapshotCommit lock = tableLock;
        tableLock = null;
        if (lock != null) {
            lock.close();
        }
    }

    @Override
    public void call(Context context) {
        createMetadata(
                context.snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(context.deltaFiles, removedFiles, addedFiles),
                context.indexFiles);
    }

    @Override
    public void retry(ManifestCommittable committable) {
        // a retry runs no pre-commit validation and may republish metadata as it stands
        SchemaValidation.validateHistoricalIcebergTypes(
                table.schemaManager()::listAll, table.coreOptions());
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot =
                snapshotManager
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()))
                        .stream()
                        .max(Comparator.comparingLong(Snapshot::id))
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "There is no snapshot for commit user "
                                                        + commitUser
                                                        + " and identifier "
                                                        + committable.identifier()
                                                        + " for table "
                                                        + table.name()
                                                        + ". This is unexpected."));
        long snapshotId = snapshot.id();
        createMetadata(
                snapshot,
                (removedFiles, addedFiles) ->
                        collectFileChanges(snapshotId, removedFiles, addedFiles),
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX));
    }

    @Override
    public void setTable(FileStoreTable table) {
        // nothing to do
    }

    private void createMetadata(
            Snapshot snapshot,
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles) {
        long snapshotId = snapshot.id();
        try {
            // a stale callback outliving a rollback (or drop and recreate) must do nothing;
            // read the snapshot file directly - snapshot caches may predate the rollback
            Snapshot current;
            try {
                current =
                        SnapshotManager.tryFromPath(
                                table.fileIO(), table.snapshotManager().snapshotPath(snapshotId));
            } catch (FileNotFoundException e) {
                return;
            }
            if (!commitIdentity(current).equals(commitIdentity(snapshot))) {
                return;
            }
            // the snapshot cache may still hold a rolled-back timeline under reused ids;
            // every by-id read below must see the disk state the guard just verified
            table.snapshotManager().invalidateCache();

            if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID) {
                // If Iceberg metadata is stored separately in another directory, dropping the table
                // will not delete old Iceberg metadata. So we delete them here, when the table is
                // created again and the first snapshot is committed. It runs under the lock,
                // and only while this snapshot is the head: a callback that outlived the
                // recreation of the table, or a later commit, must not wipe what that published
                cleanUnderLock(
                        snapshot,
                        this::isCurrentHead,
                        () -> table.fileIO().delete(pathFactory.metadataDirectory(), true));
            }

            String abandonedUuid = null;
            int abandonedLastColumnId = 0;
            long abandonedNextRowId = 0;
            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                if (metadataMatchesSnapshot(snapshotId, snapshot)) {
                    // a retry repairs hint, pointer and files only while this snapshot is
                    // still the head; a replay of an older committable must not move them back
                    String generation =
                            publishUnderLock(snapshot, lock -> repairPublication(lock, snapshot));
                    if (generation == null) {
                        return;
                    }
                    // a failed earlier attempt skipped the normal post-publication cleanup
                    String pending = readRetirePending();
                    List<Path> files = applicableMetadataFiles(snapshotId);
                    files.addAll(olderFilesUnreachableFrom(snapshotId, generation));
                    if (deleteUnderLock(snapshot, generation, files)
                            && retireAbandonedSuffix(snapshot)) {
                        clearRetirePending(snapshot, pending);
                    }
                    return;
                }
                // a reused snapshot id: only the current head may replace the abandoned
                // metadata; a delayed replay must not move hint or pointer backwards
                Long latestNow = table.snapshotManager().latestSnapshotId();
                if (latestNow == null || latestNow != snapshotId) {
                    return;
                }
                // read the identity now, delete only at the write site: readers and the
                // catalog pointer keep a working file until the replacement is built
                IcebergMetadata abandoned =
                        readUsableMetadata(pathFactory.toMetadataPath(snapshotId));
                if (abandoned != null) {
                    abandonedUuid = abandoned.tableUuid();
                    abandonedLastColumnId = abandoned.lastColumnId();
                    if (abandoned.nextRowId() != null) {
                        abandonedNextRowId = abandoned.nextRowId();
                    }
                }
            }
            // steady-state commits skip the listing; anything suspicious lists the actual
            // files, because the hint alone can lag while readers still probe past it
            Path retirePending = new Path(pathFactory.metadataDirectory(), RETIRE_PENDING_FILENAME);
            // what the marker holds before anything is listed: it is cleared at the end only if
            // it still holds this, because a rollback since then may have left more behind
            boolean markerExists = table.fileIO().exists(retirePending);
            String pending = markerExists ? readRetirePending() : null;
            boolean suspectRollback =
                    abandonedUuid != null
                            || markerExists
                            || readVersionHint() != snapshotId - 1
                            || table.fileIO().exists(pathFactory.toMetadataPath(snapshotId + 1));
            long newestExisting = suspectRollback ? newestExistingMetadataVersion() : -1;
            boolean retireSuffix = abandonedUuid != null || newestExisting > snapshotId;
            if (retireSuffix && newestExisting > snapshotId) {
                // the newest abandoned version carries the authoritative high-water mark
                IcebergMetadata surviving =
                        readUsableMetadata(pathFactory.toMetadataPath(newestExisting));
                if (surviving != null) {
                    if (abandonedUuid == null) {
                        abandonedUuid = surviving.tableUuid();
                    }
                    abandonedLastColumnId =
                            Math.max(abandonedLastColumnId, surviving.lastColumnId());
                    if (surviving.nextRowId() != null) {
                        abandonedNextRowId = Math.max(abandonedNextRowId, surviving.nextRowId());
                    }
                }
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(snapshotId - 1);

            if (table.fileIO().exists(baseMetadataPath)) {
                createMetadataWithBase(
                        fileChangesCollector,
                        indexFiles.stream()
                                .filter(
                                        index ->
                                                index.indexFile()
                                                        .indexType()
                                                        .equals(DELETION_VECTORS_INDEX))
                                .collect(Collectors.toList()),
                        snapshot,
                        baseMetadataPath,
                        abandonedLastColumnId,
                        abandonedNextRowId);
            } else {
                RebuildRequest rebuild =
                        new RebuildRequest(
                                abandonedUuid, abandonedLastColumnId, abandonedNextRowId);
                if (missingBaseStrategy == IcebergOptions.MissingBaseStrategy.REPLAY) {
                    rebuild = replayOrRebuild(snapshot, fileChangesCollector, rebuild);
                }
                if (rebuild != null) {
                    createMetadataWithoutBase(
                            snapshot,
                            rebuild.inheritUuid,
                            rebuild.lastColumnIdFloor,
                            rebuild.nextRowIdFloor);
                }
            }

            // only after the replacement is durable, so readers keep a working head
            boolean retired = !retireSuffix || retireAbandonedSuffix(snapshot);
            if (suspectRollback && retired) {
                // the listing ran and every leftover above the head is gone
                clearRetirePending(snapshot, pending);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Moves the hint and the pointer of the catalog to the metadata that a snapshot already has,
     * when the snapshot is the head. Returns the generation of that metadata, or null when the
     * snapshot is not the head. Runs under {@link #publishUnderLock}, where the head and the file
     * are checked again: they may have changed since the caller looked.
     */
    @Nullable
    private String repairPublication(Lock lock, Snapshot snapshot) throws IOException {
        long snapshotId = snapshot.id();
        Long latest = table.snapshotManager().latestSnapshotId();
        if (latest == null || latest != snapshotId) {
            return null;
        }
        IcebergMetadata existing = readUsableMetadata(pathFactory.toMetadataPath(snapshotId));
        if (existing == null || !metadataMatchesSnapshot(existing, snapshot)) {
            return null;
        }
        lock.ensureValid();
        if (readVersionHint() != snapshotId) {
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
        }
        if (metadataCommitter != null) {
            // recommit the pointer (previous version as base, so a lagging
            // catalog advances) before retiring files it may still reference
            Path existingPath = pathFactory.toMetadataPath(snapshotId);
            Path basePath = pathFactory.toMetadataPath(snapshotId - 1);
            IcebergMetadata base =
                    table.fileIO().exists(basePath)
                            ? IcebergMetadata.fromPath(table.fileIO(), basePath)
                            : null;
            commitToExternalCatalog(existing, existingPath, base, base == null ? null : basePath);
        }
        return generationOf(existing);
    }

    // -------------------------------------------------------------------------------------
    // Create metadata afresh
    // -------------------------------------------------------------------------------------

    /**
     * Rebuilds the metadata from every data file of the snapshot that was checked. The snapshot is
     * not read again by its id: a rollback can give the id to another commit, and the work is
     * abandoned when the snapshot on disk is not the checked one any more.
     */
    private void createMetadataWithoutBase(
            Snapshot paimonSnapshot,
            @Nullable String inheritUuid,
            int lastColumnIdFloor,
            long nextRowIdFloor)
            throws IOException {
        long snapshotId = paimonSnapshot.id();
        replayPoint(ReplayPoint.REBUILD, snapshotId);
        if (!isCurrentSource(paimonSnapshot)) {
            abandonRebuild(paimonSnapshot);
            return;
        }
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(paimonSnapshot);
        SchemaCache schemaCache = new SchemaCache();
        List<IcebergManifestEntry> dataFileEntries = new ArrayList<>();
        List<IcebergManifestEntry> dvFileEntries = new ArrayList<>();
        SummaryMetrics metrics = new SummaryMetrics();
        Set<BinaryRow> changedPartitions = new HashSet<>();

        List<DataSplit> filteredDataSplits;
        try {
            filteredDataSplits =
                    snapshotReader.read().dataSplits().stream()
                            .filter(DataSplit::rawConvertible)
                            .collect(Collectors.toList());
        } catch (RuntimeException e) {
            // a rollback deletes the manifests of the snapshots it drops
            if (isMissingFile(e) && !isCurrentSource(paimonSnapshot)) {
                abandonRebuild(paimonSnapshot);
                return;
            }
            throw e;
        }
        for (DataSplit dataSplit : filteredDataSplits) {
            changedPartitions.add(dataSplit.partition());
            dataSplitToManifestEntries(
                    dataSplit, snapshotId, schemaCache, dataFileEntries, dvFileEntries);

            for (DataFileMeta paimonFileMeta : dataSplit.dataFiles()) {
                metrics.addedDataFiles++;
                metrics.addedRecords += paimonFileMeta.rowCount();
                metrics.addedFilesSize += paimonFileMeta.fileSize();
            }
        }

        List<IcebergManifestFileMeta> dataManifestFileMetas = new ArrayList<>();
        if (!dataFileEntries.isEmpty()) {
            dataManifestFileMetas.addAll(
                    manifestFile.rollingWrite(dataFileEntries.iterator(), snapshotId));
        }

        List<IcebergManifestFileMeta> dvManifestFileMetas = new ArrayList<>();
        if (!dvFileEntries.isEmpty()) {
            dvManifestFileMetas.addAll(
                    manifestFile.rollingWrite(
                            dvFileEntries.iterator(),
                            snapshotId,
                            IcebergManifestFileMeta.Content.DELETES));
        }

        List<IcebergManifestFileMeta> allManifestFileMetas = new ArrayList<>();
        allManifestFileMetas.addAll(dataManifestFileMetas);
        allManifestFileMetas.addAll(dvManifestFileMetas);

        metrics.changedPartitionCount = changedPartitions.size();
        metrics.totalDataFiles = metrics.addedDataFiles;
        metrics.deletedDataFiles = 0;
        metrics.deletedRecords = 0;
        metrics.deletedFilesSize = 0;
        metrics.totalRecords = metrics.addedRecords;
        metrics.totalFilesSize = metrics.addedFilesSize;
        long totalDeleteFiles = dvFileEntries.stream().filter(IcebergManifestEntry::isLive).count();
        long totalPositionDeleteRecords =
                dvFileEntries.stream()
                        .filter(IcebergManifestEntry::isLive)
                        .mapToLong(entry -> entry.file().recordCount())
                        .sum();
        metrics.totalDeleteFiles = totalDeleteFiles;
        metrics.totalPositionDeletes = totalPositionDeleteRecords;
        metrics.totalEqualityDeletes = 0;

        // a rebuild replaces metadata whose ids are already out with readers: never reuse them
        Long snapshotFirstRowId = computeSnapshotFirstRowId(nextRowIdFloor);
        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(allManifestFileMetas, snapshotFirstRowId);
        allManifestFileMetas = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName = manifestList.writeWithoutRolling(allManifestFileMetas);

        // current schema follows the latest; the snapshot entry records its own schema
        int schemaId = (int) schemaCache.getLatestSchemaId();
        int snapshotSchemaId = (int) paimonSnapshot.schemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergPartitionField> partitionFields = partitionFieldsOf(icebergSchema);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(
                        IcebergSnapshotSummary.APPEND.operation(), paimonSnapshot, metrics);

        IcebergSnapshot snapshot =
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId == Snapshot.FIRST_SNAPSHOT_ID ? null : (Long) (snapshotId - 1),
                        // the Paimon snapshot's own commit time, the as-of time readers see
                        paimonSnapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        snapshotSchemaId,
                        snapshotFirstRowId,
                        addedRows);

        // the refs are set when the metadata is written, see refsOf
        Map<String, IcebergRef> refs = new HashMap<>();

        // keep the identity of the metadata this rebuild replaces, so already loaded readers
        // and external catalogs keep refreshing the same table
        String tableUuid = inheritUuid != null ? inheritUuid : UUID.randomUUID().toString();

        List<IcebergSchema> allSchemas =
                IntStream.rangeClosed(0, schemaId)
                        .mapToObj(schemaCache::get)
                        .collect(Collectors.toList());
        IcebergMetadata metadata =
                new IcebergMetadata(
                        formatVersion,
                        tableUuid,
                        table.location().toString(),
                        snapshotId,
                        // every emitted schema counts, and a rebuild must not regress
                        // below the replaced metadata's high-water mark
                        Math.max(
                                lastColumnIdFloor,
                                allSchemas.stream()
                                        .mapToInt(IcebergSchema::highestFieldId)
                                        .max()
                                        .orElse(icebergSchema.highestFieldId())),
                        allSchemas,
                        schemaId,
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        lastPartitionIdOf(partitionFields),
                        Collections.singletonList(snapshot),
                        (int) snapshotId,
                        nextRowId,
                        refs);

        // the metadata is built from the snapshot that was read above, and it is published only
        // while that snapshot is still the one on disk
        Publication publication =
                publishUnderLock(
                        paimonSnapshot,
                        lock -> writeAndPoint(lock, paimonSnapshot, metadata, null, null));
        if (publication != null && publication.head) {
            // cleanup only after the catalog serves the new head: a skipped or failed
            // publication must not delete files an external pointer still references. The
            // files are chosen here and deleted under the lock, while the publication stands
            List<Path> files = new ArrayList<>();
            if (publication.ours) {
                // what the older metadata references is unneeded because this rebuild wrote
                // metadata that does not use it. Another callback's metadata of the same
                // snapshot, which is what stays when this write lost, may use any of it
                files.addAll(filesReferencedBefore(snapshotId));
            }
            files.addAll(applicableMetadataFiles(snapshotId));
            deleteUnderLock(paimonSnapshot, publication.generation, files);
        }
    }

    private void abandonRebuild(Snapshot paimonSnapshot) {
        LOG.info(
                "Snapshot {} of table {} was replaced before its Iceberg metadata was rebuilt, "
                        + "so nothing is written.",
                paimonSnapshot.id(),
                table.fullName());
        replayPoint(ReplayPoint.ABANDONED, paimonSnapshot.id());
    }

    private void dataSplitToManifestEntries(
            DataSplit dataSplit,
            long snapshotId,
            SchemaCache schemaCache,
            List<IcebergManifestEntry> dataFileEntries,
            List<IcebergManifestEntry> dvFileEntries) {
        List<RawFile> rawFiles = dataSplit.convertToRawFiles().get();

        for (int i = 0; i < dataSplit.dataFiles().size(); i++) {
            DataFileMeta paimonFileMeta = dataSplit.dataFiles().get(i);
            RawFile rawFile = rawFiles.get(i);
            IcebergDataFileMeta fileMeta =
                    IcebergDataFileMeta.create(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize(),
                            schemaCache.get(paimonFileMeta.schemaId()),
                            paimonFileMeta.valueStats(),
                            paimonFileMeta.valueStatsCols());
            dataFileEntries.add(
                    new IcebergManifestEntry(
                            IcebergManifestEntry.Status.ADDED,
                            snapshotId,
                            snapshotId,
                            snapshotId,
                            fileMeta));

            if (needAddDvToIceberg
                    && dataSplit.deletionFiles().isPresent()
                    && dataSplit.deletionFiles().get().get(i) != null) {
                DeletionFile deletionFile = dataSplit.deletionFiles().get().get(i);

                // Iceberg will check the cardinality between deserialized dv and iceberg deletion
                // file, so if deletionFile.cardinality() is null, we should stop synchronizing all
                // dvs.
                Preconditions.checkState(
                        deletionFile.cardinality() != null,
                        "cardinality in DeletionFile is null, stop generating dv for iceberg. "
                                + "dataFile path is {}, deletionFile is {}",
                        rawFile.path(),
                        deletionFile);

                // We can not get the file size of the complete DV index file from the DeletionFile,
                // so we set 'fileSizeInBytes' to -1(default in iceberg)
                IcebergDataFileMeta deleteFileMeta =
                        IcebergDataFileMeta.createForDeleteFile(
                                IcebergDataFileMeta.Content.POSITION_DELETES,
                                deletionFile.path(),
                                PUFFIN_FORMAT,
                                dataSplit.partition(),
                                deletionFile.cardinality(),
                                -1,
                                rawFile.path(),
                                deletionFile.offset(),
                                deletionFile.length());

                dvFileEntries.add(
                        new IcebergManifestEntry(
                                IcebergManifestEntry.Status.ADDED,
                                snapshotId,
                                snapshotId,
                                snapshotId,
                                deleteFileMeta));
            }
        }
    }

    List<IcebergPartitionField> partitionFieldsOf(IcebergSchema icebergSchema) {
        return getPartitionFields(table.schema().partitionKeys(), icebergSchema);
    }

    static int lastPartitionIdOf(List<IcebergPartitionField> partitionFields) {
        return partitionFields.stream()
                .mapToInt(IcebergPartitionField::fieldId)
                .max()
                // not sure why, this is a result tested by hand
                .orElse(IcebergPartitionField.FIRST_FIELD_ID - 1);
    }

    private List<IcebergPartitionField> getPartitionFields(
            List<String> partitionKeys, IcebergSchema icebergSchema) {
        Map<String, IcebergDataField> fields = new HashMap<>();
        for (IcebergDataField field : icebergSchema.fields()) {
            fields.put(field.name(), field);
        }

        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (String partitionKey : partitionKeys) {
            result.add(new IcebergPartitionField(fields.get(partitionKey), fieldId));
            fieldId++;
        }
        return result;
    }

    /** VARIANT is an Iceberg format-version-3 type; reject publishing it into v2 metadata. */
    static void checkVariantNotPublishable(RowType rowType) {
        Collection<String> variantFields = new LinkedHashSet<>();
        for (DataField field : rowType.getFields()) {
            collectVariantFields(field.name(), field.type(), variantFields);
        }
        Preconditions.checkArgument(
                variantFields.isEmpty(),
                "Columns %s use the VARIANT type, which requires Iceberg format version 3. "
                        + "Set 'metadata.iceberg.format-version' = '3' to publish this table.",
                variantFields);
    }

    private static void collectVariantFields(
            String path, DataType type, Collection<String> variantFields) {
        switch (type.getTypeRoot()) {
            case VARIANT:
                variantFields.add(path + ": " + type.asSQLString());
                break;
            case ARRAY:
                collectVariantFields(
                        path + ".element", ((ArrayType) type).getElementType(), variantFields);
                break;
            case MULTISET:
                collectVariantFields(
                        path + ".element", ((MultisetType) type).getElementType(), variantFields);
                break;
            case MAP:
                collectVariantFields(path + ".key", ((MapType) type).getKeyType(), variantFields);
                collectVariantFields(
                        path + ".value", ((MapType) type).getValueType(), variantFields);
                break;
            case ROW:
                for (DataField field : ((RowType) type).getFields()) {
                    collectVariantFields(path + "." + field.name(), field.type(), variantFields);
                }
                break;
            default:
                break;
        }
    }

    // -------------------------------------------------------------------------------------
    // Create metadata based on old ones
    // -------------------------------------------------------------------------------------

    /**
     * Deletes every metadata version above the current Paimon snapshots, which would otherwise
     * shadow the replaced timeline for readers probing past the hint. A failed deletion fails the
     * commit so a retry finishes the job; the manifests they reference stay on storage (the shared
     * prefix makes reference counting non-trivial), and nothing removes them later.
     *
     * <p>The head retires them, and so does a snapshot that a later commit overtook, once the hint
     * names no version above the head: until a head is published, the hint and the pointer of the
     * catalog may still name one of them. The versions above the head are listed here, and deleted
     * under the table lock, which checks the head again for every one of them: a commit that came
     * after the listing makes its own version a valid one. Returns false, and deletes nothing more,
     * when the versions may not be retired or the publication of the snapshot was replaced.
     */
    private boolean retireAbandonedSuffix(Snapshot source) throws IOException {
        List<Path> abandoned = new ArrayList<>();
        Long latestNow = table.snapshotManager().latestSnapshotId();
        for (FileStatus status : table.fileIO().listStatus(pathFactory.metadataDirectory())) {
            if (latestNow != null && metadataVersionOf(status.getPath()) > latestNow) {
                abandoned.add(status.getPath());
            }
        }
        for (int from = 0; from < abandoned.size(); from += cleanupBatchSize) {
            List<Path> batch =
                    abandoned.subList(from, Math.min(abandoned.size(), from + cleanupBatchSize));
            boolean done =
                    cleanUnderLock(
                            source,
                            this::mayRetireAboveHead,
                            () -> {
                                Long head = table.snapshotManager().latestSnapshotId();
                                for (Path path : batch) {
                                    if (head == null || metadataVersionOf(path) <= head) {
                                        continue;
                                    }
                                    table.fileIO().deleteQuietly(path);
                                    if (table.fileIO().exists(path)) {
                                        throw new IllegalStateException(
                                                "Failed to retire abandoned Iceberg metadata "
                                                        + path);
                                    }
                                }
                            });
            if (!done) {
                return false;
            }
        }
        return true;
    }

    /** The version of a metadata file such as v5.metadata.json, or -1 for any other file. */
    static long metadataVersionOf(Path path) {
        String name = path.getName();
        if (!name.startsWith("v") || !name.endsWith(".metadata.json")) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(1, name.indexOf('.')));
        } catch (NumberFormatException ignored) {
            return -1;
        }
    }

    static final String RETIRE_PENDING_FILENAME = "retire-pending";

    /** What the marker of a rollback holds, or null when there is none or it cannot be read. */
    @Nullable
    private String readRetirePending() {
        try {
            return table.fileIO()
                    .readFileUtf8(
                            new Path(pathFactory.metadataDirectory(), RETIRE_PENDING_FILENAME))
                    .trim();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Deletes the marker of a rollback once the leftovers of the rollback are retired, but only if
     * it still holds what it held before they were listed. A rollback writes it again, and the
     * leftovers of that one are not in the listing.
     */
    private void clearRetirePending(Snapshot source, @Nullable String seenBeforeListing)
            throws IOException {
        if (seenBeforeListing == null) {
            return;
        }
        cleanUnderLock(
                source,
                published -> isPublished(published, null),
                () -> {
                    if (seenBeforeListing.equals(readRetirePending())) {
                        table.fileIO()
                                .deleteQuietly(
                                        new Path(
                                                pathFactory.metadataDirectory(),
                                                RETIRE_PENDING_FILENAME));
                    }
                });
    }

    @FunctionalInterface
    interface LockedAction<T> {
        T run(Lock lock) throws IOException;
    }

    /**
     * Runs an operation under the lock that the commits of the table take, so that the operation
     * and a rollback or a commit of the table cannot interleave. The lock comes from the catalog of
     * the table, and a table without one is not protected.
     */
    private <T> T runUnderTableLock(LockedAction<T> action) throws IOException {
        return runWithLock(table, tableLock(), action);
    }

    private static <T> T runWithLock(
            FileStoreTable table, SnapshotCommit lock, LockedAction<T> action) throws IOException {
        try {
            return lock.runWithLock(() -> action.run(lock));
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to run an operation with the lock of table " + table.fullName(), e);
        }
    }

    /**
     * The lock of the table, which the catalog of the table provides. One is made for the callback
     * and closed with it: making one loads a catalog, and every publication and cleanup takes the
     * lock.
     */
    private synchronized SnapshotCommit tableLock() {
        if (tableLock == null) {
            // the callbacks of a tag have no commit user, and a lock can ask for one
            String lockUser = commitUser.isEmpty() ? UUID.randomUUID().toString() : commitUser;
            tableLock =
                    table.catalogEnvironment()
                            .snapshotCommit(table.snapshotManager(), table.coreOptions(), lockUser);
        }
        return tableLock;
    }

    /**
     * Runs the part of a rollback that deletes snapshots under the lock that the publication of
     * Iceberg metadata takes, so a rollback never happens between the last check of a snapshot and
     * the publication of its metadata. It takes the lock whatever the Iceberg setting is, of the
     * handle that rolls back or of the table as it is stored: a writer that opened the table
     * earlier, or that turned Iceberg metadata on for its own job, can still publish after the
     * stored setting says off. A catalog without a lock gives a lock that does nothing.
     */
    public static void runRollback(FileStoreTable table, Runnable rollback) {
        SnapshotCommit lock =
                table.catalogEnvironment()
                        .snapshotCommit(
                                table.snapshotManager(),
                                table.coreOptions(),
                                UUID.randomUUID().toString());
        try {
            runWithLock(
                    table,
                    lock,
                    held -> {
                        // under the lock, a cleanup of the leftovers of an earlier rollback cannot
                        // clear this marker between its check and its delete
                        markRetirePendingForRollback(table);
                        rollback.run();
                        return null;
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            IOUtils.closeQuietly(lock);
        }
    }

    /** Whether the table, as it is stored, has Iceberg metadata on, whatever a handle sets. */
    private static boolean icebergMetadataEnabled(FileStoreTable table) {
        return table.schemaManager()
                .latest()
                .map(
                        schema ->
                                Options.fromMap(schema.options())
                                                .get(IcebergOptions.METADATA_ICEBERG_STORAGE)
                                        != IcebergOptions.StorageType.DISABLED)
                .orElse(false);
    }

    /** Whether the commits of the table take a lock that keeps other writers and rollbacks out. */
    private boolean tableLockCoordinates() {
        return tableLock().isCoordinating();
    }

    /**
     * Publishes metadata under the lock that the commits and the rollbacks of the table take.
     * Everything that decides what readers see goes through here: the metadata file, the version
     * hint and the pointer of the external catalog. The source is checked again under the lock, so
     * a rollback cannot replace the snapshot between that check and the publication. Dropping and
     * recreating the table does not take the lock: a publication that passed the check can still
     * write into the metadata directory of the new table. Building the metadata stays outside the
     * lock, and so does the cleanup that follows.
     *
     * <p>Returns null, and does not run the publication, when the snapshot is not the one on disk
     * any more. The files that were built for it stay on storage.
     */
    @Nullable
    <T> T publishUnderLock(Snapshot source, LockedAction<T> publication) throws IOException {
        replayPoint(ReplayPoint.BEFORE_WRITE, source.id());
        return runUnderTableLock(
                lock -> {
                    if (!isCurrentSource(source)) {
                        return null;
                    }
                    lock.ensureValid();
                    replayPoint(ReplayPoint.LOCKED, source.id());
                    return publication.run(lock);
                });
    }

    /** Whether the Paimon snapshot on disk is still the one that the metadata is built from. */
    private boolean isCurrentSource(Snapshot source) {
        // the snapshot cache may still hold a rolled-back timeline under reused ids
        table.snapshotManager().invalidateCache();
        Path path = table.snapshotManager().snapshotPath(source.id());
        try {
            Snapshot onDisk = SnapshotManager.tryFromPath(table.fileIO(), path);
            return commitIdentity(onDisk).equals(commitIdentity(source));
        } catch (FileNotFoundException e) {
            try {
                confirmMissing(path, e);
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
            return false;
        }
    }

    /** Whether the Paimon snapshot on disk is still the head, and the one the metadata is from. */
    private boolean isCurrentHead(Snapshot source) {
        Long latest = table.snapshotManager().latestSnapshotId();
        return isCurrentSource(source) && latest != null && latest == source.id();
    }

    /**
     * Whether the metadata that was published for the snapshot is still in place: the snapshot is
     * the one on disk, and the metadata file of its version is built from it. The commit identity
     * is not enough for what depends on the files a metadata file lists, because two callbacks can
     * build metadata files from the same snapshot, with different files, and only one of them is
     * durable. Such a cleanup passes the generation it was chosen for, and the metadata file must
     * be of that generation. A cleanup that does not depend on the listed files passes null.
     */
    private boolean isPublished(Snapshot source, @Nullable String generation) {
        if (!isCurrentSource(source)) {
            return false;
        }
        try {
            IcebergMetadata durable = readUsableMetadata(pathFactory.toMetadataPath(source.id()));
            return durable != null
                    && metadataMatchesSnapshot(durable, source)
                    && (generation == null || generation.equals(generationOf(durable)));
        } catch (IOException e) {
            // a failed read says nothing about the file: fail, and the retry cleans up
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Whether the files chosen for the publication of the snapshot can still be deleted: the
     * publication stands, see {@link #isPublished}, or a later head superseded it. A later head
     * deletes the metadata file of the snapshot with the older versions once it published its own,
     * and what the metadata of the snapshot did not need, nothing after it needs.
     */
    private boolean isPublishedOrSuperseded(Snapshot source, String generation) {
        if (isPublished(source, generation)) {
            return true;
        }
        try {
            return isCurrentSource(source)
                    && readVersionHint() > source.id()
                    && !table.fileIO().exists(pathFactory.toMetadataPath(source.id()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Whether the metadata of the snapshot is in place, and the snapshot is the head, or the hint,
     * and the pointer of the catalog that moves with it, names no version above the head.
     */
    private boolean mayRetireAboveHead(Snapshot source) {
        if (!isPublished(source, null)) {
            return false;
        }
        Long latest = table.snapshotManager().latestSnapshotId();
        return latest != null && (latest == source.id() || readVersionHint() <= latest);
    }

    /** Deletions that run under the table lock. */
    @FunctionalInterface
    private interface Deletions {
        void run() throws IOException;
    }

    /**
     * Deletes the files that the publication of a snapshot made unneeded. The files are chosen
     * outside the lock, from what the publication replaced. They are deleted under the lock that
     * the commits and the rollbacks of the table take, in batches that keep a hold of it short.
     * Before every batch the publication is checked again: a rollback and a replacement can
     * complete between the publication and the deletion, and the files may belong to the new
     * timeline then. The rest of the cleanup is abandoned in that case, and the files stay on
     * storage: the cleaning of orphan files does not look at the Iceberg metadata. A later head
     * that superseded the publication does not stop the cleanup, see {@link
     * #isPublishedOrSuperseded}.
     *
     * <p>The generation is that of the metadata the files were chosen for, see {@link
     * #isPublished}.
     *
     * <p>Returns whether every file was handled, and false when the cleanup was abandoned.
     */
    boolean deleteUnderLock(Snapshot source, String generation, List<Path> files)
            throws IOException {
        for (int from = 0; from < files.size(); from += cleanupBatchSize) {
            List<Path> batch = files.subList(from, Math.min(files.size(), from + cleanupBatchSize));
            boolean done =
                    cleanUnderLock(
                            source,
                            published -> isPublishedOrSuperseded(published, generation),
                            () -> {
                                for (Path file : batch) {
                                    table.fileIO().deleteQuietly(file);
                                }
                            });
            if (!done) {
                return false;
            }
        }
        return true;
    }

    /**
     * Runs deletions under the table lock, once the snapshot they are for passes the check. Returns
     * false, with nothing deleted, when it does not.
     */
    private boolean cleanUnderLock(
            Snapshot source, Predicate<Snapshot> stillCurrent, Deletions deletions)
            throws IOException {
        replayPoint(ReplayPoint.BEFORE_CLEANUP, source.id());
        boolean done =
                runUnderTableLock(
                        lock -> {
                            if (!stillCurrent.test(source)) {
                                return false;
                            }
                            lock.ensureValid();
                            replayPoint(ReplayPoint.CLEANUP_LOCKED, source.id());
                            deletions.run();
                            return true;
                        });
        if (!done) {
            LOG.info(
                    "Iceberg metadata of snapshot {} of table {} was replaced before its cleanup "
                            + "ran. The cleanup is abandoned, and its files stay on storage.",
                    source.id(),
                    table.fullName());
        }
        return done;
    }

    /**
     * Writes the metadata file of the snapshot and, when the snapshot is the head, moves the hint
     * and the pointer of the external catalog to the metadata that is durable afterwards. Runs
     * under {@link #publishUnderLock}.
     */
    private Publication writeAndPoint(
            Lock lock,
            Snapshot snapshot,
            IcebergMetadata metadata,
            @Nullable IcebergMetadata baseMetadata,
            @Nullable Path baseMetadataPath)
            throws IOException {
        IcebergMetadata durable = writeMetadataFile(snapshot, metadata);
        return pointTo(lock, snapshot.id(), metadata, durable, baseMetadata, baseMetadataPath);
    }

    /**
     * Writes the metadata file of the snapshot, and returns the metadata that is durable for it
     * afterwards: the callback's own, or that of another callback that got there first. Both are
     * built from the same Paimon snapshot, and they do not list the same files, so a callback that
     * did not write the winner does not own what the winner depends on.
     *
     * <p>Runs under the lock, where the snapshot was checked: a file of an older timeline in the
     * way is replaced, and a file built from this snapshot is never.
     */
    IcebergMetadata writeMetadataFile(Snapshot snapshot, IcebergMetadata built) throws IOException {
        long snapshotId = snapshot.id();
        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        IcebergMetadata metadata = built.withRefs(refsOf(built.snapshots()));
        String json = metadata.toJson();
        boolean written;
        try {
            // atomic-first: where rename overwrites, a stale twin is replaced with no window at
            // all; otherwise fall back to delete-then-write, the smallest window available
            written = table.fileIO().tryToWriteAtomic(metadataPath, json);
            if (!written
                    && table.fileIO().exists(metadataPath)
                    && !metadataMatchesSnapshot(snapshotId, snapshot)) {
                table.fileIO().deleteQuietly(metadataPath);
                written = table.fileIO().tryToWriteAtomic(metadataPath, json);
            }
        } catch (IOException e) {
            // the outcome is unknown until what is durable is read: the lock keeps every other
            // writer out, so a file that is absent, or not built from this snapshot, is not ours
            boolean exists;
            IcebergMetadata durable;
            try {
                exists = table.fileIO().exists(metadataPath);
                durable = exists ? readUsableMetadata(metadataPath) : null;
            } catch (IOException | RuntimeException unknown) {
                e.addSuppressed(unknown);
                throw e;
            }
            if (exists && durable == null) {
                // malformed, which a write that was cut short can leave
                throw e;
            }
            if (durable == null || !metadataMatchesSnapshot(durable, snapshot)) {
                throw new MetadataNotWritten(
                        "Failed to write Iceberg metadata "
                                + metadataPath
                                + " for table "
                                + table.name(),
                        e);
            }
            return durable;
        }
        if (written) {
            return metadata;
        }
        // another callback published this snapshot first, and its file stays
        IcebergMetadata winner = readUsableMetadata(metadataPath);
        if (winner == null || !metadataMatchesSnapshot(winner, snapshot)) {
            // no twin published this snapshot's metadata; fail so the commit retries
            throw new MetadataNotWritten(
                    "Failed to replace Iceberg metadata "
                            + metadataPath
                            + " for table "
                            + table.name());
        }
        return winner;
    }

    /**
     * The refs of the tags of the given snapshots. They are read when the metadata is written,
     * under the lock that the creation and the deletion of a tag take, so none of them is lost.
     */
    private Map<String, IcebergRef> refsOf(List<IcebergSnapshot> snapshots) {
        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
        Set<Long> snapshotIds =
                snapshots.stream().map(IcebergSnapshot::snapshotId).collect(Collectors.toSet());
        // every tag of a snapshot becomes its own ref: dropping sibling tags would make
        // them invisible in Iceberg and break VERSION AS OF for them
        Map<String, IcebergRef> refs = new HashMap<>();
        for (Map.Entry<Snapshot, List<String>> entry : table.tagManager().tags().entrySet()) {
            long taggedSnapshotId = entry.getKey().id();
            if (snapshotIds.contains(taggedSnapshotId)) {
                for (String tagName : entry.getValue()) {
                    refs.put(tagName, new IcebergRef(taggedSnapshotId));
                }
            }
        }
        return refs;
    }

    /** The write of a metadata file is known to have left nothing of the callback behind. */
    static final class MetadataNotWritten extends IllegalStateException {

        private MetadataNotWritten(String message) {
            super(message);
        }

        private MetadataNotWritten(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Moves the hint and the pointer of the external catalog to the durable metadata of the
     * snapshot, when the snapshot is the head. The pointer gets the durable metadata, whoever wrote
     * it: what the catalog serves has to be what the file holds.
     */
    Publication pointTo(
            Lock lock,
            long snapshotId,
            IcebergMetadata own,
            IcebergMetadata durable,
            @Nullable IcebergMetadata baseMetadata,
            @Nullable Path baseMetadataPath)
            throws IOException {
        // a delayed callback may still write its metadata (a newer commit extends it), but
        // only the current head may move the hint and the external catalog
        Long latest = table.snapshotManager().latestSnapshotId();
        boolean head = latest != null && latest == snapshotId;
        if (head) {
            lock.ensureValid();
            table.fileIO()
                    .overwriteFileUtf8(
                            new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                            String.valueOf(snapshotId));
            commitToExternalCatalog(
                    durable,
                    pathFactory.toMetadataPath(snapshotId),
                    baseMetadata,
                    baseMetadataPath);
        }
        String generation = generationOf(durable);
        return new Publication(head, generation.equals(generationOf(own)), generation);
    }

    /**
     * Names the files that the metadata of a snapshot depends on: the manifest list of its current
     * snapshot, which every build writes anew. Two metadata files built from the same Paimon
     * snapshot differ in it. It is a name and not a path, because the same list can be written with
     * another prefix.
     */
    static String generationOf(IcebergMetadata metadata) {
        return new Path(metadata.currentSnapshot().manifestList()).getName();
    }

    /** What a publication left durable for the snapshot, and whether it is the callback's own. */
    static final class Publication {

        final boolean head;
        /** Whether the callback wrote the durable metadata, and so owns the files it lists. */
        final boolean ours;
        /** The generation of the durable metadata, see {@link #generationOf}. */
        final String generation;

        Publication(boolean head, boolean ours, String generation) {
            this.head = head;
            this.ours = ours;
            this.generation = generation;
        }
    }

    /**
     * Marks that a rollback may have left abandoned metadata behind; written before the rollback
     * deletes anything, and cleared once a commit has listed and retired the leftovers.
     */
    public static void markRetirePendingForRollback(FileStoreTable table) {
        if (!icebergMetadataEnabled(table)) {
            return;
        }
        try {
            Path dir = catalogTableMetadataPath(table);
            if (table.fileIO().exists(dir)) {
                // every rollback leaves its own marker, so that a cleanup that listed the
                // leftovers before it does not clear it
                table.fileIO()
                        .overwriteFileUtf8(
                                new Path(dir, RETIRE_PENDING_FILENAME),
                                UUID.randomUUID().toString());
            }
        } catch (Exception e) {
            // best-effort: the commit-time suspicion gate still covers the common cases
        }
    }

    /** The version recorded in the hint file, or -1 when absent or unreadable. */
    private long readVersionHint() {
        try {
            return Long.parseLong(
                    table.fileIO()
                            .readFileUtf8(
                                    new Path(
                                            pathFactory.metadataDirectory(), VERSION_HINT_FILENAME))
                            .trim());
        } catch (Exception e) {
            return -1;
        }
    }

    private void commitToExternalCatalog(
            IcebergMetadata metadata,
            Path metadataPath,
            @Nullable IcebergMetadata baseMetadata,
            @Nullable Path baseMetadataPath) {
        if (metadataCommitter == null) {
            return;
        }
        switch (metadataCommitter.identifier()) {
            case "hive":
                metadataCommitter.commitMetadata(metadataPath, baseMetadataPath);
                break;
            case "rest":
                metadataCommitter.commitMetadata(metadata, baseMetadata);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported metadata committer: " + metadataCommitter.identifier());
        }
    }

    /** The newest existing metadata file version, or -1 when there is none. */
    private long newestExistingMetadataVersion() throws IOException {
        FileStatus[] statuses;
        try {
            statuses = table.fileIO().listStatus(pathFactory.metadataDirectory());
        } catch (FileNotFoundException e) {
            // only a missing directory counts as empty; a transient listing failure must
            // fail the commit, or a stale suffix would silently survive
            return -1;
        }
        long newest = -1;
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (!name.startsWith("v") || !name.endsWith(".metadata.json")) {
                continue;
            }
            try {
                newest = Math.max(newest, Long.parseLong(name.substring(1, name.indexOf('.'))));
            } catch (NumberFormatException ignored) {
            }
        }
        return newest;
    }

    /**
     * The given metadata file. Returns null when the file is missing or malformed, which stays the
     * same however often it is read. Every other failure propagates, because a storage failure says
     * nothing about the file and a retry can succeed.
     */
    @Nullable
    IcebergMetadata readUsableMetadata(Path metadataPath) throws IOException {
        byte[] bytes;
        try {
            bytes = IOUtils.readFully(table.fileIO().newInputStream(metadataPath), true);
        } catch (IOException e) {
            if (isMissingFile(e)) {
                confirmMissing(metadataPath, e);
                return null;
            }
            throw e;
        }
        try {
            return IcebergMetadata.fromJson(new String(bytes, StandardCharsets.UTF_8));
        } catch (UncheckedIOException e) {
            if (!(e.getCause() instanceof JsonProcessingException)) {
                throw e;
            }
            // a read that ended early does not parse either, and a retry can read the file
            long length = table.fileIO().getFileSize(metadataPath);
            if (length != bytes.length) {
                throw new IOException(
                        "Read "
                                + bytes.length
                                + " of the "
                                + length
                                + " bytes of Iceberg metadata "
                                + metadataPath);
            }
            return null;
        }
    }

    /**
     * Fails with a storage failure when a file that a read did not find is there after all. A
     * missing file decides what the callback does, so a transient failure must not pass for one.
     */
    void confirmMissing(Path path, IOException notFound) throws IOException {
        if (table.fileIO().exists(path)) {
            IOException failure = new IOException("Failed to read " + path + ", which exists");
            // not the cause: a cause that says not found would classify the failure as missing
            failure.addSuppressed(notFound);
            throw failure;
        }
    }

    /**
     * Whether the failure says that a file does not exist, and not that I/O failed. An {@link
     * java.io.EOFException} does not say it: a stream that closed early looks the same as a file
     * that is short, and a retry can read the first.
     */
    static boolean isMissingFile(Throwable throwable) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t instanceof FileNotFoundException || t instanceof NoSuchFileException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /**
     * Whether the metadata file of the version was built from the snapshot. A file that is missing
     * or malformed does not match, and a failed read propagates: it says nothing about the file.
     */
    boolean metadataMatchesSnapshot(long snapshotId, Snapshot snapshot) throws IOException {
        IcebergMetadata existing = readUsableMetadata(pathFactory.toMetadataPath(snapshotId));
        return existing != null && metadataMatchesSnapshot(existing, snapshot);
    }

    /**
     * Whether the metadata was built from this very Paimon snapshot, judged by the commit identity
     * in the summary of its current snapshot. Metadata without an identity (older releases) is
     * trusted, so the protection only covers metadata written since.
     */
    private static boolean metadataMatchesSnapshot(IcebergMetadata metadata, Snapshot snapshot) {
        IcebergSnapshot current;
        try {
            current = metadata.currentSnapshot();
        } catch (RuntimeException e) {
            // it names a current snapshot that it does not hold
            return false;
        }
        if (current.summary() == null) {
            return false;
        }
        String identity = current.summary().get(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY);
        return identity == null || identity.equals(commitIdentity(snapshot));
    }

    /** Builds the metadata on top of the base file, then exposes it or falls back to a rebuild. */
    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            Path baseMetadataPath,
            int lastColumnIdFloor,
            long nextRowIdFloor)
            throws IOException {
        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);
        BuildResult result =
                buildMetadataWithBase(
                        fileChangesCollector,
                        indexFiles,
                        snapshot,
                        baseMetadata,
                        lastColumnIdFloor,
                        nextRowIdFloor,
                        true);
        switch (result.kind) {
            case STALE_SOURCE:
                return;
            case NEEDS_REBUILD:
                createMetadataWithoutBase(
                        snapshot,
                        result.inheritUuid,
                        result.lastColumnIdFloor,
                        result.nextRowIdFloor);
                return;
            case COMPLETED:
                exposeMetadataWithBase(result, snapshot, baseMetadata, baseMetadataPath);
                return;
            default:
                throw new IllegalStateException("Unknown build result " + result.kind);
        }
    }

    /** What {@link #buildMetadataWithBase} decided. */
    enum BuildKind {
        /** The metadata was built, and its manifests and manifest list are written. */
        COMPLETED,
        /** The base cannot be extended, the caller has to rebuild from the table. */
        NEEDS_REBUILD,
        /** The snapshot was replaced by a rollback or recreation, nothing is to be written. */
        STALE_SOURCE
    }

    static final class BuildResult {

        final BuildKind kind;
        @Nullable final IcebergMetadata metadata;
        @Nullable final List<IcebergSnapshot> toExpireExceptLast;
        @Nullable final String inheritUuid;
        final int lastColumnIdFloor;
        final long nextRowIdFloor;
        @Nullable final String reason;

        BuildResult(
                BuildKind kind,
                @Nullable IcebergMetadata metadata,
                @Nullable List<IcebergSnapshot> toExpireExceptLast,
                @Nullable String inheritUuid,
                int lastColumnIdFloor,
                long nextRowIdFloor,
                @Nullable String reason) {
            this.kind = kind;
            this.metadata = metadata;
            this.toExpireExceptLast = toExpireExceptLast;
            this.inheritUuid = inheritUuid;
            this.lastColumnIdFloor = lastColumnIdFloor;
            this.nextRowIdFloor = nextRowIdFloor;
            this.reason = reason;
        }

        static BuildResult completed(
                IcebergMetadata metadata, List<IcebergSnapshot> toExpireExceptLast) {
            return new BuildResult(
                    BuildKind.COMPLETED, metadata, toExpireExceptLast, null, 0, 0L, null);
        }

        static BuildResult needsRebuild(
                @Nullable String inheritUuid,
                int lastColumnIdFloor,
                long nextRowIdFloor,
                String reason) {
            return new BuildResult(
                    BuildKind.NEEDS_REBUILD,
                    null,
                    null,
                    inheritUuid,
                    lastColumnIdFloor,
                    nextRowIdFloor,
                    reason);
        }

        static BuildResult staleSource() {
            return new BuildResult(BuildKind.STALE_SOURCE, null, null, null, 0, 0L, null);
        }
    }

    /**
     * Builds the metadata of {@code snapshot} on top of {@code baseMetadata} and writes its
     * manifests and manifest list. It never writes a metadata file, never publishes, never cleans
     * up and never rebuilds: whatever it cannot extend is reported in the result.
     */
    BuildResult buildMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot,
            IcebergMetadata baseMetadata,
            int lastColumnIdFloor,
            long nextRowIdFloor,
            boolean compactManifests)
            throws IOException {
        long snapshotId = snapshot.id();
        // row ids handed out by the base or by abandoned metadata must never be reused
        long rowIdFloor =
                Math.max(
                        nextRowIdFloor,
                        baseMetadata.nextRowId() == null ? 0L : baseMetadata.nextRowId());

        // a base left on the abandoned timeline must be rebuilt, not extended
        if (table.snapshotManager().snapshotExists(snapshotId - 1)
                && !metadataMatchesSnapshot(
                        baseMetadata, table.snapshotManager().snapshot(snapshotId - 1))) {
            Long latestNow = table.snapshotManager().latestSnapshotId();
            if (latestNow == null || latestNow != snapshotId) {
                // a delayed replay on the abandoned timeline: leave publication to the head
                return BuildResult.staleSource();
            }
            // keep the stale base's identity so external catalogs do not recreate the table
            return BuildResult.needsRebuild(
                    baseMetadata.tableUuid(),
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor,
                    "the base is on an abandoned timeline");
        }

        if (!isSameFormatVersion(baseMetadata.formatVersion())) {
            // we need to recreate iceberg metadata if format version changed
            return BuildResult.needsRebuild(
                    null,
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor,
                    "the format version changed");
        }

        if (formatVersion == IcebergMetadata.FORMAT_VERSION_V3
                && baseMetadata.nextRowId() == null) {
            // v3 base metadata written before Paimon emitted row lineage; recreate to self-heal
            return BuildResult.needsRebuild(
                    baseMetadata.tableUuid(),
                    Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                    rowIdFloor,
                    "the base has no row lineage");
        }

        // decide the schema story before any manifest is written
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = (int) schemaCache.getLatestSchemaId();
        int snapshotSchemaId = (int) snapshot.schemaId();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        // re-verified each commit: a rollback re-evolution can redefine an already
        // verified id while this callback only ever sees increasing snapshot ids
        for (IcebergSchema known : baseMetadata.schemas()) {
            if (known.schemaId() > schemaId) {
                continue;
            }
            IcebergSchema current =
                    known.schemaId() == schemaId
                            ? icebergSchema
                            : schemaCache.get(known.schemaId());
            if (!known.equals(current)) {
                // a re-evolution reused this id with different fields; rebuild from scratch
                return BuildResult.needsRebuild(
                        baseMetadata.tableUuid(),
                        Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                        rowIdFloor,
                        "a schema id was reused with different fields");
            }
        }
        if (schemaId < baseMetadata.currentSchemaId()) {
            // pointer-only schema rollback keeps the base; an abandoned-timeline base
            // (snapshot entry mismatching the live snapshot) is rebuilt
            IcebergSnapshot baseCurrent = baseMetadata.currentSnapshot();
            SnapshotManager snapshotManager = table.snapshotManager();
            boolean pointerRollbackOnly =
                    baseCurrent != null
                            && snapshotManager.snapshotExists(snapshotId - 1)
                            && baseCurrent.schemaId()
                                    == (int) snapshotManager.snapshot(snapshotId - 1).schemaId();
            if (!pointerRollbackOnly) {
                return BuildResult.needsRebuild(
                        baseMetadata.tableUuid(),
                        Math.max(lastColumnIdFloor, baseMetadata.lastColumnId()),
                        rowIdFloor,
                        "the schema pointer moved back");
            }
        }

        List<IcebergManifestFileMeta> baseManifestFileMetas =
                manifestList.read(baseMetadata.currentSnapshot().manifestList());

        // base manifest file for data files
        List<IcebergManifestFileMeta> baseDataManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DATA)
                        .collect(Collectors.toList());

        // base manifest file for deletion vector index files
        List<IcebergManifestFileMeta> baseDVManifestFileMetas =
                baseManifestFileMetas.stream()
                        .filter(meta -> meta.content() == IcebergManifestFileMeta.Content.DELETES)
                        .collect(Collectors.toList());

        Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles = new LinkedHashMap<>();
        Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles = new LinkedHashMap<>();
        boolean isAddOnly = fileChangesCollector.collect(removedFiles, addedFiles);
        Set<BinaryRow> modifiedPartitionsSet =
                removedFiles.values().stream()
                        .map(Pair::getLeft)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        addedFiles.values().stream().map(Pair::getLeft).forEach(modifiedPartitionsSet::add);
        List<BinaryRow> modifiedPartitions = new ArrayList<>(modifiedPartitionsSet);

        // Note that this check may be different from `removedFiles.isEmpty()`,
        // because if a file's level is changed, it will first be removed and then added.
        // In this case, if `baseMetadata` already contains this file, we should not add a
        // duplicate.
        List<IcebergManifestFileMeta> newDataManifestFileMetas;
        List<IcebergDataFileMeta> deletedFiles = new ArrayList<>();
        String operation;
        if (isAddOnly) {
            // Fast case. We don't need to remove files from `baseMetadata`. We only need to append
            // new metadata files.
            newDataManifestFileMetas = new ArrayList<>(baseDataManifestFileMetas);
            newDataManifestFileMetas.addAll(
                    createNewlyAddedManifestFileMetas(addedFiles, snapshotId));
            operation = IcebergSnapshotSummary.APPEND.operation();
        } else {
            Pair<List<IcebergManifestFileMeta>, String> result =
                    createWithDeleteManifestFileMetas(
                            removedFiles,
                            addedFiles,
                            modifiedPartitions,
                            baseDataManifestFileMetas,
                            snapshotId,
                            snapshot.commitKind(),
                            deletedFiles);
            newDataManifestFileMetas = result.getLeft();
            operation = result.getRight();
        }

        List<IcebergManifestFileMeta> newDVManifestFileMetas = new ArrayList<>();
        if (needAddDvToIceberg) {
            if (!indexFiles.isEmpty()) {
                // reconstruct the dv index
                newDVManifestFileMetas.addAll(createDvManifestFileMetas(snapshot));
            } else {
                // no new dv index, reuse the old one
                newDVManifestFileMetas.addAll(baseDVManifestFileMetas);
            }
        }

        // compact data manifest file if needed
        if (compactManifests) {
            newDataManifestFileMetas =
                    compactMetadataIfNeeded(newDataManifestFileMetas, snapshotId);
        }

        SummaryMetrics metrics = new SummaryMetrics();
        metrics.addedDataFiles = addedFiles.size();
        metrics.addedRecords =
                addedFiles.values().stream().mapToLong(p -> p.getRight().rowCount()).sum();
        metrics.addedFilesSize =
                addedFiles.values().stream().mapToLong(p -> p.getRight().fileSize()).sum();
        metrics.deletedDataFiles = deletedFiles.size();
        metrics.deletedRecords =
                deletedFiles.stream().mapToLong(IcebergDataFileMeta::recordCount).sum();
        metrics.deletedFilesSize =
                deletedFiles.stream().mapToLong(IcebergDataFileMeta::fileSizeInBytes).sum();
        Set<BinaryRow> changedPartitions = new HashSet<>();
        addedFiles.values().stream().map(Pair::getLeft).forEach(changedPartitions::add);
        deletedFiles.stream()
                .map(file -> removedFiles.get(file.filePath()).getLeft())
                .forEach(changedPartitions::add);
        if (needAddDvToIceberg && !indexFiles.isEmpty()) {
            changedPartitions.addAll(
                    partitionsWithChangedDeleteFiles(indexFiles, baseDVManifestFileMetas));
        }
        metrics.changedPartitionCount = changedPartitions.size();

        metrics.totalDataFiles = computeLiveFileCount(newDataManifestFileMetas);
        metrics.totalRecords = computeLiveRowCount(newDataManifestFileMetas);

        // file sizes are not in the manifest list, so the previous total is reused when its
        // file count matches the base manifests
        IcebergSnapshot baseSnapshot = baseMetadata.currentSnapshot();
        long previousTotalFilesSize =
                baseTotalsUsable(baseSnapshot, baseDataManifestFileMetas)
                        ? getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE)
                        : computeTotalFilesSizeFromManifests(baseDataManifestFileMetas);
        metrics.totalFilesSize =
                Math.max(
                        0,
                        previousTotalFilesSize + metrics.addedFilesSize - metrics.deletedFilesSize);

        metrics.totalDeleteFiles = computeLiveFileCount(newDVManifestFileMetas);
        metrics.totalPositionDeletes = computeLiveRowCount(newDVManifestFileMetas);
        metrics.totalEqualityDeletes = 0;

        Long snapshotFirstRowId = computeSnapshotFirstRowId(rowIdFloor);

        ManifestRowIdAssignment rowIdAssignment =
                assignManifestFirstRowIds(
                        Stream.concat(
                                        newDataManifestFileMetas.stream(),
                                        newDVManifestFileMetas.stream())
                                .collect(Collectors.toList()),
                        snapshotFirstRowId);
        List<IcebergManifestFileMeta> newManifestFileMetasWithRowIds = rowIdAssignment.manifests;
        Long addedRows = snapshotFirstRowId == null ? null : rowIdAssignment.assignedRows;
        Long nextRowId =
                snapshotFirstRowId == null
                        ? null
                        : snapshotFirstRowId + rowIdAssignment.assignedRows;
        String manifestListFileName =
                manifestList.writeWithoutRolling(newManifestFileMetasWithRowIds);

        IcebergSnapshotSummary snapshotSummary =
                computeSnapshotSummary(operation, snapshot, metrics);

        // add new schemas if needed
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (schemaId > baseMetadata.currentSchemaId()) {
            // append only ids the list does not already carry
            Set<Integer> knownSchemaIds =
                    schemas.stream().map(IcebergSchema::schemaId).collect(Collectors.toSet());
            schemas = new ArrayList<>(schemas);
            schemas.addAll(
                    IntStream.rangeClosed(baseMetadata.currentSchemaId() + 1, schemaId)
                            .filter(id -> !knownSchemaIds.contains(id))
                            .mapToObj(schemaCache::get)
                            .collect(Collectors.toList()));
        }
        // a schema-pointer rollback (validated above): only the current pointer moves

        List<IcebergSnapshot> snapshots = new ArrayList<>(baseMetadata.snapshots());
        snapshots.add(
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        snapshotId - 1,
                        // the Paimon snapshot's own commit time, the as-of time readers see
                        snapshot.timeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        // the snapshot's own schema, for time travel
                        snapshotSchemaId,
                        snapshotFirstRowId,
                        addedRows));

        // all snapshots in this list, except the last one, need to expire
        List<IcebergSnapshot> toExpireExceptLast = new ArrayList<>();
        for (int i = 0; i + 1 < snapshots.size(); i++) {
            toExpireExceptLast.add(snapshots.get(i));
            // commit callback is called before expire, so we cannot use current earliest snapshot
            // and have to check expire condition by ourselves
            if (!shouldExpire(snapshots.get(i), snapshotId)) {
                snapshots = snapshots.subList(i, snapshots.size());
                break;
            }
        }

        // the refs are set when the metadata is written, see refsOf
        Map<String, IcebergRef> refs = new HashMap<>();

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        // must not regress when the current schema is older than the base's
                        // never below what the replaced metadata already handed out
                        Math.max(
                                lastColumnIdFloor,
                                Math.max(
                                        baseMetadata.lastColumnId(),
                                        icebergSchema.highestFieldId())),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId,
                        nextRowId,
                        refs);
        return BuildResult.completed(metadata, toExpireExceptLast);
    }

    /** Writes the built metadata, and publishes it and cleans up when it is the head. */
    private void exposeMetadataWithBase(
            BuildResult result,
            Snapshot snapshot,
            IcebergMetadata baseMetadata,
            Path baseMetadataPath)
            throws IOException {
        long snapshotId = snapshot.id();
        IcebergMetadata metadata = result.metadata;
        List<IcebergSnapshot> toExpireExceptLast = result.toExpireExceptLast;

        Publication publication =
                publishUnderLock(
                        snapshot,
                        lock ->
                                writeAndPoint(
                                        lock, snapshot, metadata, baseMetadata, baseMetadataPath));
        if (publication != null && publication.head) {
            // cleanup only after the catalog serves the new head: a skipped or failed
            // publication must not delete files an external pointer still references. The
            // files are chosen here and deleted under the lock, while the publication stands
            List<Path> files = applicableMetadataFiles(snapshotId);
            if (publication.ours) {
                // the snapshots that retention dropped are unneeded because this build's
                // metadata does not keep them. Another callback's metadata of the same
                // snapshot, which is what stays when this write lost, may keep them
                for (int i = 0; i + 1 < toExpireExceptLast.size(); i++) {
                    collectExpired(files, toExpireExceptLast.get(i), toExpireExceptLast.get(i + 1));
                }
            }
            deleteUnderLock(snapshot, publication.generation, files);
        }
    }

    interface FileChangesCollector {
        boolean collect(
                Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
                Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles)
                throws IOException;
    }

    boolean collectFileChanges(
            List<ManifestEntry> manifestEntries,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        boolean isAddOnly = true;
        DataFilePathFactories factories = new DataFilePathFactories(fileStorePathFactory);
        for (ManifestEntry entry : manifestEntries) {
            DataFilePathFactory dataFilePathFactory =
                    factories.get(entry.partition(), entry.bucket());
            String path = dataFilePathFactory.toPath(entry).toString();
            switch (entry.kind()) {
                case ADD:
                    if (shouldAddFileToIceberg(entry.file())) {
                        removedFiles.remove(path);
                        addedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    }
                    break;
                case DELETE:
                    isAddOnly = false;
                    addedFiles.remove(path);
                    removedFiles.put(path, Pair.of(entry.partition(), entry.file()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown ManifestEntry FileKind " + entry.kind());
            }
        }
        return isAddOnly;
    }

    boolean collectFileChanges(
            long snapshotId,
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        return collectFileChanges(
                table.store()
                        .newScan()
                        .withKind(ScanMode.DELTA)
                        .withSnapshot(snapshotId)
                        .plan()
                        .files(),
                removedFiles,
                addedFiles);
    }

    private boolean shouldAddFileToIceberg(DataFileMeta meta) {
        if (table.primaryKeys().isEmpty()) {
            return true;
        } else {
            if (needAddDvToIceberg) {
                return meta.level() > 0;
            }
            int maxLevel = table.coreOptions().numLevels() - 1;
            return meta.level() == maxLevel;
        }
    }

    private List<IcebergManifestFileMeta> createNewlyAddedManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles, long currentSnapshotId)
            throws IOException {
        if (addedFiles.isEmpty()) {
            return Collections.emptyList();
        }

        SchemaCache schemaCache = new SchemaCache();
        return manifestFile.rollingWrite(
                addedFiles.entrySet().stream()
                        .map(
                                e -> {
                                    DataFileMeta paimonFileMeta = e.getValue().getRight();
                                    IcebergDataFileMeta icebergFileMeta =
                                            IcebergDataFileMeta.create(
                                                    IcebergDataFileMeta.Content.DATA,
                                                    e.getKey(),
                                                    paimonFileMeta.fileFormat(),
                                                    e.getValue().getLeft(),
                                                    paimonFileMeta.rowCount(),
                                                    paimonFileMeta.fileSize(),
                                                    schemaCache.get(paimonFileMeta.schemaId()),
                                                    paimonFileMeta.valueStats(),
                                                    paimonFileMeta.valueStatsCols());
                                    return new IcebergManifestEntry(
                                            IcebergManifestEntry.Status.ADDED,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            icebergFileMeta);
                                })
                        .iterator(),
                currentSnapshotId);
    }

    private Pair<List<IcebergManifestFileMeta>, String> createWithDeleteManifestFileMetas(
            Map<String, Pair<BinaryRow, DataFileMeta>> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles,
            List<BinaryRow> modifiedPartitions,
            List<IcebergManifestFileMeta> baseManifestFileMetas,
            long currentSnapshotId,
            Snapshot.CommitKind commitKind,
            List<IcebergDataFileMeta> deletedFiles)
            throws IOException {
        String operation = IcebergSnapshotSummary.APPEND.operation();
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();

        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitions);
        // the fields where a modified partition is NaN, which manifest bounds never cover
        boolean[] modifiedNan = new boolean[partitionType.getFieldCount()];
        for (BinaryRow partition : modifiedPartitions) {
            for (int i = 0; i < modifiedNan.length; i++) {
                modifiedNan[i] |= isNaN(partition, i, partitionType.getTypeAt(i));
            }
        }

        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            Long[] nullCounts = new Long[numFields];
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                // an omitted bound means the value is unknown; keep the slot null. NaN is never
                // in the bounds, so they cannot rule out a NaN partition of a summary that saw NaN
                boolean boundsMissNan = modifiedNan[i] && summary.containsNan();
                byte[] lowerBound = boundsMissNan ? null : summary.lowerBound();
                byte[] upperBound = boundsMissNan ? null : summary.upperBound();
                if (lowerBound != null) {
                    minValues.setField(i, IcebergConversions.toPaimonObject(fieldType, lowerBound));
                }
                if (upperBound != null) {
                    maxValues.setField(i, IcebergConversions.toPaimonObject(fieldType, upperBound));
                }
                // `contains_null` does not give the number of nulls, so the count is unknown;
                // a count of 1 would make a manifest with one live row look all null
                nullCounts[i] = summary.containsNull() ? null : 0L;
            }

            if (predicate == null
                    || predicate.test(
                            fileMeta.liveRowsCount(),
                            minValues,
                            maxValues,
                            new GenericArray(nullCounts))) {
                // check if any IcebergManifestEntry in this manifest file meta is removed
                List<IcebergManifestEntry> entries =
                        manifestFile.read(new Path(fileMeta.manifestPath()).getName());
                boolean canReuseFile = true;
                for (IcebergManifestEntry entry : entries) {
                    if (entry.isLive()) {
                        String path = entry.file().filePath();
                        if (addedFiles.containsKey(path)) {
                            // added file already exists (most probably due to level changes),
                            // remove it to not add a duplicate.
                            addedFiles.remove(path);
                        } else if (removedFiles.containsKey(path)) {
                            canReuseFile = false;
                        }
                    }
                }

                if (canReuseFile) {
                    // nothing is removed, use this file meta again
                    newManifestFileMetas.add(fileMeta);
                } else {
                    // some file is removed, rewrite this file meta
                    operation =
                            commitKind == Snapshot.CommitKind.COMPACT
                                    ? IcebergSnapshotSummary.REPLACE.operation()
                                    : IcebergSnapshotSummary.OVERWRITE.operation();
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(fileMeta, entries);
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        if (entry.isLive()) {
                            boolean removed = removedFiles.containsKey(entry.file().filePath());
                            if (removed) {
                                deletedFiles.add(entry.file());
                            }
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removed
                                                    ? IcebergManifestEntry.Status.DELETED
                                                    : IcebergManifestEntry.Status.EXISTING,
                                            // a deleted entry records the snapshot that
                                            // deleted the file, not the one that added it
                                            removed ? currentSnapshotId : entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    newManifestFileMetas.addAll(
                            manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
                }
            } else {
                // partition of this file meta is not modified in this snapshot,
                // use this file meta again
                newManifestFileMetas.add(fileMeta);
            }
        }

        newManifestFileMetas.addAll(
                createNewlyAddedManifestFileMetas(addedFiles, currentSnapshotId));
        return Pair.of(newManifestFileMetas, operation);
    }

    private static boolean isNaN(BinaryRow row, int pos, DataType type) {
        if (row.isNullAt(pos)) {
            return false;
        }
        switch (type.getTypeRoot()) {
            case FLOAT:
                return Float.isNaN(row.getFloat(pos));
            case DOUBLE:
                return Double.isNaN(row.getDouble(pos));
            default:
                return false;
        }
    }

    // -------------------------------------------------------------------------------------
    // Compact
    // -------------------------------------------------------------------------------------

    private List<IcebergManifestFileMeta> compactMetadataIfNeeded(
            List<IcebergManifestFileMeta> toCompact, long currentSnapshotId) throws IOException {
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long targetSizeInBytes = table.coreOptions().manifestTargetSize().getBytes();

        List<IcebergManifestFileMeta> candidates = new ArrayList<>();
        long totalSizeInBytes = 0;
        for (IcebergManifestFileMeta meta : toCompact) {
            if (meta.manifestLength() < targetSizeInBytes * 2 / 3) {
                candidates.add(meta);
                totalSizeInBytes += meta.manifestLength();
            } else {
                result.add(meta);
            }
        }

        Options options = new Options(table.options());
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MIN_FILE_NUM)) {
            return toCompact;
        }
        if (candidates.size() < options.get(IcebergOptions.COMPACT_MAX_FILE_NUM)
                && totalSizeInBytes < targetSizeInBytes) {
            return toCompact;
        }

        Function<IcebergManifestFileMeta, List<IcebergManifestEntry>> processor =
                meta -> {
                    List<IcebergManifestEntry> sourceEntries =
                            materializeFirstRowIds(
                                    meta,
                                    IcebergManifestFile.create(table, pathFactory)
                                            .read(new Path(meta.manifestPath()).getName()));
                    List<IcebergManifestEntry> entries = new ArrayList<>();
                    for (IcebergManifestEntry entry : sourceEntries) {
                        // a deletion made by this commit is recorded against the current
                        // snapshot but keeps the file sequence number of the older snapshot
                        // that added the file, so it has to be recognised by snapshot id
                        if (entry.fileSequenceNumber() == currentSnapshotId
                                || entry.snapshotId() == currentSnapshotId
                                || entry.status() == IcebergManifestEntry.Status.EXISTING) {
                            entries.add(entry);
                        } else {
                            // rewrite status if this entry is from an older snapshot
                            IcebergManifestEntry.Status newStatus;
                            if (entry.status() == IcebergManifestEntry.Status.ADDED) {
                                newStatus = IcebergManifestEntry.Status.EXISTING;
                            } else if (entry.status() == IcebergManifestEntry.Status.DELETED) {
                                continue;
                            } else {
                                throw new UnsupportedOperationException(
                                        "Unknown IcebergManifestEntry.Status " + entry.status());
                            }
                            entries.add(
                                    new IcebergManifestEntry(
                                            newStatus,
                                            entry.snapshotId(),
                                            entry.sequenceNumber(),
                                            entry.fileSequenceNumber(),
                                            entry.file()));
                        }
                    }
                    if (meta.sequenceNumber() == currentSnapshotId) {
                        // this file is created for this snapshot, so it is not recorded in any
                        // iceberg metas, we need to clean it
                        table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                    }
                    return entries;
                };
        Iterable<IcebergManifestEntry> newEntries =
                ManifestReadThreadPool.sequentialBatchedExecute(processor, candidates, null);
        result.addAll(manifestFile.rollingWrite(newEntries.iterator(), currentSnapshotId));
        return result;
    }

    // -------------------------------------------------------------------------------------
    // Expire
    // -------------------------------------------------------------------------------------

    boolean shouldExpire(IcebergSnapshot snapshot, long currentSnapshotId) {
        return shouldExpire(snapshot.snapshotId(), snapshot.timestampMs(), currentSnapshotId);
    }

    boolean shouldExpire(long snapshotId, long timestampMs, long currentSnapshotId) {
        Options options = new Options(table.options());
        if (snapshotId > currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN)) {
            return false;
        }
        if (snapshotId <= currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX)) {
            return true;
        }
        return timestampMs
                < System.currentTimeMillis()
                        - options.get(CoreOptions.SNAPSHOT_TIME_RETAINED).toMillis();
    }

    /**
     * The names of the lists that a replay replaced for the snapshot, oldest replay first. A name
     * stands for the list of the metadata directory of this table, whatever the record says.
     */
    static List<String> replayedOriginals(IcebergSnapshot snapshot) {
        String json =
                snapshot.summary() == null
                        ? null
                        : snapshot.summary().get(SNAPSHOT_SUMMARY_REPLAY_ORIGINALS);
        if (json == null) {
            return Collections.emptyList();
        }
        try {
            List<String> names = new ArrayList<>();
            for (String original : JsonSerdeUtil.fromJson(json, String[].class)) {
                names.add(new Path(original).getName());
            }
            return names;
        } catch (RuntimeException e) {
            // a record that cannot be read names nothing that can be deleted
            LOG.warn("Cannot read the replay record of Iceberg snapshot {}", snapshot.snapshotId());
            return Collections.emptyList();
        }
    }

    private boolean existsUnchecked(Path path) {
        try {
            return table.fileIO().exists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The manifests that stay needed while the snapshot does, including its replay originals. */
    Set<String> manifestPathsReachableFrom(IcebergSnapshot snapshot) {
        Set<String> paths = manifestPathsIn(new Path(snapshot.manifestList()).getName());
        addManifestPathsOfOriginals(snapshot, paths);
        return paths;
    }

    void addManifestPathsOfOriginals(IcebergSnapshot snapshot, Set<String> paths) {
        for (String original : replayedOriginals(snapshot)) {
            if (existsUnchecked(pathFactory.toManifestListPath(original))) {
                paths.addAll(manifestPathsIn(original));
            }
        }
    }

    /**
     * Adds the files of a snapshot that retention drops to the files to delete: its own files and
     * its replay originals. Nothing is deleted here.
     */
    private void collectExpired(List<Path> files, IcebergSnapshot toExpire, IcebergSnapshot next) {
        Set<String> inUse = manifestPathsReachableFrom(next);
        collectExpiredList(files, new Path(toExpire.manifestList()).getName(), inUse);
        collectExpiredOriginals(files, toExpire, inUse);
    }

    void collectExpiredOriginals(
            List<Path> files, IcebergSnapshot toExpire, Set<String> pathsInUse) {
        for (String original : replayedOriginals(toExpire)) {
            if (existsUnchecked(pathFactory.toManifestListPath(original))) {
                collectExpiredList(files, original, pathsInUse);
            }
        }
    }

    Set<String> manifestPathsIn(String manifestListName) {
        // compare by physical path: a carried-over manifest may be re-listed with different
        // list-level fields (e.g. an assigned first_row_id) while sharing the same file
        Set<String> paths = new HashSet<>();
        for (IcebergManifestFileMeta meta : manifestList.read(manifestListName)) {
            paths.add(meta.manifestPath());
        }
        return paths;
    }

    void collectExpiredList(List<Path> files, String toExpire, Set<String> pathsInUse) {
        for (IcebergManifestFileMeta meta : manifestList.read(toExpire)) {
            if (pathsInUse.contains(meta.manifestPath())) {
                continue;
            }
            files.add(new Path(meta.manifestPath()));
        }
        files.add(pathFactory.toManifestListPath(toExpire));
    }

    /**
     * The files that a rebuild makes unneeded: what every older metadata file references. Nothing
     * is deleted here.
     */
    private List<Path> filesReferencedBefore(long snapshotId) throws IOException {
        List<Path> files = new ArrayList<>();
        Set<String> expiredManifestLists = new HashSet<>();
        Set<String> expiredManifestFileMetas = new HashSet<>();
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();

        while (it.hasNext()) {
            Path path = it.next();
            IcebergMetadata metadata = readUsableMetadata(path);
            if (metadata == null || metadata.snapshots() == null) {
                // an older version that is missing or malformed names nothing that can be
                // deleted through it, and it must not block the cleanup of the others: every
                // later rebuild would fail on it
                LOG.warn(
                        "Skipping the cleanup of the Iceberg metadata {} of table {}, because it "
                                + "is missing or malformed. The files only it references stay on "
                                + "storage.",
                        path,
                        table.fullName());
                continue;
            }

            for (IcebergSnapshot snapshot : metadata.snapshots()) {
                // a replay left the lists it copied, and they go with the snapshot
                List<Path> lists = new ArrayList<>();
                lists.add(new Path(snapshot.manifestList()));
                for (String original : replayedOriginals(snapshot)) {
                    lists.add(pathFactory.toManifestListPath(original));
                }
                for (Path listPath : lists) {
                    String listName = listPath.getName();
                    if (expiredManifestLists.contains(listName)) {
                        continue;
                    }
                    expiredManifestLists.add(listName);

                    // A retained metadata JSON can reference a list an earlier rebuild already
                    // deleted. Reading it must not fail: we only open it to delete what it
                    // points at, and that earlier pass already did so.
                    if (!table.fileIO().exists(listPath)) {
                        continue;
                    }

                    List<IcebergManifestFileMeta> metas;
                    try {
                        metas = manifestList.decode(listName, table.fileIO().getFileSize(listPath));
                    } catch (IcebergManifestFile.CorruptManifestException e) {
                        // the rebuild is what a history that is broken falls back to, so it must
                        // not fail on a list that is not one. The files only it names stay
                        LOG.warn(
                                "Skipping the manifest list {} of table {}, because it is "
                                        + "corrupt. The files only it names stay on storage.",
                                listPath,
                                table.fullName(),
                                e);
                        continue;
                    }
                    for (IcebergManifestFileMeta meta : metas) {
                        String metaName = new Path(meta.manifestPath()).getName();
                        if (expiredManifestFileMetas.contains(metaName)) {
                            continue;
                        }
                        expiredManifestFileMetas.add(metaName);
                        files.add(new Path(meta.manifestPath()));
                    }
                    files.add(listPath);
                }
            }
        }
        return files;
    }

    /**
     * What the older versions reference and the metadata of the snapshot does not reach, when that
     * metadata has no base: a replay or a rebuild published it, and the cleanup of the files it
     * made unneeded may not have run. Nothing is deleted here.
     */
    private List<Path> olderFilesUnreachableFrom(long snapshotId, String generation)
            throws IOException {
        if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId - 1))) {
            return new ArrayList<>();
        }
        List<Path> files = filesReferencedBefore(snapshotId);
        if (files.isEmpty()) {
            return files;
        }
        IcebergMetadata durable = readUsableMetadata(pathFactory.toMetadataPath(snapshotId));
        if (durable == null || !generation.equals(generationOf(durable))) {
            return new ArrayList<>();
        }
        Set<String> reachable = new HashSet<>();
        for (IcebergSnapshot snapshot : durable.snapshots()) {
            reachable.add(new Path(snapshot.manifestList()).getName());
            reachable.addAll(replayedOriginals(snapshot));
            for (String manifest : manifestPathsReachableFrom(snapshot)) {
                reachable.add(new Path(manifest).getName());
            }
        }
        files.removeIf(file -> reachable.contains(file.getName()));
        return files;
    }

    /**
     * The metadata files that the retention of previous versions drops once the metadata of the
     * snapshot is published. Nothing is deleted here.
     */
    List<Path> applicableMetadataFiles(long snapshotId) throws IOException {
        List<Path> files = new ArrayList<>();
        Options options = new Options(table.options());
        if (options.get(IcebergOptions.METADATA_DELETE_AFTER_COMMIT)) {
            long earliestMetadataId =
                    snapshotId - options.get(IcebergOptions.METADATA_PREVIOUS_VERSIONS_MAX);
            if (earliestMetadataId > 0) {
                Iterator<Path> it =
                        pathFactory
                                .getAllMetadataPathBefore(table.fileIO(), earliestMetadataId)
                                .iterator();
                while (it.hasNext()) {
                    files.add(it.next());
                }
            }
        }
        return files;
    }

    @Override
    public void notifyCreation(String tagName) {
        // The base TagCallback API does not carry a snapshot id, but Iceberg refs
        // require one. The tag is persisted by TagManager before this callback
        // fires, so resolve the snapshot the tag points to and delegate to the
        // snapshot aware overload.
        Optional<Tag> tag = table.tagManager().get(tagName);
        if (!tag.isPresent()) {
            LOG.info(
                    "Tag {} not found in Paimon TagManager when creating Iceberg ref. Unable to create tag.",
                    tagName);
            return;
        }
        notifyCreation(tagName, tag.get().id());
    }

    @Override
    public void notifyCreation(String tagName, long snapshotId) {
        try {
            // the file of the head is read, changed and written back under the lock, so no
            // publication or rollback of the table interleaves
            runUnderTableLock(
                    lock -> {
                        addRef(lock, tagName, snapshotId);
                        return null;
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to create tag " + tagName + " for table " + table.name(), e);
        }
    }

    private void addRef(Lock lock, String tagName, long snapshotId) throws IOException {
        table.snapshotManager().invalidateCache();
        Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            LOG.info(
                    "Latest Iceberg snapshot not found when creating tag {} for snapshot {}. Unable to create tag.",
                    tagName,
                    snapshotId);
            return;
        }

        Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
        if (!table.fileIO().exists(baseMetadataPath)) {
            LOG.info(
                    "Iceberg metadata file {} not found when creating tag {} for snapshot {}. Unable to create tag.",
                    baseMetadataPath,
                    tagName,
                    snapshotId);
            return;
        }

        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

        // Tags can only be included in Iceberg if they point to an Iceberg snapshot that
        // exists. Otherwise an Iceberg client fails to parse the metadata and all reads fail.
        boolean tagSnapshotInIceberg = false;
        for (IcebergSnapshot snapshot : baseMetadata.snapshots()) {
            if (snapshot.snapshotId() == snapshotId) {
                tagSnapshotInIceberg = true;
                break;
            }
        }

        if (!tagSnapshotInIceberg) {
            LOG.warn(
                    "Snapshot {} does not exist in Iceberg metadata. Unable to create tag {}.",
                    snapshotId,
                    tagName);
            return;
        }

        baseMetadata.refs().put(tagName, new IcebergRef(snapshotId));

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        baseMetadata.currentSnapshotId(),
                        baseMetadata.lastColumnId(),
                        baseMetadata.schemas(),
                        baseMetadata.currentSchemaId(),
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        baseMetadata.snapshots(),
                        baseMetadata.currentSnapshotId(),
                        baseMetadata.nextRowId(),
                        baseMetadata.refs());

        /*
        Overwrite the latest metadata file
        Currently the Paimon table snapshot id value is the same as the Iceberg metadata
        version number. Tag creation overwrites the latest metadata file to maintain this.
        There is no need to update the catalog after overwrite.
         */
        lock.ensureValid();
        table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
        LOG.info(
                "Iceberg metadata file {} overwritten to add tag {} for snapshot {}.",
                baseMetadataPath,
                tagName,
                snapshotId);
    }

    @Override
    public void notifyDeletion(String tagName) {
        try {
            runUnderTableLock(
                    lock -> {
                        removeRef(lock, tagName);
                        return null;
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to delete tag " + tagName + " for table " + table.name(), e);
        }
    }

    private void removeRef(Lock lock, String tagName) throws IOException {
        table.snapshotManager().invalidateCache();
        Snapshot latestSnapshot = table.snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            LOG.info(
                    "Latest Iceberg snapshot not found when deleting tag {}. Unable to delete tag.",
                    tagName);
            return;
        }

        Path baseMetadataPath = pathFactory.toMetadataPath(latestSnapshot.id());
        if (!table.fileIO().exists(baseMetadataPath)) {
            LOG.info(
                    "Iceberg metadata file {} not found when deleting tag {}. Unable to delete tag.",
                    baseMetadataPath,
                    tagName);
            return;
        }

        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);

        baseMetadata.refs().remove(tagName);

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.formatVersion(),
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        baseMetadata.currentSnapshotId(),
                        baseMetadata.lastColumnId(),
                        baseMetadata.schemas(),
                        baseMetadata.currentSchemaId(),
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        baseMetadata.snapshots(),
                        baseMetadata.currentSnapshotId(),
                        baseMetadata.nextRowId(),
                        baseMetadata.refs());

        /*
        Overwrite the latest metadata file
        Currently the Paimon table snapshot id value is the same as the Iceberg metadata
        version number. Tag creation overwrites the latest metadata file to maintain this.
        There is no need to update the catalog after overwrite.
         */
        lock.ensureValid();
        table.fileIO().overwriteFileUtf8(baseMetadataPath, metadata.toJson());
        LOG.info(
                "Iceberg metadata file {} overwritten to delete tag {}.",
                baseMetadataPath,
                tagName);
    }

    // -------------------------------------------------------------------------------------
    // Deletion vectors
    // -------------------------------------------------------------------------------------

    private boolean needAddDvToIceberg() {
        CoreOptions options = table.coreOptions();
        // there may be dv indexes using bitmap32 in index files even if 'deletion-vectors.bitmap64'
        // is true, but analyzing all deletion vectors is very costly, so we do not check exactly
        // currently.
        return options.deletionVectorsEnabled()
                && options.deletionVectorBitmap64()
                && formatVersion == IcebergMetadata.FORMAT_VERSION_V3;
    }

    private List<IcebergManifestFileMeta> createDvManifestFileMetas(Snapshot snapshot) {
        List<IcebergManifestEntry> icebergDvEntries = new ArrayList<>();

        long snapshotId = snapshot.id();
        List<IndexManifestEntry> newIndexes =
                indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX);
        if (newIndexes.isEmpty()) {
            return Collections.emptyList();
        }
        for (IndexManifestEntry entry : newIndexes) {
            LinkedHashMap<String, DeletionVectorMeta> dvMetas = entry.indexFile().dvRanges();
            Path bucketPath = fileStorePathFactory.bucketPath(entry.partition(), entry.bucket());
            if (dvMetas != null) {
                for (DeletionVectorMeta dvMeta : dvMetas.values()) {

                    // Iceberg will check the cardinality between deserialized dv and iceberg
                    // deletion file, so if deletionFile.cardinality() is null, we should stop
                    // synchronizing all dvs.
                    Preconditions.checkState(
                            dvMeta.cardinality() != null,
                            "cardinality in DeletionVector is null, stop generate dv for iceberg. "
                                    + "dataFile path is {}, indexFile path is {}",
                            new Path(bucketPath, dvMeta.dataFileName()),
                            indexFileHandler.filePath(entry).toString());

                    IcebergDataFileMeta deleteFileMeta =
                            IcebergDataFileMeta.createForDeleteFile(
                                    IcebergDataFileMeta.Content.POSITION_DELETES,
                                    indexFileHandler.filePath(entry).toString(),
                                    PUFFIN_FORMAT,
                                    entry.partition(),
                                    dvMeta.cardinality(),
                                    entry.indexFile().fileSize(),
                                    new Path(bucketPath, dvMeta.dataFileName()).toString(),
                                    (long) dvMeta.offset(),
                                    (long) dvMeta.length());

                    icebergDvEntries.add(
                            new IcebergManifestEntry(
                                    IcebergManifestEntry.Status.ADDED,
                                    snapshotId,
                                    snapshotId,
                                    snapshotId,
                                    deleteFileMeta));
                }
            }
        }

        if (icebergDvEntries.isEmpty()) {
            return Collections.emptyList();
        }

        return manifestFile.rollingWrite(
                icebergDvEntries.iterator(), snapshotId, IcebergManifestFileMeta.Content.DELETES);
    }

    private Set<BinaryRow> partitionsWithChangedDeleteFiles(
            List<IndexManifestEntry> indexFiles,
            List<IcebergManifestFileMeta> baseDvManifestFileMetas) {
        Set<String> baseDeleteFiles = new HashSet<>();
        for (IcebergManifestFileMeta meta : baseDvManifestFileMetas) {
            for (IcebergManifestEntry entry :
                    manifestFile.read(new Path(meta.manifestPath()).getName())) {
                if (entry.isLive()) {
                    baseDeleteFiles.add(entry.file().filePath());
                }
            }
        }

        Set<BinaryRow> partitions = new HashSet<>();
        for (IndexManifestEntry entry : indexFiles) {
            if (!DELETION_VECTORS_INDEX.equals(entry.indexFile().indexType())) {
                continue;
            }
            boolean inBase = baseDeleteFiles.contains(indexFileHandler.filePath(entry).toString());
            boolean changed;
            if (entry.kind() == FileKind.ADD) {
                // a retry passes every index file of the snapshot as ADD, unchanged ones included
                LinkedHashMap<String, DeletionVectorMeta> dvRanges = entry.indexFile().dvRanges();
                changed = !inBase && dvRanges != null && !dvRanges.isEmpty();
            } else {
                changed = inBase;
            }
            if (changed) {
                partitions.add(entry.partition());
            }
        }
        return partitions;
    }

    // -------------------------------------------------------------------------------------
    // Snapshot Summary Computation
    // -------------------------------------------------------------------------------------

    private static class SummaryMetrics {
        long addedDataFiles;
        long addedRecords;
        long addedFilesSize;
        long deletedDataFiles;
        long deletedRecords;
        long deletedFilesSize;
        long changedPartitionCount;
        long totalDataFiles;
        long totalRecords;
        long totalFilesSize;
        long totalDeleteFiles;
        long totalPositionDeletes;
        long totalEqualityDeletes;
    }

    /**
     * Summary entry identifying the Paimon snapshot this metadata was built from; it tells live
     * metadata from metadata a rollback abandoned.
     */
    static final String SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY = "paimon-commit-identity";

    /**
     * The manifest lists that a replay replaced with copies for a snapshot, as a JSON array of file
     * names in the metadata directory. Readers of older metadata may still use them, so the replay
     * leaves them. They go with the snapshot, when retention expires it, and this record is how
     * that cleanup finds them after the metadata versions that listed them are deleted.
     */
    static final String SNAPSHOT_SUMMARY_REPLAY_ORIGINALS = "paimon-replay-originals";

    static String commitIdentity(Snapshot snapshot) {
        // snapshot uuid when present; legacy snapshots fall back to user/identifier/time
        if (snapshot.uuid() != null) {
            return snapshot.uuid();
        }
        return snapshot.commitUser()
                + ":"
                + snapshot.commitIdentifier()
                + ":"
                + snapshot.timeMillis();
    }

    private IcebergSnapshotSummary computeSnapshotSummary(
            String operation, Snapshot snapshot, SummaryMetrics metrics) {

        IcebergSnapshotSummary summary = new IcebergSnapshotSummary(operation);

        long addedDataFiles = Math.max(0, metrics.addedDataFiles);
        long addedRecords = Math.max(0, metrics.addedRecords);
        long addedFilesSize = Math.max(0, metrics.addedFilesSize);
        long deletedDataFiles = Math.max(0, metrics.deletedDataFiles);
        long deletedRecords = Math.max(0, metrics.deletedRecords);
        long deletedFilesSize = Math.max(0, metrics.deletedFilesSize);
        long changedPartitionCount = Math.max(0, metrics.changedPartitionCount);
        long totalRecords = Math.max(0, metrics.totalRecords);
        long totalDataFiles = Math.max(0, metrics.totalDataFiles);
        long totalFilesSize = Math.max(0, metrics.totalFilesSize);
        long totalDeleteFiles = Math.max(0, metrics.totalDeleteFiles);
        long totalPositionDeletes = Math.max(0, metrics.totalPositionDeletes);
        long totalEqualityDeletes = Math.max(0, metrics.totalEqualityDeletes);

        summary.put(SNAPSHOT_SUMMARY_ADDED_DATA_FILES, Long.toString(addedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_ADDED_RECORDS, Long.toString(addedRecords));
        summary.put(SNAPSHOT_SUMMARY_ADDED_FILES_SIZE, Long.toString(addedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_DELETED_DATA_FILES, Long.toString(deletedDataFiles));
        summary.put(SNAPSHOT_SUMMARY_DELETED_RECORDS, Long.toString(deletedRecords));
        summary.put(SNAPSHOT_SUMMARY_REMOVED_FILES_SIZE, Long.toString(deletedFilesSize));
        summary.put(SNAPSHOT_SUMMARY_CHANGED_PARTITION_COUNT, Long.toString(changedPartitionCount));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_RECORDS, Long.toString(totalRecords));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DATA_FILES, Long.toString(totalDataFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE, Long.toString(totalFilesSize));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_DELETE_FILES, Long.toString(totalDeleteFiles));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_POSITION_DELETES, Long.toString(totalPositionDeletes));
        summary.put(SNAPSHOT_SUMMARY_TOTAL_EQUALITY_DELETES, Long.toString(totalEqualityDeletes));

        Map<String, String> properties = snapshot.properties();
        if (properties != null) {
            properties.forEach(
                    (key, value) -> {
                        // the record of replayed originals drives deletions, so only a replay
                        // writes it
                        if (value != null && !SNAPSHOT_SUMMARY_REPLAY_ORIGINALS.equals(key)) {
                            summary.put(key, value);
                        }
                    });
        }
        // after the user-property copy, so a same-key property cannot overwrite it
        summary.put(SNAPSHOT_SUMMARY_PAIMON_COMMIT_IDENTITY, commitIdentity(snapshot));

        return summary;
    }

    private long computeLiveFileCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream().mapToLong(IcebergManifestFileMeta::liveFilesCount).sum();
    }

    private long computeLiveRowCount(List<IcebergManifestFileMeta> manifestMetas) {
        return manifestMetas.stream().mapToLong(IcebergManifestFileMeta::liveRowsCount).sum();
    }

    /**
     * Whether the base summary's total file size can be reused. File sizes are not in the manifest
     * list, so the total is only trusted while the base's file count matches its manifests.
     * Otherwise every manifest entry has to be read to recompute it.
     */
    boolean baseTotalsUsable(
            IcebergSnapshot baseSnapshot, List<IcebergManifestFileMeta> baseDataManifestFileMetas) {
        Long totalDataFiles = getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_DATA_FILES);
        Long totalFilesSize = getSummaryLong(baseSnapshot, SNAPSHOT_SUMMARY_TOTAL_FILES_SIZE);
        return totalFilesSize != null
                && totalDataFiles != null
                && totalDataFiles == computeLiveFileCount(baseDataManifestFileMetas);
    }

    @Nullable
    Long getSummaryLong(@Nullable IcebergSnapshot snapshot, String key) {
        if (snapshot == null) {
            return null;
        }
        Map<String, String> summaryMap = snapshot.summary().getSummary();
        String value = summaryMap.get(key);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            LOG.warn(
                    "Unable to parse snapshot summary field {}={} as long. The value will be recomputed.",
                    key,
                    value);
            return null;
        }
    }

    private long computeTotalFilesSizeFromManifests(List<IcebergManifestFileMeta> manifestMetas)
            throws IOException {
        long total = 0;
        for (IcebergManifestFileMeta meta : manifestMetas) {
            for (IcebergManifestEntry entry :
                    manifestFile.read(new Path(meta.manifestPath()).getName())) {
                if (entry.isLive()) {
                    total += entry.file().fileSizeInBytes();
                }
            }
        }
        return total;
    }

    // -------------------------------------------------------------------------------------
    // Gap replay
    // -------------------------------------------------------------------------------------

    /** The points of a replay that tests can observe and interrupt. */
    enum ReplayPoint {
        /** A base candidate was accepted, the value is its version. */
        BASE_SELECTED,
        /** A base candidate was rejected, the value is its version. */
        CANDIDATE_REJECTED,
        /** The lists of the retained history were read and charged, nothing is copied yet. */
        LISTS_READ,
        /** The retained history is copied, the value is the number of copied manifests. */
        COPIED,
        /** A snapshot was replayed, the value is its snapshot id. */
        STEP_DONE,
        /** The metadata is complete, its file is not written yet. */
        BEFORE_WRITE,
        /** A publication holds the table lock and checked its source, nothing is written yet. */
        LOCKED,
        /** A cleanup of files that a publication made unneeded is about to take the lock. */
        BEFORE_CLEANUP,
        /** A cleanup holds the table lock and checked its publication, nothing is deleted yet. */
        CLEANUP_LOCKED,
        /** The metadata file of the replay is durable. */
        EXPOSED,
        /** The replay was dropped and nothing was written for the snapshot. */
        ABANDONED,
        /** A rebuild from every data file starts. */
        REBUILD
    }

    /** Observes every {@link ReplayPoint}, and may throw to interrupt the replay. */
    @FunctionalInterface
    interface ReplayHook {
        void at(ReplayPoint point, long value);
    }

    /** What a rebuild from the table inherits from the metadata it replaces. */
    static final class RebuildRequest {

        @Nullable final String inheritUuid;
        final int lastColumnIdFloor;
        final long nextRowIdFloor;

        RebuildRequest(@Nullable String inheritUuid, int lastColumnIdFloor, long nextRowIdFloor) {
            this.inheritUuid = inheritUuid;
            this.lastColumnIdFloor = lastColumnIdFloor;
            this.nextRowIdFloor = nextRowIdFloor;
        }
    }

    /** The reason a replay is not attempted, or null when the table is eligible. */
    @Nullable
    private String replayIneligibleReason() {
        CoreOptions options = table.coreOptions();
        if (!table.primaryKeys().isEmpty()) {
            return "the table has primary keys";
        }
        if (options.deletionVectorsEnabled()) {
            return "deletion vectors are enabled";
        }
        if (options.dataEvolutionEnabled()) {
            return "data evolution is enabled";
        }
        if (formatVersion != IcebergMetadata.FORMAT_VERSION_V2) {
            return "the Iceberg format version is " + formatVersion;
        }
        switch (storageType) {
            case TABLE_LOCATION:
            case HIVE_CATALOG:
            case HADOOP_CATALOG:
                return null;
            default:
                return "the storage type is " + storageType;
        }
    }

    /**
     * Replays the Iceberg snapshots after the newest valid older metadata version instead of
     * rebuilding from every data file. Returns null when nothing is left to do, and what a rebuild
     * has to inherit when no replay was possible, or it failed before it exposed anything, and the
     * snapshot is still the head.
     */
    @Nullable
    private RebuildRequest replayOrRebuild(
            Snapshot snapshot, FileChangesCollector collector, RebuildRequest rebuild)
            throws IOException {
        if (snapshot.id() == Snapshot.FIRST_SNAPSHOT_ID) {
            return rebuild;
        }
        String reason = replayIneligibleReason();
        if (reason != null) {
            LOG.warn(
                    "Table {} does not replay missing Iceberg metadata ({}), so it is rebuilt.",
                    table.fullName(),
                    reason);
            return rebuild;
        }
        // A rollback can replace the snapshot after the last check of it and before the metadata
        // is written, and only a lock that the rollbacks and the commits of the table take closes
        // that. Without one the replay would publish metadata of the wrong timeline, and a
        // rebuild would too, so neither is attempted.
        if (!tableLockCoordinates()) {
            throw new IllegalStateException(
                    String.format(
                            "%s = %s needs a lock that keeps the other commits and rollbacks of "
                                    + "table %s out while its Iceberg metadata is published, and "
                                    + "no lock is configured for it. Configure a lock for the "
                                    + "catalog, or set %s = %s.",
                            IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(),
                            IcebergOptions.MissingBaseStrategy.REPLAY,
                            table.fullName(),
                            IcebergOptions.METADATA_MISSING_BASE_STRATEGY.key(),
                            IcebergOptions.MissingBaseStrategy.REBUILD));
        }
        return new IcebergGapReplay(this, snapshot, collector, rebuild).run();
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    private boolean isSameFormatVersion(int baseFormatVersion) {
        if (baseFormatVersion != formatVersion) {
            Preconditions.checkArgument(
                    formatVersion > baseFormatVersion,
                    "format version in base metadata is {}, and it's bigger than the current format version {}, "
                            + "this is not allowed!");

            LOG.info(
                    "format version in base metadata is {}, and it's different from the current format version {}. "
                            + "New metadata will be recreated using format version {}.",
                    baseFormatVersion,
                    formatVersion,
                    formatVersion);
            return false;
        }
        return true;
    }

    /**
     * Row-lineage bookkeeping for a new snapshot, mandatory in Iceberg format version 3: the
     * snapshot's first-row-id starts at the base metadata's next-row-id watermark. The snapshot's
     * added-rows and the table's next-row-id are NOT derived here: they depend on how many rows
     * {@link #assignManifestFirstRowIds} actually assigns (which can exceed this commit's added
     * records when a carried-over manifest is assigned for the first time, e.g. a Layer-1-written
     * manifest being upgraded), so callers must recompute them from the assignment's result. For
     * format version 2 the field stays null so nothing is written.
     */
    @Nullable
    private Long computeSnapshotFirstRowId(long baseNextRowId) {
        return formatVersion >= IcebergMetadata.FORMAT_VERSION_V3 ? baseNextRowId : null;
    }

    /**
     * Result of {@link #assignManifestFirstRowIds}: the manifests with first_row_id assigned, and
     * the total number of rows actually consumed from the row-id space by that assignment (which
     * may be larger than this commit's added-records count; see the class-level note there).
     */
    private static class ManifestRowIdAssignment {
        private final List<IcebergManifestFileMeta> manifests;
        private final long assignedRows;

        private ManifestRowIdAssignment(
                List<IcebergManifestFileMeta> manifests, long assignedRows) {
            this.manifests = manifests;
            this.assignedRows = assignedRows;
        }
    }

    /**
     * Iceberg v3: assign first_row_id (field 520) to data manifests that do not have one yet.
     * Manifests carried over from base metadata that are already assigned keep their value; delete
     * manifests stay null. The watermark starts at the snapshot's first-row-id and advances by each
     * newly-assigned manifest's TRUE inheriting-rows count (see {@link #trueInheritingRowsCount}),
     * returned as {@link ManifestRowIdAssignment#assignedRows}.
     *
     * <p>A manifest written entirely under manifest-level assignment satisfies "inheriting rows ==
     * ADDED rows", so the bound is exact for it. A manifest carried over from before assignment
     * existed may hold EXISTING entries whose field 142 is also still null; the bound covers them
     * without reading the manifest, at the cost of spec-legal id gaps when some of those entries
     * were already materialized. DELETED entries never inherit ids and are excluded. Callers MUST
     * use {@code assignedRows} (not this commit's added-records count) to advance the snapshot's
     * added-rows / table next-row-id, precisely because of that mismatch.
     */
    private ManifestRowIdAssignment assignManifestFirstRowIds(
            List<IcebergManifestFileMeta> manifests, @Nullable Long snapshotFirstRowId) {
        if (snapshotFirstRowId == null) {
            return new ManifestRowIdAssignment(manifests, 0L);
        }
        List<IcebergManifestFileMeta> result = new ArrayList<>();
        long watermark = snapshotFirstRowId;
        for (IcebergManifestFileMeta meta : manifests) {
            if (meta.content() == IcebergManifestFileMeta.Content.DATA
                    && meta.firstRowId() == null) {
                result.add(meta.withFirstRowId(watermark));
                // spec-sanctioned upper bound: only ADDED and EXISTING rows can inherit
                // ids from this manifest (readers never assign ids to DELETED entries).
                // Rows whose field 142 is already materialized merely widen the reserved
                // range, leaving legal id gaps - in exchange the commit path never has to
                // read manifest contents.
                watermark += meta.addedRowsCount() + meta.existingRowsCount();
            } else {
                result.add(meta);
            }
        }
        return new ManifestRowIdAssignment(result, watermark - snapshotFirstRowId);
    }

    /**
     * Iceberg v3 requires the inherited first_row_id to be written into file metadata when entries
     * are copied into a rewritten manifest. Computes each entry's effective id in base manifest
     * order (explicit field 142, or inherited from the manifest's first_row_id, skipping DELETED
     * entries exactly like GA readers do) and returns entries with the id materialized. No-op for
     * delete manifests and for base manifests without an assigned first_row_id (v2 metadata, or v3
     * metadata written before manifest-level assignment existed — those stay in the spec's
     * upgraded-table state).
     */
    private static List<IcebergManifestEntry> materializeFirstRowIds(
            IcebergManifestFileMeta baseMeta, List<IcebergManifestEntry> entries) {
        if (baseMeta.content() != IcebergManifestFileMeta.Content.DATA
                || baseMeta.firstRowId() == null) {
            return entries;
        }
        List<IcebergManifestEntry> result = new ArrayList<>();
        long watermark = baseMeta.firstRowId();
        for (IcebergManifestEntry entry : entries) {
            if (entry.status() != IcebergManifestEntry.Status.DELETED
                    && entry.file().firstRowId() == null) {
                result.add(entry.withFile(entry.file().withFirstRowId(watermark)));
                watermark += entry.file().recordCount();
            } else {
                // DELETED entries never inherit an id (GA readers skip them when
                // assigning), so their field 142 stays null and the walk does not advance
                result.add(entry);
            }
        }
        return result;
    }

    class SchemaCache {

        SchemaManager schemaManager = table.schemaManager();
        Map<Long, IcebergSchema> schemas = new HashMap<>();

        IcebergSchema get(long schemaId) {
            return schemas.computeIfAbsent(
                    schemaId,
                    id -> {
                        TableSchema schema = schemaManager.schema(id);
                        if (formatVersion < IcebergMetadata.FORMAT_VERSION_V3) {
                            // VARIANT is an Iceberg format-version-3 type; v2 metadata cannot
                            // represent it
                            checkVariantNotPublishable(schema.logicalRowType());
                        }
                        SchemaValidation.validateIcebergGeospatialTypes(
                                schema.logicalRowType(), table.coreOptions());
                        SchemaValidation.validateIcebergTimestampPrecisions(
                                schema.logicalRowType(), table.coreOptions());
                        return IcebergSchema.create(schema);
                    });
        }

        long getLatestSchemaId() {
            return schemaManager.latest().get().id();
        }
    }
}
