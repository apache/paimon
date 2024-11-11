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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.factories.FactoryUtil;
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
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link CommitCallback} to create Iceberg compatible metadata, so Iceberg readers can read
 * Paimon's {@link RawFile}.
 */
public abstract class AbstractIcebergCommitCallback implements CommitCallback {

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    protected final FileStoreTable table;
    private final String commitUser;

    private final IcebergPathFactory pathFactory;
    private final @Nullable IcebergMetadataCommitter metadataCommitter;

    private final FileStorePathFactory fileStorePathFactory;
    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;

    // -------------------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------------------

    public AbstractIcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;

        IcebergOptions.StorageType storageType =
                table.coreOptions().toConfiguration().get(IcebergOptions.METADATA_ICEBERG_STORAGE);
        switch (storageType) {
            case TABLE_LOCATION:
                this.pathFactory = new IcebergPathFactory(new Path(table.location(), "metadata"));
                break;
            case HADOOP_CATALOG:
            case HIVE_CATALOG:
                Path dbPath = table.location().getParent();
                final String dbSuffix = ".db";
                if (dbPath.getName().endsWith(dbSuffix)) {
                    String dbName =
                            dbPath.getName()
                                    .substring(0, dbPath.getName().length() - dbSuffix.length());
                    String tableName = table.location().getName();
                    Path separatePath =
                            new Path(
                                    dbPath.getParent(),
                                    String.format("iceberg/%s/%s/metadata", dbName, tableName));
                    this.pathFactory = new IcebergPathFactory(separatePath);
                } else {
                    throw new UnsupportedOperationException(
                            "Storage type ICEBERG_WAREHOUSE can only be used on Paimon tables in a Paimon warehouse.");
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown storage type " + storageType.name());
        }

        IcebergMetadataCommitterFactory metadataCommitterFactory;
        try {
            metadataCommitterFactory =
                    FactoryUtil.discoverFactory(
                            AbstractIcebergCommitCallback.class.getClassLoader(),
                            IcebergMetadataCommitterFactory.class,
                            storageType.toString());
        } catch (FactoryException ignore) {
            metadataCommitterFactory = null;
        }
        this.metadataCommitter =
                metadataCommitterFactory == null ? null : metadataCommitterFactory.create(table);

        this.fileStorePathFactory = table.store().pathFactory();
        this.manifestFile = IcebergManifestFile.create(table, pathFactory);
        this.manifestList = IcebergManifestList.create(table, pathFactory);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void call(List<ManifestEntry> committedEntries, Snapshot snapshot) {
        createMetadata(
                snapshot.id(),
                (removedFiles, addedFiles) ->
                        collectFileChanges(committedEntries, removedFiles, addedFiles));
    }

    @Override
    public void retry(ManifestCommittable committable) {
        SnapshotManager snapshotManager = table.snapshotManager();
        long snapshotId =
                snapshotManager
                        .findSnapshotsForIdentifiers(
                                commitUser, Collections.singletonList(committable.identifier()))
                        .stream()
                        .mapToLong(Snapshot::id)
                        .max()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "There is no snapshot for commit user "
                                                        + commitUser
                                                        + " and identifier "
                                                        + committable.identifier()
                                                        + ". This is unexpected."));
        createMetadata(
                snapshotId,
                (removedFiles, addedFiles) ->
                        collectFileChanges(snapshotId, removedFiles, addedFiles));
    }

    private void createMetadata(long snapshotId, FileChangesCollector fileChangesCollector) {
        try {
            if (snapshotId == Snapshot.FIRST_SNAPSHOT_ID) {
                // If Iceberg metadata is stored separately in another directory, dropping the table
                // will not delete old Iceberg metadata. So we delete them here, when the table is
                // created again and the first snapshot is committed.
                table.fileIO().delete(pathFactory.metadataDirectory(), true);
            }

            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(snapshotId - 1);
            if (table.fileIO().exists(baseMetadataPath)) {
                createMetadataWithBase(fileChangesCollector, snapshotId, baseMetadataPath);
            } else {
                createMetadataWithoutBase(snapshotId);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // -------------------------------------------------------------------------------------
    // Create metadata afresh
    // -------------------------------------------------------------------------------------

    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        SchemaCache schemaCache = new SchemaCache();
        Iterator<IcebergManifestEntry> entryIterator =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .flatMap(
                                s ->
                                        dataSplitToManifestEntries(s, snapshotId, schemaCache)
                                                .stream())
                        .iterator();
        List<IcebergManifestFileMeta> manifestFileMetas =
                manifestFile.rollingWrite(entryIterator, snapshotId);
        String manifestListFileName = manifestList.writeWithoutRolling(manifestFileMetas);

        int schemaId = (int) table.schema().id();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergPartitionField> partitionFields =
                getPartitionFields(table.schema().partitionKeys(), icebergSchema);
        IcebergSnapshot snapshot =
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        System.currentTimeMillis(),
                        IcebergSnapshotSummary.APPEND,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId);

        String tableUuid = UUID.randomUUID().toString();
        IcebergMetadata metadata =
                new IcebergMetadata(
                        tableUuid,
                        table.location().toString(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        Collections.singletonList(icebergSchema),
                        schemaId,
                        Collections.singletonList(new IcebergPartitionSpec(partitionFields)),
                        partitionFields.stream()
                                .mapToInt(IcebergPartitionField::fieldId)
                                .max()
                                .orElse(
                                        // not sure why, this is a result tested by hand
                                        IcebergPartitionField.FIRST_FIELD_ID - 1),
                        Collections.singletonList(snapshot),
                        (int) snapshotId);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));

        expireAllBefore(snapshotId);

        if (metadataCommitter != null) {
            metadataCommitter.commitMetadata(metadataPath, null);
        }
    }

    private List<IcebergManifestEntry> dataSplitToManifestEntries(
            DataSplit dataSplit, long snapshotId, SchemaCache schemaCache) {
        List<IcebergManifestEntry> result = new ArrayList<>();
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
                            paimonFileMeta.valueStats());
            result.add(
                    new IcebergManifestEntry(
                            IcebergManifestEntry.Status.ADDED,
                            snapshotId,
                            snapshotId,
                            snapshotId,
                            fileMeta));
        }
        return result;
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

    // -------------------------------------------------------------------------------------
    // Create metadata based on old ones
    // -------------------------------------------------------------------------------------

    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector, long snapshotId, Path baseMetadataPath)
            throws IOException {
        IcebergMetadata baseMetadata = IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath);
        List<IcebergManifestFileMeta> baseManifestFileMetas =
                manifestList.read(baseMetadata.currentSnapshot().manifestList());

        Map<String, BinaryRow> removedFiles = new LinkedHashMap<>();
        Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles = new LinkedHashMap<>();
        boolean isAddOnly = fileChangesCollector.collect(removedFiles, addedFiles);
        Set<BinaryRow> modifiedPartitionsSet = new LinkedHashSet<>(removedFiles.values());
        modifiedPartitionsSet.addAll(
                addedFiles.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
        List<BinaryRow> modifiedPartitions = new ArrayList<>(modifiedPartitionsSet);

        // Note that this check may be different from `removedFiles.isEmpty()`,
        // because if a file's level is changed, it will first be removed and then added.
        // In this case, if `baseMetadata` already contains this file, we should not add a
        // duplicate.
        List<IcebergManifestFileMeta> newManifestFileMetas;
        IcebergSnapshotSummary snapshotSummary;
        if (isAddOnly) {
            // Fast case. We don't need to remove files from `baseMetadata`. We only need to append
            // new metadata files.
            newManifestFileMetas = new ArrayList<>(baseManifestFileMetas);
            newManifestFileMetas.addAll(createNewlyAddedManifestFileMetas(addedFiles, snapshotId));
            snapshotSummary = IcebergSnapshotSummary.APPEND;
        } else {
            Pair<List<IcebergManifestFileMeta>, IcebergSnapshotSummary> result =
                    createWithDeleteManifestFileMetas(
                            removedFiles,
                            addedFiles,
                            modifiedPartitions,
                            baseManifestFileMetas,
                            snapshotId);
            newManifestFileMetas = result.getLeft();
            snapshotSummary = result.getRight();
        }
        String manifestListFileName =
                manifestList.writeWithoutRolling(
                        compactMetadataIfNeeded(newManifestFileMetas, snapshotId));

        // add new schema if needed
        SchemaCache schemaCache = new SchemaCache();
        int schemaId = (int) table.schema().id();
        IcebergSchema icebergSchema = schemaCache.get(schemaId);
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (baseMetadata.currentSchemaId() != schemaId) {
            schemas = new ArrayList<>(schemas);
            schemas.add(icebergSchema);
        }

        List<IcebergSnapshot> snapshots = new ArrayList<>(baseMetadata.snapshots());
        snapshots.add(
                new IcebergSnapshot(
                        snapshotId,
                        snapshotId,
                        System.currentTimeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId));

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

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        icebergSchema.highestFieldId(),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId);

        Path metadataPath = pathFactory.toMetadataPath(snapshotId);
        table.fileIO().tryToWriteAtomic(metadataPath, metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));

        table.fileIO().deleteQuietly(baseMetadataPath);
        for (int i = 0; i + 1 < toExpireExceptLast.size(); i++) {
            expireManifestList(
                    new Path(toExpireExceptLast.get(i).manifestList()).getName(),
                    new Path(toExpireExceptLast.get(i + 1).manifestList()).getName());
        }

        if (metadataCommitter != null) {
            metadataCommitter.commitMetadata(metadataPath, baseMetadataPath);
        }
    }

    private interface FileChangesCollector {
        boolean collect(
                Map<String, BinaryRow> removedFiles,
                Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles)
                throws IOException;
    }

    private boolean collectFileChanges(
            List<ManifestEntry> manifestEntries,
            Map<String, BinaryRow> removedFiles,
            Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles) {
        boolean isAddOnly = true;
        for (ManifestEntry entry : manifestEntries) {
            String path =
                    fileStorePathFactory.bucketPath(entry.partition(), entry.bucket())
                            + "/"
                            + entry.fileName();
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
                    removedFiles.put(path, entry.partition());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown ManifestEntry FileKind " + entry.kind());
            }
        }
        return isAddOnly;
    }

    private boolean collectFileChanges(
            long snapshotId,
            Map<String, BinaryRow> removedFiles,
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

    protected abstract boolean shouldAddFileToIceberg(DataFileMeta meta);

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
                                                    paimonFileMeta.valueStats());
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

    private Pair<List<IcebergManifestFileMeta>, IcebergSnapshotSummary>
            createWithDeleteManifestFileMetas(
                    Map<String, BinaryRow> removedFiles,
                    Map<String, Pair<BinaryRow, DataFileMeta>> addedFiles,
                    List<BinaryRow> modifiedPartitions,
                    List<IcebergManifestFileMeta> baseManifestFileMetas,
                    long currentSnapshotId)
                    throws IOException {
        IcebergSnapshotSummary snapshotSummary = IcebergSnapshotSummary.APPEND;
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();

        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitions);

        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            long[] nullCounts = new long[numFields];
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                minValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.lowerBound()));
                maxValues.setField(
                        i, IcebergConversions.toPaimonObject(fieldType, summary.upperBound()));
                // IcebergPartitionSummary only has `containsNull` field and does not have the
                // exact number of nulls.
                nullCounts[i] = summary.containsNull() ? 1 : 0;
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
                    snapshotSummary = IcebergSnapshotSummary.OVERWRITE;
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : entries) {
                        if (entry.isLive()) {
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removedFiles.containsKey(entry.file().filePath())
                                                    ? IcebergManifestEntry.Status.DELETED
                                                    : IcebergManifestEntry.Status.EXISTING,
                                            entry.snapshotId(),
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
        return Pair.of(newManifestFileMetas, snapshotSummary);
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
                    List<IcebergManifestEntry> entries = new ArrayList<>();
                    for (IcebergManifestEntry entry :
                            IcebergManifestFile.create(table, pathFactory)
                                    .read(new Path(meta.manifestPath()).getName())) {
                        if (entry.fileSequenceNumber() == currentSnapshotId
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

    private boolean shouldExpire(IcebergSnapshot snapshot, long currentSnapshotId) {
        Options options = new Options(table.options());
        if (snapshot.snapshotId()
                > currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN)) {
            return false;
        }
        if (snapshot.snapshotId()
                <= currentSnapshotId - options.get(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX)) {
            return true;
        }
        return snapshot.timestampMs()
                < System.currentTimeMillis()
                        - options.get(CoreOptions.SNAPSHOT_TIME_RETAINED).toMillis();
    }

    private void expireManifestList(String toExpire, String next) {
        Set<IcebergManifestFileMeta> metaInUse = new HashSet<>(manifestList.read(next));
        for (IcebergManifestFileMeta meta : manifestList.read(toExpire)) {
            if (metaInUse.contains(meta)) {
                continue;
            }
            table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
        }
        table.fileIO().deleteQuietly(pathFactory.toManifestListPath(toExpire));
    }

    private void expireAllBefore(long snapshotId) throws IOException {
        Set<String> expiredManifestLists = new HashSet<>();
        Set<String> expiredManifestFileMetas = new HashSet<>();
        Iterator<Path> it =
                pathFactory.getAllMetadataPathBefore(table.fileIO(), snapshotId).iterator();

        while (it.hasNext()) {
            Path path = it.next();
            IcebergMetadata metadata = IcebergMetadata.fromPath(table.fileIO(), path);

            for (IcebergSnapshot snapshot : metadata.snapshots()) {
                Path listPath = new Path(snapshot.manifestList());
                String listName = listPath.getName();
                if (expiredManifestLists.contains(listName)) {
                    continue;
                }
                expiredManifestLists.add(listName);

                for (IcebergManifestFileMeta meta : manifestList.read(listName)) {
                    String metaName = new Path(meta.manifestPath()).getName();
                    if (expiredManifestFileMetas.contains(metaName)) {
                        continue;
                    }
                    expiredManifestFileMetas.add(metaName);
                    table.fileIO().deleteQuietly(new Path(meta.manifestPath()));
                }
                table.fileIO().deleteQuietly(listPath);
            }
            table.fileIO().deleteQuietly(path);
        }
    }

    // -------------------------------------------------------------------------------------
    // Utils
    // -------------------------------------------------------------------------------------

    private class SchemaCache {

        SchemaManager schemaManager = new SchemaManager(table.fileIO(), table.location());
        Map<Long, IcebergSchema> schemas = new HashMap<>();

        private IcebergSchema get(long schemaId) {
            return schemas.computeIfAbsent(
                    schemaId, id -> IcebergSchema.create(schemaManager.schema(id)));
        }
    }
}
