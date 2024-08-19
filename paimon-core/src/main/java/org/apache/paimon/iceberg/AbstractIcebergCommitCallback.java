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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergConversions;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.manifest.IcebergPartitionSummary;
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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
    private final FileStorePathFactory fileStorePathFactory;

    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;

    public AbstractIcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;
        this.pathFactory = new IcebergPathFactory(table.location());
        this.fileStorePathFactory = table.store().pathFactory();

        RowType partitionType = table.schema().logicalPartitionType();
        RowType entryType = IcebergManifestEntry.schema(partitionType);
        Options manifestFileAvroOptions = Options.fromMap(table.options());
        // https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/ManifestReader.java
        manifestFileAvroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_entry,"
                        + "manifest_entry_data_file:r2,"
                        + "r2_partition:r102");
        FileFormat manifestFileAvro = FileFormat.getFileFormat(manifestFileAvroOptions, "avro");
        this.manifestFile =
                new IcebergManifestFile(
                        table.fileIO(),
                        partitionType,
                        manifestFileAvro.createReaderFactory(entryType),
                        manifestFileAvro.createWriterFactory(entryType),
                        table.coreOptions().manifestCompression(),
                        pathFactory.manifestFileFactory(),
                        table.coreOptions().manifestTargetSize());

        Options manifestListAvroOptions = Options.fromMap(table.options());
        // https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/ManifestLists.java
        manifestListAvroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_file,"
                        + "manifest_file_partitions:r508");
        FileFormat manifestListAvro = FileFormat.getFileFormat(manifestListAvroOptions, "avro");
        this.manifestList =
                new IcebergManifestList(
                        table.fileIO(),
                        manifestListAvro.createReaderFactory(IcebergManifestFileMeta.schema()),
                        manifestListAvro.createWriterFactory(IcebergManifestFileMeta.schema()),
                        table.coreOptions().manifestCompression(),
                        pathFactory.manifestListFactory());
    }

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
            if (table.fileIO().exists(pathFactory.toMetadataPath(snapshotId))) {
                return;
            }

            Path baseMetadataPath = pathFactory.toMetadataPath(snapshotId - 1);
            if (table.fileIO().exists(baseMetadataPath)) {
                createMetadataWithBase(
                        fileChangesCollector,
                        snapshotId,
                        IcebergMetadata.fromPath(table.fileIO(), baseMetadataPath));
            } else {
                createMetadataWithoutBase(snapshotId);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        Iterator<IcebergManifestEntry> entryIterator =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .flatMap(s -> dataSplitToManifestEntries(s, snapshotId).stream())
                        .iterator();
        List<IcebergManifestFileMeta> manifestFileMetas =
                manifestFile.rollingWrite(entryIterator, snapshotId);
        String manifestListFileName = manifestList.writeWithoutRolling(manifestFileMetas);

        List<IcebergPartitionField> partitionFields =
                getPartitionFields(table.schema().logicalPartitionType());
        int schemaId = (int) table.schema().id();
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
                        table.schema().highestFieldId(),
                        Collections.singletonList(new IcebergSchema(table.schema())),
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
        table.fileIO().tryToWriteAtomic(pathFactory.toMetadataPath(snapshotId), metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));
    }

    private List<IcebergManifestEntry> dataSplitToManifestEntries(
            DataSplit dataSplit, long snapshotId) {
        List<IcebergManifestEntry> result = new ArrayList<>();
        for (RawFile rawFile : dataSplit.convertToRawFiles().get()) {
            IcebergDataFileMeta fileMeta =
                    new IcebergDataFileMeta(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize());
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

    private void createMetadataWithBase(
            FileChangesCollector fileChangesCollector,
            long snapshotId,
            IcebergMetadata baseMetadata)
            throws IOException {
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
        String manifestListFileName = manifestList.writeWithoutRolling(newManifestFileMetas);

        // add new schema if needed
        int schemaId = (int) table.schema().id();
        List<IcebergSchema> schemas = baseMetadata.schemas();
        if (baseMetadata.currentSchemaId() != schemaId) {
            schemas = new ArrayList<>(schemas);
            schemas.add(new IcebergSchema(table.schema()));
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

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        snapshotId,
                        table.schema().highestFieldId(),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) snapshotId);
        table.fileIO().tryToWriteAtomic(pathFactory.toMetadataPath(snapshotId), metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(snapshotId));
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

        return manifestFile.rollingWrite(
                addedFiles.entrySet().stream()
                        .map(
                                e -> {
                                    IcebergDataFileMeta fileMeta =
                                            new IcebergDataFileMeta(
                                                    IcebergDataFileMeta.Content.DATA,
                                                    e.getKey(),
                                                    e.getValue().getRight().fileFormat(),
                                                    e.getValue().getLeft(),
                                                    e.getValue().getRight().rowCount(),
                                                    e.getValue().getRight().fileSize());
                                    return new IcebergManifestEntry(
                                            IcebergManifestEntry.Status.ADDED,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            currentSnapshotId,
                                            fileMeta);
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
            }
        }

        newManifestFileMetas.addAll(
                createNewlyAddedManifestFileMetas(addedFiles, currentSnapshotId));
        return Pair.of(newManifestFileMetas, snapshotSummary);
    }

    private List<IcebergPartitionField> getPartitionFields(RowType partitionType) {
        List<IcebergPartitionField> result = new ArrayList<>();
        int fieldId = IcebergPartitionField.FIRST_FIELD_ID;
        for (DataField field : partitionType.getFields()) {
            result.add(new IcebergPartitionField(field, fieldId));
            fieldId++;
        }
        return result;
    }

    @Override
    public void close() throws Exception {}
}
