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
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link CommitCallback} to create Iceberg compatible metadata, so Iceberg readers can read
 * Paimon's {@link RawFile}.
 */
public class IcebergCommitCallback implements CommitCallback {

    // see org.apache.iceberg.hadoop.Util
    private static final String VERSION_HINT_FILENAME = "version-hint.text";

    private final FileStoreTable table;
    private final String commitUser;
    private final IcebergPathFactory pathFactory;

    private final IcebergManifestFile manifestFile;
    private final IcebergManifestList manifestList;

    public IcebergCommitCallback(FileStoreTable table, String commitUser) {
        this.table = table;
        this.commitUser = commitUser;
        this.pathFactory = new IcebergPathFactory(table.location());

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
    public void call(
            List<ManifestEntry> committedEntries, long identifier, @Nullable Long watermark) {
        try {
            commitMetadata(identifier);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void retry(ManifestCommittable committable) {
        try {
            commitMetadata(committable.identifier());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void commitMetadata(long identifier) throws IOException {
        Pair<Long, Long> pair = getCurrentAndBaseSnapshotIds(identifier);
        long currentSnapshot = pair.getLeft();
        Long baseSnapshot = pair.getRight();

        if (baseSnapshot == null) {
            createMetadataWithoutBase(currentSnapshot);
        } else {
            createMetadataWithBase(committable, currentSnapshot, baseSnapshot);
        }
    }

    private Pair<Long, Long> getCurrentAndBaseSnapshotIds(long commitIdentifier) {
        SnapshotManager snapshotManager = table.snapshotManager();
        List<Snapshot> currentSnapshots =
                snapshotManager.findSnapshotsForIdentifiers(
                        commitUser, Collections.singletonList(commitIdentifier));
        Preconditions.checkArgument(
                currentSnapshots.size() == 1,
                "Cannot find snapshot with user {} and identifier {}",
                commitUser,
                commitIdentifier);
        long currentSnapshotId = currentSnapshots.get(0).id();

        long earliest =
                Preconditions.checkNotNull(
                        snapshotManager.earliestSnapshotId(),
                        "Cannot determine earliest snapshot ID. This is unexpected.");
        Long baseSnapshotId = null;
        for (long id = currentSnapshotId - 1; id >= earliest; id--) {
            try {
                Snapshot snapshot = snapshotManager.snapshot(id);
                if (!snapshot.commitUser().equals(commitUser)
                        || snapshot.commitIdentifier() < commitIdentifier) {
                    if (table.fileIO().exists(pathFactory.toMetadataPath(id))) {
                        baseSnapshotId = id;
                    }
                    break;
                }
            } catch (Exception ignore) {
                break;
            }
        }

        return Pair.of(currentSnapshotId, baseSnapshotId);
    }

    private void createMetadataWithoutBase(long snapshotId) throws IOException {
        SnapshotReader snapshotReader = table.newSnapshotReader().withSnapshot(snapshotId);
        Function<DataSplit, Stream<IcebergManifestEntry>> dataSplitToManifestEntries =
                s ->
                        s.convertToRawFiles().get().stream()
                                .map(r -> rawFileToManifestEntry(r, s.partition(), snapshotId));
        Iterator<IcebergManifestEntry> entryIterator =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .flatMap(dataSplitToManifestEntries)
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
                        new IcebergSnapshotSummary(IcebergSnapshotSummary.OPERATION_APPEND),
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

    private void createMetadataWithBase(
            ManifestCommittable committable, long currentSnapshotId, long baseSnapshotId)
            throws IOException {
        Set<BinaryRow> modifiedPartitions =
                committable.fileCommittables().stream()
                        .map(CommitMessage::partition)
                        .collect(Collectors.toSet());
        List<BinaryRow> modifiedPartitionsList = new ArrayList<>(modifiedPartitions);

        List<DataSplit> dataSplits =
                table.newSnapshotReader()
                        .withPartitionFilter(modifiedPartitionsList)
                        .read()
                        .dataSplits();
        Map<String, Pair<RawFile, BinaryRow>> currentRawFiles = new HashMap<>();
        for (DataSplit dataSplit : dataSplits) {
            if (dataSplit.rawConvertible() && modifiedPartitions.contains(dataSplit.partition())) {
                dataSplit
                        .convertToRawFiles()
                        .get()
                        .forEach(
                                r ->
                                        currentRawFiles.put(
                                                r.path(), Pair.of(r, dataSplit.partition())));
            }
        }

        IcebergMetadata baseMetadata =
                IcebergMetadata.fromPath(
                        table.fileIO(), pathFactory.toMetadataPath(baseSnapshotId));
        List<IcebergManifestFileMeta> baseManifestFileMetas =
                manifestList.read(baseMetadata.currentSnapshot().manifestList());
        RowType partitionType = table.schema().logicalPartitionType();
        PartitionPredicate predicate =
                PartitionPredicate.fromMultiple(partitionType, modifiedPartitionsList);

        IcebergSnapshotSummary snapshotSummary =
                new IcebergSnapshotSummary(IcebergSnapshotSummary.OPERATION_APPEND);
        List<IcebergManifestFileMeta> newManifestFileMetas = new ArrayList<>();
        for (IcebergManifestFileMeta fileMeta : baseManifestFileMetas) {
            // use partition predicate to only check modified partitions
            int numFields = partitionType.getFieldCount();
            GenericRow minValues = new GenericRow(numFields);
            GenericRow maxValues = new GenericRow(numFields);
            for (int i = 0; i < numFields; i++) {
                IcebergPartitionSummary summary = fileMeta.partitions().get(i);
                DataType fieldType = partitionType.getTypeAt(i);
                minValues.setField(i, IcebergConversions.toObject(fieldType, summary.lowerBound()));
                maxValues.setField(i, IcebergConversions.toObject(fieldType, summary.upperBound()));
            }

            if (predicate.test(
                    fileMeta.liveRowsCount(),
                    minValues,
                    maxValues,
                    // IcebergPartitionSummary only has `containsNull` field and does not have the
                    // exact number of nulls, so we set null count to 0 to not affect filtering
                    new GenericArray(new long[numFields]))) {
                // check if any IcebergManifestEntry in this manifest file meta is removed
                List<IcebergManifestEntry> entries =
                        manifestFile.read(new Path(fileMeta.manifestPath()).getName());
                Set<String> removedPaths = new HashSet<>();
                for (IcebergManifestEntry entry : entries) {
                    if (entry.isLive() && modifiedPartitions.contains(entry.file().partition())) {
                        String path = entry.file().filePath();
                        if (currentRawFiles.containsKey(path)) {
                            currentRawFiles.remove(path);
                        } else {
                            removedPaths.add(path);
                        }
                    }
                }

                if (removedPaths.isEmpty()) {
                    // nothing is removed, use this file meta again
                    newManifestFileMetas.add(fileMeta);
                } else {
                    // some file is removed, rewrite this file meta
                    snapshotSummary =
                            new IcebergSnapshotSummary(IcebergSnapshotSummary.OPERATION_OVERWRITE);
                    List<IcebergManifestEntry> newEntries = new ArrayList<>();
                    for (IcebergManifestEntry entry : entries) {
                        if (entry.isLive()) {
                            newEntries.add(
                                    new IcebergManifestEntry(
                                            removedPaths.contains(entry.file().filePath())
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

        if (!currentRawFiles.isEmpty()) {
            // add new raw files
            newManifestFileMetas.addAll(
                    manifestFile.rollingWrite(
                            currentRawFiles.values().stream()
                                    .map(
                                            p ->
                                                    rawFileToManifestEntry(
                                                            p.getLeft(),
                                                            p.getRight(),
                                                            currentSnapshotId))
                                    .iterator(),
                            currentSnapshotId));
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
                        currentSnapshotId,
                        currentSnapshotId,
                        System.currentTimeMillis(),
                        snapshotSummary,
                        pathFactory.toManifestListPath(manifestListFileName).toString(),
                        schemaId));

        IcebergMetadata metadata =
                new IcebergMetadata(
                        baseMetadata.tableUuid(),
                        baseMetadata.location(),
                        currentSnapshotId,
                        table.schema().highestFieldId(),
                        schemas,
                        schemaId,
                        baseMetadata.partitionSpecs(),
                        baseMetadata.lastPartitionId(),
                        snapshots,
                        (int) currentSnapshotId);
        table.fileIO()
                .tryToWriteAtomic(pathFactory.toMetadataPath(currentSnapshotId), metadata.toJson());
        table.fileIO()
                .overwriteFileUtf8(
                        new Path(pathFactory.metadataDirectory(), VERSION_HINT_FILENAME),
                        String.valueOf(currentSnapshotId));
    }

    private IcebergManifestEntry rawFileToManifestEntry(
            RawFile rawFile, BinaryRow partition, long snapshotId) {
        IcebergDataFileMeta fileMeta =
                new IcebergDataFileMeta(
                        IcebergDataFileMeta.Content.DATA,
                        rawFile.path(),
                        rawFile.format(),
                        partition,
                        rawFile.rowCount(),
                        rawFile.fileSize());
        return new IcebergManifestEntry(
                IcebergManifestEntry.Status.ADDED, snapshotId, snapshotId, snapshotId, fileMeta);
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
