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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.manifest.IcebergDataFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestEntry;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile;
import org.apache.paimon.iceberg.manifest.IcebergManifestFileMeta;
import org.apache.paimon.iceberg.manifest.IcebergManifestList;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.iceberg.metadata.IcebergPartitionField;
import org.apache.paimon.iceberg.metadata.IcebergPartitionSpec;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.iceberg.metadata.IcebergSnapshot;
import org.apache.paimon.iceberg.metadata.IcebergSnapshotSummary;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

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

        createMetadataWithoutBase(currentSnapshot);
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
                    baseSnapshotId = id;
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
        Iterator<IcebergManifestEntry> entryIterator =
                snapshotReader.read().dataSplits().stream()
                        .filter(DataSplit::rawConvertible)
                        .flatMap(s -> dataSplitToDataFileMeta(s).stream())
                        .map(
                                m ->
                                        new IcebergManifestEntry(
                                                IcebergManifestEntry.Status.ADDED,
                                                snapshotId,
                                                snapshotId,
                                                snapshotId,
                                                m))
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

    private List<IcebergDataFileMeta> dataSplitToDataFileMeta(DataSplit dataSplit) {
        List<IcebergDataFileMeta> result = new ArrayList<>();
        for (RawFile rawFile : dataSplit.convertToRawFiles().get()) {
            result.add(
                    new IcebergDataFileMeta(
                            IcebergDataFileMeta.Content.DATA,
                            rawFile.path(),
                            rawFile.format(),
                            dataSplit.partition(),
                            rawFile.rowCount(),
                            rawFile.fileSize()));
        }
        return result;
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
