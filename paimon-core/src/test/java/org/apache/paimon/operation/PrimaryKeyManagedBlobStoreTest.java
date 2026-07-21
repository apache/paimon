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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.postpone.PostponeBucketWriter;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for managed BLOB storage in primary-key tables. */
class PrimaryKeyManagedBlobStoreTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testExternalizeRawBlobBeforeMergeTreeBuffer() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createStore(fileIO);
        byte[] expected = "raw-before-buffer".getBytes(StandardCharsets.UTF_8);
        KeyValue keyValue = keyValue(1, RowKind.INSERT, expected);

        store.commitData(
                Collections.singletonList(keyValue), ignored -> BinaryRow.EMPTY_ROW, ignored -> 0);

        ManifestEntry entry = store.newScan().plan().files().get(0);
        List<ManagedBlobReferenceFile.Reference> references = references(fileIO, store, entry);
        assertThat(references).hasSize(1);
        assertThat(references.get(0).relativePath())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);

        KeyValue read =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId()).get(0);
        assertThat(read.value().getBlob(1).toData()).isEqualTo(expected);
    }

    @Test
    void testPostponeBucketExternalizesBlobArray() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store =
                createStore(
                        fileIO,
                        "payloads",
                        DataTypes.ARRAY(DataTypes.BLOB()),
                        BucketMode.POSTPONE_BUCKET);
        byte[] expected = "postpone-bucket-blob-array".getBytes(StandardCharsets.UTF_8);
        KeyValue keyValue =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(
                                        1,
                                        new GenericArray(new Object[] {Blob.fromData(expected)})));

        store.commitData(
                Collections.singletonList(keyValue),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> BucketMode.POSTPONE_BUCKET);

        ManifestEntry entry = store.newScan().plan().files().get(0);
        assertThat(entry.bucket()).isEqualTo(BucketMode.POSTPONE_BUCKET);
        assertThat(references(fileIO, store, entry)).hasSize(1);
        KeyValue read =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId()).get(0);
        assertThat(read.value().getArray(1).getBlob(0).toData()).isEqualTo(expected);
    }

    @Test
    void testPostponeBucketExternalizesBlobMap() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store =
                createStore(
                        fileIO,
                        "pictures",
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()),
                        BucketMode.POSTPONE_BUCKET);
        byte[] expected = "postpone-bucket-blob-map".getBytes(StandardCharsets.UTF_8);
        KeyValue keyValue =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(1, blobMap("picture", Blob.fromData(expected))));

        store.commitData(
                Collections.singletonList(keyValue),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> BucketMode.POSTPONE_BUCKET);

        ManifestEntry entry = store.newScan().plan().files().get(0);
        assertThat(entry.bucket()).isEqualTo(BucketMode.POSTPONE_BUCKET);
        assertThat(references(fileIO, store, entry)).hasSize(1);
        InternalMap result =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId())
                        .get(0)
                        .value()
                        .getMap(1);
        assertThat(result.keyArray().getString(0).toString()).isEqualTo("picture");
        assertThat(result.valueArray().getBlob(0).toData()).isEqualTo(expected);
    }

    @Test
    void testPostponeBucketBufferedRewriteKeepsManagedBlobReferences() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store =
                createStore(fileIO, "payload", DataTypes.BLOB(), BucketMode.POSTPONE_BUCKET);
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        try (IOManager ioManager = IOManager.create(tempDir.resolve("io").toString())) {
            try {
                write.withIOManager(ioManager);
                write.write(
                        BinaryRow.EMPTY_ROW,
                        BucketMode.POSTPONE_BUCKET,
                        keyValue(
                                1,
                                RowKind.INSERT,
                                "before-buffered-rewrite".getBytes(StandardCharsets.UTF_8)));
                PostponeBucketWriter writer =
                        (PostponeBucketWriter)
                                write.getWriterWrapper(
                                                BinaryRow.EMPTY_ROW, BucketMode.POSTPONE_BUCKET)
                                        .writer;
                writer.toBufferedWriter();

                assertThat(writer.useBufferedSinkWriter()).isTrue();
                Path bucketPath =
                        store.pathFactory()
                                .bucketPath(BinaryRow.EMPTY_ROW, BucketMode.POSTPONE_BUCKET);
                assertThat(fileIO.listStatus(bucketPath))
                        .extracting(status -> status.getPath().getName())
                        .allMatch(
                                name ->
                                        name.endsWith(
                                                ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX));

                write.write(
                        BinaryRow.EMPTY_ROW,
                        BucketMode.POSTPONE_BUCKET,
                        keyValue(
                                2,
                                RowKind.INSERT,
                                "after-buffered-rewrite".getBytes(StandardCharsets.UTF_8)));
                List<CommitMessage> messages = write.prepareCommit(false, 1L);
                try (FileStoreCommit commit = store.newCommit()) {
                    commit.commit(new ManifestCommittable(1L, null, messages), false);
                }
            } finally {
                write.close();
            }
        }

        ManifestEntry entry = store.newScan().plan().files().get(0);
        assertThat(references(fileIO, store, entry)).hasSize(2);
        assertThat(store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId()))
                .extracting(
                        kv -> new String(kv.value().getBlob(1).toData(), StandardCharsets.UTF_8))
                .containsExactlyInAnyOrder("before-buffered-rewrite", "after-buffered-rewrite");
    }

    @Test
    void testExternalizeAndReadBlobArray() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createArrayStore(fileIO);
        byte[] expected = "array-payload".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-array-payload".getBytes(StandardCharsets.UTF_8);
        Path source = new Path(tempDir.resolve("external-array.blob").toUri());
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(source, false)) {
            out.write(second);
        }
        Blob external = Blob.fromFile(fileIO, source.toString());
        KeyValue keyValue =
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(
                                        1,
                                        new GenericArray(
                                                new Object[] {
                                                    Blob.fromData(expected), null, external
                                                })));

        store.commitData(
                Collections.singletonList(keyValue), ignored -> BinaryRow.EMPTY_ROW, ignored -> 0);

        ManifestEntry entry = store.newScan().plan().files().get(0);
        assertThat(references(fileIO, store, entry)).hasSize(2);
        InternalArray blobs =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId())
                        .get(0)
                        .value()
                        .getArray(1);
        assertThat(blobs.size()).isEqualTo(3);
        assertThat(blobs.getBlob(0).toData()).isEqualTo(expected);
        assertThat(blobs.isNullAt(1)).isTrue();
        assertThat(blobs.getBlob(2).toDescriptor().uri()).isNotEqualTo(source.toString());
        assertThat(blobs.getBlob(2).toData()).isEqualTo(second);
    }

    @Test
    void testExternalizeAndReadBlobMap() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createMapStore(fileIO);
        byte[] expected = "map-payload".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-map-payload".getBytes(StandardCharsets.UTF_8);
        Path source = new Path(tempDir.resolve("external-map.blob").toUri());
        try (org.apache.paimon.fs.PositionOutputStream out =
                fileIO.newOutputStream(source, false)) {
            out.write(second);
        }
        Map<Object, Object> input = new LinkedHashMap<>();
        input.put(BinaryString.fromString("first"), Blob.fromData(expected));
        input.put(BinaryString.fromString("null"), null);
        input.put(BinaryString.fromString("second"), Blob.fromFile(fileIO, source.toString()));

        store.commitData(
                Collections.singletonList(
                        new KeyValue()
                                .replace(
                                        GenericRow.of(1),
                                        RowKind.INSERT,
                                        GenericRow.of(1, new GenericMap(input)))),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);

        ManifestEntry entry = store.newScan().plan().files().get(0);
        assertThat(references(fileIO, store, entry)).hasSize(2);
        InternalMap blobs =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId())
                        .get(0)
                        .value()
                        .getMap(1);
        Map<String, Blob> actual = new HashMap<>();
        InternalArray keys = blobs.keyArray();
        InternalArray values = blobs.valueArray();
        for (int i = 0; i < blobs.size(); i++) {
            actual.put(keys.getString(i).toString(), values.isNullAt(i) ? null : values.getBlob(i));
        }
        assertThat(actual).containsOnlyKeys("first", "null", "second");
        assertThat(actual.get("first").toData()).isEqualTo(expected);
        assertThat(actual.get("null")).isNull();
        assertThat(actual.get("second").toDescriptor().uri()).isNotEqualTo(source.toString());
        assertThat(actual.get("second").toData()).isEqualTo(second);
    }

    @Test
    void testCompactionRebuildsExactBlobReferences() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createStore(fileIO);

        store.commitData(
                Collections.singletonList(
                        keyValue(1, RowKind.INSERT, "old".getBytes(StandardCharsets.UTF_8))),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        ManagedBlobReferenceFile.Reference oldReference =
                references(fileIO, store, store.newScan().plan().files().get(0)).get(0);

        store.commitData(
                Arrays.asList(
                        keyValue(1, RowKind.UPDATE_AFTER, "new".getBytes(StandardCharsets.UTF_8)),
                        keyValue(2, RowKind.INSERT, "second".getBytes(StandardCharsets.UTF_8))),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        List<ManagedBlobReferenceFile.Reference> survivingReferences = new java.util.ArrayList<>();
        for (ManifestEntry file : store.newScan().plan().files()) {
            survivingReferences.addAll(references(fileIO, store, file));
        }
        survivingReferences.remove(oldReference);
        assertThat(survivingReferences).hasSize(2);

        forceFullCompaction(store);

        List<ManifestEntry> files = store.newScan().plan().files();
        assertThat(files).hasSize(1);
        List<ManagedBlobReferenceFile.Reference> compactedReferences =
                references(fileIO, store, files.get(0));
        assertThat(compactedReferences)
                .containsExactlyInAnyOrderElementsOf(survivingReferences)
                .doesNotContain(oldReference);
        assertThat(store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId()))
                .extracting(
                        kv -> new String(kv.value().getBlob(1).toData(), StandardCharsets.UTF_8))
                .containsExactlyInAnyOrder("new", "second");
    }

    @Test
    void testBlobArrayCompactionRebuildsExactReferences() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createArrayStore(fileIO);

        store.commitData(
                Arrays.asList(
                        arrayKeyValue(1, RowKind.INSERT, "old"),
                        arrayKeyValue(2, RowKind.INSERT, "deleted")),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        List<ManagedBlobReferenceFile.Reference> oldReferences = new java.util.ArrayList<>();
        for (ManifestEntry file : store.newScan().plan().files()) {
            oldReferences.addAll(references(fileIO, store, file));
        }
        assertThat(oldReferences).hasSize(2);

        store.commitData(
                Arrays.asList(
                        arrayKeyValue(1, RowKind.UPDATE_AFTER, "new"),
                        arrayKeyValue(2, RowKind.DELETE, "must-not-be-written")),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        forceFullCompaction(store);

        List<ManifestEntry> files = store.newScan().plan().files();
        assertThat(files).hasSize(1);
        assertThat(references(fileIO, store, files.get(0)))
                .hasSize(1)
                .doesNotContainAnyElementsOf(oldReferences);
        List<KeyValue> rows = store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).value().getArray(1).getBlob(0).toData())
                .isEqualTo("new".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testBlobMapCompactionRebuildsExactReferences() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        TestFileStore store = createMapStore(fileIO);

        store.commitData(
                Arrays.asList(
                        mapKeyValue(1, RowKind.INSERT, "old"),
                        mapKeyValue(2, RowKind.INSERT, "deleted")),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        List<ManagedBlobReferenceFile.Reference> oldReferences = new java.util.ArrayList<>();
        for (ManifestEntry file : store.newScan().plan().files()) {
            oldReferences.addAll(references(fileIO, store, file));
        }
        assertThat(oldReferences).hasSize(2);

        store.commitData(
                Arrays.asList(
                        mapKeyValue(1, RowKind.UPDATE_AFTER, "new"),
                        mapKeyValue(2, RowKind.DELETE, "must-not-be-written")),
                ignored -> BinaryRow.EMPTY_ROW,
                ignored -> 0);
        forceFullCompaction(store);

        List<ManifestEntry> files = store.newScan().plan().files();
        assertThat(files).hasSize(1);
        assertThat(references(fileIO, store, files.get(0)))
                .hasSize(1)
                .doesNotContainAnyElementsOf(oldReferences);
        List<KeyValue> rows = store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId());
        assertThat(rows).hasSize(1);
        InternalMap result = rows.get(0).value().getMap(1);
        assertThat(result.keyArray().getString(0).toString()).isEqualTo("key");
        assertThat(result.valueArray().getBlob(0).toData())
                .isEqualTo("new".getBytes(StandardCharsets.UTF_8));
    }

    private TestFileStore createStore(FileIO fileIO) throws Exception {
        return createStore(fileIO, "payload", DataTypes.BLOB());
    }

    private TestFileStore createArrayStore(FileIO fileIO) throws Exception {
        return createStore(fileIO, "payloads", DataTypes.ARRAY(DataTypes.BLOB()));
    }

    private TestFileStore createMapStore(FileIO fileIO) throws Exception {
        return createStore(fileIO, "payloads", DataTypes.MAP(DataTypes.STRING(), DataTypes.BLOB()));
    }

    private TestFileStore createStore(FileIO fileIO, String payloadName, DataType payloadType)
            throws Exception {
        return createStore(fileIO, payloadName, payloadType, 1);
    }

    private TestFileStore createStore(
            FileIO fileIO, String payloadName, DataType payloadType, int bucket) throws Exception {
        Path tablePath = new Path(tempDir.toUri());
        List<DataField> valueFields =
                Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, payloadName, payloadType));
        RowType valueType = new RowType(valueFields);
        RowType keyType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        SpecialFields.KEY_FIELD_ID_START,
                                        SpecialFields.KEY_FIELD_PREFIX + "id",
                                        DataTypes.INT())));
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), String.valueOf(bucket));
        options.put(CoreOptions.BLOB_FIELD.key(), payloadName);
        options.put(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "1 b");
        TableSchema schema =
                new SchemaManager(fileIO, tablePath)
                        .createTable(
                                new Schema(
                                        valueFields,
                                        Collections.emptyList(),
                                        Collections.singletonList("id"),
                                        options,
                                        ""));
        KeyValueFieldsExtractor extractor =
                new KeyValueFieldsExtractor() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public List<DataField> keyFields(TableSchema ignored) {
                        return keyType.getFields();
                    }

                    @Override
                    public List<DataField> valueFields(TableSchema tableSchema) {
                        return tableSchema.fields();
                    }
                };
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        bucket,
                        RowType.of(),
                        keyType,
                        valueType,
                        extractor,
                        DeduplicateMergeFunction.factory(),
                        schema)
                .build();
    }

    private List<ManagedBlobReferenceFile.Reference> references(
            FileIO fileIO, TestFileStore store, ManifestEntry entry) throws Exception {
        DataFileMeta dataFile = entry.file();
        String referenceFile =
                dataFile.extraFiles().stream()
                        .filter(
                                file ->
                                        file.endsWith(
                                                ManagedBlobReferenceFile.REFERENCE_FILE_SUFFIX))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing managed BLOB sidecar."));
        DataFilePathFactory pathFactory =
                store.pathFactory().createDataFilePathFactory(entry.partition(), entry.bucket());
        Path sidecar = pathFactory.toAlignedPath(referenceFile, dataFile);
        return ManagedBlobReferenceFile.read(fileIO, sidecar);
    }

    private void forceFullCompaction(TestFileStore store) throws Exception {
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        try {
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            List<CommitMessage> messages = write.prepareCommit(true, 1000L);
            try (FileStoreCommit commit = store.newCommit()) {
                commit.commit(new ManifestCommittable(1000L, null, messages), false);
            }
        } finally {
            write.close();
        }
    }

    private KeyValue keyValue(int id, RowKind kind, byte[] bytes) {
        return new KeyValue()
                .replace(GenericRow.of(id), kind, GenericRow.of(id, Blob.fromData(bytes)));
    }

    private KeyValue arrayKeyValue(int id, RowKind kind, String value) {
        return new KeyValue()
                .replace(
                        GenericRow.of(id),
                        kind,
                        GenericRow.of(
                                id,
                                new GenericArray(
                                        new Object[] {
                                            Blob.fromData(value.getBytes(StandardCharsets.UTF_8))
                                        })));
    }

    private KeyValue mapKeyValue(int id, RowKind kind, String value) {
        return new KeyValue()
                .replace(
                        GenericRow.of(id),
                        kind,
                        GenericRow.of(
                                id,
                                blobMap(
                                        "key",
                                        Blob.fromData(value.getBytes(StandardCharsets.UTF_8)))));
    }

    private GenericMap blobMap(String key, Blob value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString(key), value);
        return new GenericMap(map);
    }
}
