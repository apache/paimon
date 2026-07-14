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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.RecordWriter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end maintenance tests for source-backed BTree and Bitmap indexes. */
class PrimaryKeySortedIndexMaintenanceTest {

    @TempDir java.nio.file.Path tempDir;
    private IOManager ioManager;

    @BeforeEach
    void before() throws IOException {
        ioManager = new IOManagerImpl(tempDir.toString());
    }

    @AfterEach
    void after() throws Exception {
        ioManager.close();
    }

    @Test
    void testBuildRestoreAndReplaceSourceGroups() throws Exception {
        TestFileStore store = createStore();
        TestKeyValueGenerator generator = new TestKeyValueGenerator();
        List<KeyValue> firstBatch =
                Arrays.asList(
                        withKey(generator.nextInsert("20201110", 10, 30L, new int[] {3}, "c"), 0),
                        withKey(generator.nextInsert("20201110", 10, 10L, new int[] {1}, "a"), 4),
                        withKey(generator.nextInsert("20201110", 10, null, new int[] {0}, null), 8),
                        withKey(generator.nextInsert("20201110", 10, 20L, new int[] {2}, "b"), 2),
                        withKey(generator.nextInsert("20201110", 10, 40L, new int[] {4}, "d"), 6));
        BinaryRow partition = generator.getPartition(firstBatch.get(0));

        writeAndCommit(store, partition, 0, firstBatch.subList(0, 3), 0);
        writeAndCommit(store, partition, 0, firstBatch.subList(3, 5), 1);
        assertThat(store.newScan().plan().files()).hasSize(2);
        compactAndCommit(store, partition, 0, 2);

        List<DataFileMeta> compactedFiles =
                store.newScan().plan().files().stream()
                        .map(entry -> entry.file())
                        .collect(Collectors.toList());
        assertThat(compactedFiles).anyMatch(PrimaryKeyIndexSourcePolicy::shouldRead);
        List<IndexFileMeta> firstPayloads = sourcePayloads(store, partition, 0);
        assertCompleteGroups(firstPayloads, 5);
        Set<String> firstPayloadNames =
                firstPayloads.stream().map(IndexFileMeta::fileName).collect(Collectors.toSet());
        Set<String> firstSources = sourceNames(firstPayloads);
        assertThat(firstSources).isNotEmpty();

        KeyValue additional =
                withKey(generator.nextInsert("20201110", 10, 50L, new int[] {5}, "e"), 5);
        writeAndCommit(store, partition, 0, Arrays.asList(additional), 3);
        compactAndCommit(store, partition, 0, 4);

        List<IndexFileMeta> replacementPayloads = sourcePayloads(store, partition, 0);
        assertCompleteGroups(replacementPayloads, 6);
        assertThat(replacementPayloads)
                .extracting(IndexFileMeta::fileName)
                .doesNotContainAnyElementsOf(firstPayloadNames);
        assertThat(sourceNames(replacementPayloads)).doesNotContainAnyElementsOf(firstSources);

        AbstractFileStoreWrite<KeyValue> restored = store.newWrite();
        restored.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                restored.createWriterContainer(partition, 0);
        restored.prepareCommit(false, 5);
        assertThat(container.primaryKeyIndexMaintainer).isNotNull();
        assertThat(container.primaryKeyIndexMaintainer.buildNotCompleted()).isFalse();
        restored.close();
    }

    @Test
    void testRestoreMixedVectorAndScalarPayloads() throws Exception {
        TestFileStore store = createMixedStore();
        TestKeyValueGenerator generator = new TestKeyValueGenerator();
        KeyValue first =
                withVector(
                        withKey(generator.nextInsert("20201110", 10, 30L, new int[] {3}, "c"), 0),
                        new float[] {1, 0});
        KeyValue second =
                withVector(
                        withKey(generator.nextInsert("20201110", 10, 10L, new int[] {1}, "a"), 1),
                        new float[] {0, 1});
        BinaryRow partition = generator.getPartition(first);

        writeAndCommit(store, partition, 0, Collections.singletonList(first), 0);
        writeAndCommit(store, partition, 0, Collections.singletonList(second), 1);
        compactAndCommit(store, partition, 0, 2);

        assertThat(sourcePayloads(store, partition, 0))
                .extracting(IndexFileMeta::indexType)
                .contains("test-vector-ann", "btree", "bitmap");

        AbstractFileStoreWrite<KeyValue> restored = store.newWrite();
        restored.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                restored.createWriterContainer(partition, 0);
        restored.prepareCommit(false, 3);

        assertThat(container.primaryKeyIndexMaintainer).isNotNull();
        assertThat(container.primaryKeyIndexMaintainer.buildNotCompleted()).isFalse();
        restored.close();
    }

    private void writeAndCommit(
            TestFileStore store,
            BinaryRow partition,
            int bucket,
            List<KeyValue> records,
            long identifier)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        write.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                write.createWriterContainer(partition, bucket);
        RecordWriter<KeyValue> writer = container.writer;
        ((MemoryOwner) writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(
                                TestFileStore.WRITE_BUFFER_SIZE.getBytes(),
                                (int) TestFileStore.PAGE_SIZE.getBytes()));
        for (KeyValue record : records) {
            writer.write(record);
        }
        commit(
                store,
                Collections.singletonList(prepareCommit(container, partition, bucket)),
                identifier);
        container.writer.close();
        container.primaryKeyIndexMaintainer.close();
        write.close();
    }

    private void compactAndCommit(
            TestFileStore store, BinaryRow partition, int bucket, long identifier)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        write.withIOManager(ioManager);
        AbstractFileStoreWrite.WriterContainer<KeyValue> container =
                write.createWriterContainer(partition, bucket);
        ((MemoryOwner) container.writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(
                                TestFileStore.WRITE_BUFFER_SIZE.getBytes(),
                                (int) TestFileStore.PAGE_SIZE.getBytes()));
        container.writer.compact(true);
        commit(
                store,
                Collections.singletonList(prepareCommit(container, partition, bucket)),
                identifier);
        container.writer.close();
        container.primaryKeyIndexMaintainer.close();
        write.close();
    }

    private CommitMessage prepareCommit(
            AbstractFileStoreWrite.WriterContainer<KeyValue> container,
            BinaryRow partition,
            int bucket)
            throws Exception {
        CommitIncrement increment = container.writer.prepareCommit(true);
        container.primaryKeyIndexMaintainer.prepareCommit(
                increment.newFilesIncrement(), increment.compactIncrement(), true);
        return new CommitMessageImpl(
                partition, bucket, 1, increment.newFilesIncrement(), increment.compactIncrement());
    }

    private void commit(TestFileStore store, List<CommitMessage> messages, long identifier) {
        ManifestCommittable committable = new ManifestCommittable(identifier, null, messages);
        try (FileStoreCommit commit = store.newCommit()) {
            commit.withIOManager(ioManager).commit(committable, false);
        }
    }

    private static List<IndexFileMeta> sourcePayloads(
            TestFileStore store, BinaryRow partition, int bucket) {
        Snapshot snapshot = store.snapshotManager().latestSnapshot();
        return store.newIndexFileHandler().scanSourceIndexes(snapshot, partition, bucket);
    }

    private static void assertCompleteGroups(List<IndexFileMeta> payloads, long rowCount) {
        Map<String, List<IndexFileMeta>> byType =
                payloads.stream().collect(Collectors.groupingBy(IndexFileMeta::indexType));
        assertThat(byType.keySet()).containsExactlyInAnyOrder("btree", "bitmap");
        for (List<IndexFileMeta> typePayloads : byType.values()) {
            assertThat(typePayloads)
                    .allMatch(payload -> payload.globalIndexMeta().sourceMeta() != null);
            assertThat(typePayloads.stream().mapToLong(IndexFileMeta::rowCount).sum())
                    .as("total row count of %s source groups", typePayloads.get(0).indexType())
                    .isEqualTo(rowCount);
            Map<List<PrimaryKeyIndexSourceFile>, List<IndexFileMeta>> bySources =
                    typePayloads.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            payload ->
                                                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                                            .sourceFiles()));
            for (Map.Entry<List<PrimaryKeyIndexSourceFile>, List<IndexFileMeta>> sourceGroup :
                    bySources.entrySet()) {
                assertThat(sourceGroup.getValue().stream().mapToLong(IndexFileMeta::rowCount).sum())
                        .isEqualTo(
                                sourceGroup.getKey().stream()
                                        .mapToLong(PrimaryKeyIndexSourceFile::rowCount)
                                        .sum());
            }
        }
    }

    private static Set<String> sourceNames(List<IndexFileMeta> payloads) {
        Set<String> result = new HashSet<>();
        for (IndexFileMeta payload : payloads) {
            for (PrimaryKeyIndexSourceFile source :
                    PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles()) {
                result.add(source.fileName());
            }
        }
        return result;
    }

    private static KeyValue withKey(KeyValue record, int key) {
        return new KeyValue()
                .replace(
                        TestKeyValueGenerator.KEY_SERIALIZER
                                .toBinaryRow(GenericRow.of(key, (long) key))
                                .copy(),
                        record.sequenceNumber(),
                        record.valueKind(),
                        record.value());
    }

    private static KeyValue withVector(KeyValue record, float[] vector) {
        InternalRow original = record.value();
        GenericRow extended =
                new GenericRow(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFieldCount() + 1);
        for (int i = 0; i < TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFieldCount(); i++) {
            extended.setField(
                    i,
                    InternalRowUtils.get(
                            original, i, TestKeyValueGenerator.DEFAULT_ROW_TYPE.getTypeAt(i)));
        }
        extended.setField(
                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFieldCount(),
                BinaryVector.fromPrimitiveArray(vector));
        return record.replaceValue(extended);
    }

    private TestFileStore createStore() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES.key(), "true");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "itemId");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "comment");
        options.put(
                "fields.itemId.pk-btree.index.options",
                "{\"sorted-index.records-per-range\":\"2\"}");
        options.put(
                "fields.comment.pk-bitmap.index.options",
                "{\"sorted-index.records-per-range\":\"2\"}");
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                options,
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                new CoreOptions(options),
                                TestKeyValueGenerator.KEY_TYPE,
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE),
                        schema)
                .build();
    }

    private TestFileStore createMixedStore() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.COMPACTION_FORCE_REWRITE_ALL_FILES.key(), "true");
        options.put(CoreOptions.PK_BTREE_INDEX_COLUMNS.key(), "itemId");
        options.put(CoreOptions.PK_BITMAP_INDEX_COLUMNS.key(), "comment");
        options.put(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding");
        options.put("fields.embedding.pk-vector.index.type", "test-vector-ann");
        options.put("fields.embedding.pk-vector.distance.metric", "l2");

        List<DataField> fields =
                new ArrayList<>(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields());
        fields.add(new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT())));
        RowType rowType = new RowType(fields);
        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                options,
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        rowType,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        LookupMergeFunction.wrap(
                                DeduplicateMergeFunction.factory(),
                                new CoreOptions(options),
                                TestKeyValueGenerator.KEY_TYPE,
                                rowType),
                        schema)
                .build();
    }
}
