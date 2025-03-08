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
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.sink.BatchWriteBuilder.COMMIT_IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreWrite}. */
public class KeyValueFileStoreWriteTest {

    private static final int NUM_BUCKETS = 10;

    @TempDir java.nio.file.Path tempDir;

    private IOManager ioManager;

    private TableSchema tableSchema;

    @BeforeEach
    public void before() throws Exception {
        this.ioManager = new IOManagerImpl(tempDir.toString());

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));

        tableSchema =
                schemaManager.createTable(
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                new HashMap<>(),
                                null));
    }

    @Test
    public void testRadicalLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");

        KeyValueFileStoreWrite write = createWriteWithOptions(options);
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue keyValue = gen.next();
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.createWriterContainer(gen.getPartition(keyValue), 1, false);
        MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;
        try (MergeTreeCompactManager compactManager =
                (MergeTreeCompactManager) writer.compactManager()) {
            CompactStrategy compactStrategy = compactManager.getStrategy();
            assertThat(compactStrategy).isInstanceOf(ForceUpLevel0Compaction.class);
        }
    }

    @Test
    public void testGentleLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "gentle");

        KeyValueFileStoreWrite write = createWriteWithOptions(options);
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue keyValue = gen.next();
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.createWriterContainer(gen.getPartition(keyValue), 1, false);
        MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;
        try (MergeTreeCompactManager compactManager =
                (MergeTreeCompactManager) writer.compactManager()) {
            CompactStrategy compactStrategy = compactManager.getStrategy();
            assertThat(compactStrategy).isInstanceOf(UniversalCompaction.class);
        }
    }

    @Test
    public void testMultiWriteModeEnabled() throws Exception {
        Map<String, String> options1 = new HashMap<>();
        options1.put(CoreOptions.WRITE_MODE.key(), "multi_write");
        options1.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "2");
        String user1 = "user1";
        String user2 = "user2";
        String user3 = "user3";

        Map<String, String> options2 = new HashMap<>();
        options2.put(CoreOptions.WRITE_ONLY.key(), "true");

        Map<String, String> options3 = new HashMap<>(options2);

        TestFileStore store1 = createStoreWithOptions(options1);
        TestFileStore store2 = createStoreWithOptions(options2);
        TestFileStore store3 = createStoreWithOptions(options3);

        KeyValueFileStoreWrite write1 = (KeyValueFileStoreWrite) store1.newWrite(user1);
        KeyValueFileStoreWrite write2 = (KeyValueFileStoreWrite) store2.newWrite(user2);
        KeyValueFileStoreWrite write3 = (KeyValueFileStoreWrite) store3.newWrite(user3);

        write1.withIOManager(ioManager);
        write2.withIOManager(ioManager);
        write3.withIOManager(ioManager);

        TestKeyValueGenerator gen = new TestKeyValueGenerator();
        KeyValue kv1 = gen.nextInsert("1111", 11, 1L, null, "1");
        KeyValue kv2 = gen.nextInsert("1111", 11, 11L, null, "1");
        KeyValue kv3 = gen.nextInsert("1111", 11, 111L, null, "1");

        BinaryRow partition = gen.getPartition(kv1);

        // 1, init writer1, write kv1 and compact at first
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer1 =
                write1.createWriterContainer(partition, 1, false);
        MergeTreeWriter writer1 = (MergeTreeWriter) writerContainer1.writer;

        writer1.write(kv1);
        writer1.compact(false);
        assertThat(writer1.dataFiles()).hasSize(1);

        // 2, commit kv2 in writer2 (write-only) and kv3 in writer3 (write-only)
        store2.commitData(Collections.singletonList(kv2), part -> partition, kv -> 1, user2);
        store3.commitData(Collections.singletonList(kv3), part -> partition, kv -> 1, user3);

        // 3, prepare commit for writer 1, the commit message should contain the kv2
        // generated by write2 and kv3 generated by write3
        CommitIncrement increment = writer1.prepareCommit(true);
        assertThat(increment.compactIncrement().compactBefore()).hasSize(3);
        assertThat(
                        increment.compactIncrement().compactBefore().stream()
                                .mapToLong(DataFileMeta::rowCount)
                                .sum())
                .isEqualTo(3L);
        assertThat(increment.compactIncrement().compactAfter()).hasSize(1);
        assertThat(
                        increment.compactIncrement().compactAfter().stream()
                                .mapToLong(DataFileMeta::rowCount)
                                .sum())
                .isEqualTo(3L);

        // 4, commit writer 1 successfully
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition, 1, increment.newFilesIncrement(), increment.compactIncrement()));
        store1.newCommit(user1).commit(committable, new HashMap<>());

        // 5, compare the kv values and file count from latest snapshot, file count should be 1
        List<KeyValue> expected = Arrays.asList(kv1, kv2, kv3);
        List<KeyValue> actual =
                store1.readKvsFromSnapshot(store1.snapshotManager().latestSnapshotId());
        gen.sort(expected);
        assertThat(store1.toKvMap(actual)).isEqualTo(store1.toKvMap(expected));
        assertThat(
                        store1.newScan()
                                .withPartitionBucket(partition, 1)
                                .withSnapshot(store1.snapshotManager().latestSnapshotId())
                                .plan()
                                .files())
                .hasSize(1);
    }

    @Test
    public void testMultiWriteModeDisabled() throws Exception {
        Map<String, String> options1 = new HashMap<>();
        options1.put(CoreOptions.WRITE_MODE.key(), "single_write");
        options1.put(CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "2");
        String user1 = "user1";
        String user2 = "user2";

        Map<String, String> options2 = new HashMap<>();
        options2.put(CoreOptions.WRITE_ONLY.key(), "true");

        TestFileStore store1 = createStoreWithOptions(options1);
        TestFileStore store2 = createStoreWithOptions(options2);

        KeyValueFileStoreWrite write1 = (KeyValueFileStoreWrite) store1.newWrite(user1);
        KeyValueFileStoreWrite write2 = (KeyValueFileStoreWrite) store2.newWrite(user2);

        write1.withIOManager(ioManager);
        write2.withIOManager(ioManager);

        TestKeyValueGenerator gen = new TestKeyValueGenerator();
        KeyValue kv1 = gen.nextInsert("1111", 11, 1L, null, "1");
        KeyValue kv2 = gen.nextInsert("1111", 11, 11L, null, "1");

        BinaryRow partition = gen.getPartition(kv1);

        // 1, init writer1, write kv1 and compact at first
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer1 =
                write1.createWriterContainer(partition, 1, false);
        MergeTreeWriter writer1 = (MergeTreeWriter) writerContainer1.writer;

        writer1.write(kv1);
        writer1.compact(false);
        assertThat(writer1.dataFiles()).hasSize(1);

        // 2, commit kv2 in writer2 (write-only)
        store2.commitData(Collections.singletonList(kv2), part -> partition, kv -> 1, user2);

        // 3, prepare commit for writer 1, the compact increment should be empty
        CommitIncrement increment = writer1.prepareCommit(true);
        assertThat(increment.compactIncrement().isEmpty()).isTrue();

        // 4, commit writer 1 successfully
        ManifestCommittable committable = new ManifestCommittable(COMMIT_IDENTIFIER);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition, 1, increment.newFilesIncrement(), increment.compactIncrement()));
        store1.newCommit(user1).commit(committable, new HashMap<>());

        // 5, compare the kv values and file count from latest snapshot, file count should be 2
        List<KeyValue> expected = Arrays.asList(kv1, kv2);
        List<KeyValue> actual =
                store1.readKvsFromSnapshot(store1.snapshotManager().latestSnapshotId());
        gen.sort(expected);
        assertThat(store1.toKvMap(actual)).isEqualTo(store1.toKvMap(expected));
        assertThat(
                        store1.newScan()
                                .withPartitionBucket(partition, 1)
                                .withSnapshot(store1.snapshotManager().latestSnapshotId())
                                .plan()
                                .files())
                .hasSize(2);
    }

    private KeyValueFileStoreWrite createWriteWithOptions(Map<String, String> options) {

        TestFileStore store = createStoreWithOptions(options);

        return (KeyValueFileStoreWrite) store.newWrite();
    }

    private TestFileStore createStoreWithOptions(Map<String, String> options) {
        TableSchema schema = tableSchema.copy(options);

        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        NUM_BUCKETS,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        schema)
                .build();
    }
}
