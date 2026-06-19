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
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mergetree.MergeTreeWriter;
import org.apache.paimon.mergetree.compact.CompactStrategy;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.mergetree.compact.ForceUpLevel0Compaction;
import org.apache.paimon.mergetree.compact.MergeTreeCompactManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyValueFileStoreWrite}. */
public class KeyValueFileStoreWriteTest {

    private static final int NUM_BUCKETS = 10;

    @TempDir java.nio.file.Path tempDir;

    private IOManager ioManager;

    @BeforeEach
    public void before() throws IOException {
        this.ioManager = new IOManagerImpl(tempDir.toString());
    }

    @Test
    public void testRadicalLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "radical");
        CompactStrategy compactStrategy = createCompactStrategy(options);
        assertThat(compactStrategy).isInstanceOf(ForceUpLevel0Compaction.class);
        Integer maxCompactInterval =
                ((ForceUpLevel0Compaction) compactStrategy).maxCompactInterval();
        assertThat(maxCompactInterval).isNull();
    }

    @Test
    public void testGentleLookupCompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        options.put(CoreOptions.LOOKUP_COMPACT.key(), "gentle");
        CompactStrategy compactStrategy = createCompactStrategy(options);
        assertThat(compactStrategy).isInstanceOf(ForceUpLevel0Compaction.class);
        Integer maxCompactInterval =
                ((ForceUpLevel0Compaction) compactStrategy).maxCompactInterval();
        assertThat(maxCompactInterval).isEqualTo(10);
    }

    @Test
    public void testForceUpLevel0CompactStrategy() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.COMPACTION_FORCE_UP_LEVEL_0.key(), "true");
        CompactStrategy compactStrategy = createCompactStrategy(options);
        assertThat(compactStrategy).isInstanceOf(ForceUpLevel0Compaction.class);
    }

    @Test
    public void testSnapshotSequenceNumberInitRestoresFromSnapshotProperties() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "snapshot");
        TestFileStore store = createStoreWithOptions(options);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue first = gen.nextInsert("20201110", 10, 1L, new int[] {1, 1}, "first");
        store.commitData(Collections.singletonList(first), gen::getPartition, kv -> 1);
        Snapshot firstSnapshot = store.snapshotManager().latestSnapshot();
        assertThat(firstSnapshot.properties())
                .containsEntry(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER, "0");

        KeyValue second = gen.nextInsert("20201110", 10, 2L, new int[] {2, 2}, "second");
        store.commitData(Collections.singletonList(second), gen::getPartition, kv -> 1);
        Snapshot secondSnapshot = store.snapshotManager().latestSnapshot();
        assertThat(secondSnapshot.properties())
                .containsEntry(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER, "1");
    }

    @Test
    public void testSnapshotSequenceNumberInitBootstrapsGlobalMaxSequenceNumber() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        TestFileStore store = createStoreWithOptions(options);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        store.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 1L, new int[] {1, 1}, "first")),
                gen::getPartition,
                kv -> 2);
        store.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 2L, new int[] {2, 2}, "second")),
                gen::getPartition,
                kv -> 2);
        store.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 3L, new int[] {3, 3}, "third")),
                gen::getPartition,
                kv -> 2);
        assertThat(
                        SequenceSnapshotProperties.maxSequenceNumber(
                                store.snapshotManager().latestSnapshot()))
                .isEmpty();

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        TableSchema snapshotSchema =
                schemaManager.commitChanges(
                        SchemaChange.setOption(
                                CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "snapshot"));
        TestFileStore snapshotStore = createStore(snapshotSchema);

        snapshotStore.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 4L, new int[] {4, 4}, "fourth")),
                gen::getPartition,
                kv -> 1);
        Snapshot bootstrappedSnapshot = snapshotStore.snapshotManager().latestSnapshot();
        assertThat(bootstrappedSnapshot.properties())
                .containsEntry(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER, "2");

        snapshotStore.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 5L, new int[] {5, 5}, "fifth")),
                gen::getPartition,
                kv -> 1);
        Snapshot restoredSnapshot = snapshotStore.snapshotManager().latestSnapshot();
        assertThat(restoredSnapshot.properties())
                .containsEntry(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER, "3");
    }

    @Test
    public void testSnapshotSequenceNumberInitUsesWriterCreationSnapshot() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "snapshot");
        TestFileStore snapshotStore = createStoreWithOptions(options);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue first = gen.nextInsert("20201110", 10, 1L, new int[] {1, 1}, "first");
        snapshotStore.commitData(Collections.singletonList(first), gen::getPartition, kv -> 1);
        Snapshot snapshotWithProperties = snapshotStore.snapshotManager().latestSnapshot();
        assertThat(snapshotWithProperties.properties())
                .containsEntry(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER, "0");

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toUri()));
        TableSchema scanSchema =
                schemaManager.commitChanges(
                        SchemaChange.setOption(
                                CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "scan"));
        TestFileStore scanStore = createStore(scanSchema);

        scanStore.commitData(
                Collections.singletonList(
                        gen.nextInsert("20201110", 10, 2L, new int[] {2, 2}, "second")),
                gen::getPartition,
                kv -> 2);
        assertThat(
                        SequenceSnapshotProperties.maxSequenceNumber(
                                snapshotStore.snapshotManager().latestSnapshot()))
                .isEmpty();

        KeyValueFileStoreWrite write = (KeyValueFileStoreWrite) snapshotStore.newWrite();
        assertThat(write.startingMaxSequenceNumber(-1, snapshotWithProperties)).isEqualTo(0);
    }

    private CompactStrategy createCompactStrategy(Map<String, String> options) throws Exception {
        KeyValueFileStoreWrite write = createWriteWithOptions(options);
        write.withIOManager(ioManager);
        TestKeyValueGenerator gen = new TestKeyValueGenerator();

        KeyValue keyValue = gen.next();
        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.createWriterContainer(gen.getPartition(keyValue), 1);
        MergeTreeWriter writer = (MergeTreeWriter) writerContainer.writer;
        try (MergeTreeCompactManager compactManager =
                (MergeTreeCompactManager) writer.compactManager()) {
            return compactManager.getStrategy();
        }
    }

    private KeyValueFileStoreWrite createWriteWithOptions(Map<String, String> options)
            throws Exception {
        return (KeyValueFileStoreWrite) createStoreWithOptions(options).newWrite();
    }

    private TestFileStore createStoreWithOptions(Map<String, String> options) throws Exception {
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
        return createStore(schema);
    }

    private TestFileStore createStore(TableSchema schema) {
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
