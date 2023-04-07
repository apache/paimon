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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FileStoreExpireImpl}. After expiration, empty data file directories (buckets and
 * partitions) are deleted. It didn't extend {@link FileStoreExpireTestBase} because there are not
 * too many codes can be reused.
 */
public class FileStoreExpireDeleteDirTest {

    @TempDir java.nio.file.Path tempDir;

    private final FileIO fileIO = new LocalFileIO();

    private long commitIdentifier;
    private String root;
    private TestKeyValueGenerator gen;
    private TestFileStore store;

    @BeforeEach
    public void setup() throws Exception {
        commitIdentifier = 0L;
        root = tempDir.toString();
    }

    /**
     * This test checks FileStoreExpire can delete empty partition directories in multiple partition
     * situation. The partition keys are (dt, hr). Test process:
     *
     * <ul>
     *   <li>1. Generate snapshot 1 with (0401, 8/12), (0402, 8/12). Each partition has two buckets.
     *   <li>2. Generate snapshot 2 by deleting all data of partition dt=0401 (thus directory
     *       dt=0401 will be deleted after expiring).
     *   <li>3. Generate snapshot 3 by deleting all data of partition dt=0402/hr=8 (thus directory
     *       dt=0402/hr=8 will be deleted after expiring).
     *   <li>4. Generate snapshot 4 by deleting all data of partition dt=0402/hr=12/bucket-0 (thus
     *       directory dt=0402/hr=12/bucket-0 will be deleted after expiring).
     *   <li>5. Expire snapshot 1-3 (dt=0402/hr=20/bucket-1 survives) and check.
     * </ul>
     */
    @Test
    public void testMultiPartitions() throws Exception {
        createStore(TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);
        gen = new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);

        // step 1: generate snapshot 1 by writing 5 randomly generated records to each bucket
        // writers for each bucket
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();

        List<BinaryRow> partitions = new ArrayList<>();
        for (String dt : Arrays.asList("0401", "0402")) {
            for (int hr : Arrays.asList(8, 12)) {
                for (int bucket : Arrays.asList(0, 1)) {
                    List<KeyValue> kvs = partitionedData(5, dt, hr);
                    BinaryRow partition = gen.getPartition(kvs.get(0));
                    partitions.add(partition);
                    writeData(kvs, partition, bucket, writers);
                }
            }
        }

        commitData(writers);
        // check all paths exist
        for (BinaryRow partition : partitions) {
            for (int bucket : Arrays.asList(0, 1)) {
                assertPathExists(store.pathFactory().bucketPath(partition, bucket));
            }
        }

        // step 2: generate snapshot 2 by cleaning partition dt=0401 (through overwriting with an
        // empty ManifestCommittable)
        FileStoreCommitImpl commit = store.newCommit();
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("dt", "0401");
        commit.overwrite(
                partitionSpec, new ManifestCommittable(commitIdentifier++), Collections.emptyMap());

        // step 3: generate snapshot 3 by cleaning partition dt=0402/hr=10
        partitionSpec.put("dt", "0402");
        partitionSpec.put("hr", "8");
        commit.overwrite(
                partitionSpec, new ManifestCommittable(commitIdentifier++), Collections.emptyMap());

        // step 4: generate snapshot 4 by cleaning dt=0402/hr=12/bucket-0
        // manually make delete ManifestEntry
        BinaryRow partition = partitions.get(7);
        Predicate partitionFilter =
                PredicateBuilder.equalPartition(partition, TestKeyValueGenerator.DEFAULT_PART_TYPE);
        List<ManifestEntry> bucketEntries =
                store.newScan()
                        .withSnapshot(3)
                        .withPartitionFilter(partitionFilter)
                        .withBucket(0)
                        .plan()
                        .files();
        List<ManifestEntry> delete =
                bucketEntries.stream()
                        .map(
                                entry ->
                                        new ManifestEntry(
                                                FileKind.DELETE, partition, 0, 2, entry.file()))
                        .collect(Collectors.toList());
        // commit
        commit.tryCommitOnce(
                delete,
                Collections.emptyList(),
                commitIdentifier++,
                null,
                Collections.emptyMap(),
                Snapshot.CommitKind.APPEND,
                3L,
                null);

        // step 5: expire and check file paths
        store.newExpire(1, 1, Long.MAX_VALUE).expire();
        // whole dt=0401 is deleted
        assertPathNotExists(new Path(root, "dt=0401"));
        // whole dt=0402/hr=8 is deleted
        assertPathNotExists(new Path(root, "dt=0402/hr=8"));
        // for dt=0402/hr=12, bucket-0 is delete but bucket-1 survives
        assertPathNotExists(store.pathFactory().bucketPath(partition, 0));
        assertPathExists(store.pathFactory().bucketPath(partition, 1));
    }

    // only exists bucket directories
    @Test
    public void testNoPartitions() throws Exception {
        createStore(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);
        gen = new TestKeyValueGenerator(TestKeyValueGenerator.GeneratorMode.NON_PARTITIONED);

        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers = new HashMap<>();
        for (int bucket : Arrays.asList(0, 1)) {
            List<KeyValue> kvs = partitionedData(5);
            BinaryRow partition = gen.getPartition(kvs.get(0));
            writeData(kvs, partition, bucket, writers);
        }
        commitData(writers);

        // cleaning bucket 0
        List<ManifestEntry> bucketEntries =
                store.newScan().withSnapshot(1).withBucket(0).plan().files();
        BinaryRow partition = gen.getPartition(gen.next());
        List<ManifestEntry> delete =
                bucketEntries.stream()
                        .map(
                                entry ->
                                        new ManifestEntry(
                                                FileKind.DELETE, partition, 0, 2, entry.file()))
                        .collect(Collectors.toList());
        // commit
        store.newCommit()
                .tryCommitOnce(
                        delete,
                        Collections.emptyList(),
                        commitIdentifier++,
                        null,
                        Collections.emptyMap(),
                        Snapshot.CommitKind.APPEND,
                        1L,
                        null);

        // check before expiring
        assertPathExists(store.pathFactory().bucketPath(partition, 0));
        assertPathExists(store.pathFactory().bucketPath(partition, 1));

        // check after expiring
        store.newExpire(1, 1, Long.MAX_VALUE).expire();
        assertPathNotExists(store.pathFactory().bucketPath(partition, 0));
        assertPathExists(store.pathFactory().bucketPath(partition, 1));
    }

    private void createStore(TestKeyValueGenerator.GeneratorMode mode) throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        RowType rowType, partitionType;
        switch (mode) {
            case NON_PARTITIONED:
                rowType = TestKeyValueGenerator.NON_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.NON_PARTITIONED_PART_TYPE;
                break;
            case SINGLE_PARTITIONED:
                rowType = TestKeyValueGenerator.SINGLE_PARTITIONED_ROW_TYPE;
                partitionType = TestKeyValueGenerator.SINGLE_PARTITIONED_PART_TYPE;
                break;
            case MULTI_PARTITIONED:
                rowType = TestKeyValueGenerator.DEFAULT_ROW_TYPE;
                partitionType = TestKeyValueGenerator.DEFAULT_PART_TYPE;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported generator mode: " + mode);
        }

        store =
                new TestFileStore.Builder(
                                "avro",
                                root,
                                2,
                                partitionType,
                                TestKeyValueGenerator.KEY_TYPE,
                                rowType,
                                TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                                DeduplicateMergeFunction.factory())
                        .changelogProducer(changelogProducer)
                        .build();

        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        rowType.getFields(),
                        partitionType.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(mode),
                        Collections.emptyMap(),
                        null));
    }

    private List<KeyValue> partitionedData(int num, Object... partitionSpec) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            keyValues.add(gen.nextPartitionedData(RowKind.INSERT, partitionSpec));
        }
        return keyValues;
    }

    private void assertPathExists(Path path) throws IOException {
        assertThat(fileIO.exists(path)).isTrue();
    }

    private void assertPathNotExists(Path path) throws IOException {
        assertThat(fileIO.exists(path)).isFalse();
    }

    private void writeData(
            List<KeyValue> kvs,
            BinaryRow partition,
            int bucket,
            Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers)
            throws Exception {
        writers.computeIfAbsent(partition, p -> new HashMap<>())
                .put(bucket, writeData(kvs, partition, bucket));
    }

    // --------------------------------------------------------------------------------
    // writeData & commitData is copied from TestFileStore#commitDataImpl and modified
    // --------------------------------------------------------------------------------
    private RecordWriter<KeyValue> writeData(List<KeyValue> kvs, BinaryRow partition, int bucket)
            throws Exception {
        AbstractFileStoreWrite<KeyValue> write = store.newWrite();
        RecordWriter<KeyValue> writer =
                write.createWriterContainer(partition, bucket, false).writer;
        ((MemoryOwner) writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(
                                TestFileStore.WRITE_BUFFER_SIZE.getBytes(),
                                (int) TestFileStore.PAGE_SIZE.getBytes()));
        for (KeyValue kv : kvs) {
            writer.write(kv);
        }
        return writer;
    }

    private void commitData(Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers)
            throws Exception {
        FileStoreCommit commit = store.newCommit();
        ManifestCommittable committable = new ManifestCommittable(commitIdentifier++, null);
        for (Map.Entry<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> entryWithPartition :
                writers.entrySet()) {
            for (Map.Entry<Integer, RecordWriter<KeyValue>> entryWithBucket :
                    entryWithPartition.getValue().entrySet()) {
                CommitIncrement increment = entryWithBucket.getValue().prepareCommit(false);
                committable.addFileCommittable(
                        new CommitMessageImpl(
                                entryWithPartition.getKey(),
                                entryWithBucket.getKey(),
                                increment.newFilesIncrement(),
                                increment.compactIncrement()));
            }
        }

        commit.commit(committable, Collections.emptyMap());

        writers.values().stream()
                .flatMap(m -> m.values().stream())
                .forEach(
                        w -> {
                            try {
                                // wait for compaction to end, otherwise orphan files may occur
                                // see CompactManager#cancelCompaction for more info
                                w.sync();
                                w.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
