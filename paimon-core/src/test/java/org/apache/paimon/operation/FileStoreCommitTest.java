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
import org.apache.paimon.TestAppendFileStore;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileStoreCommitImpl}. */
public class FileStoreCommitTest {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitTest.class);

    private TestKeyValueGenerator gen;
    private String failingName;
    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        gen = new TestKeyValueGenerator();
        // for failure tests
        failingName = UUID.randomUUID().toString();
        FailingFileIO.reset(failingName, 100, 100);
    }

    @AfterEach
    public void afterEach() {
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempDir.toString());
        assertThat(FailingFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(FailingFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "false,NONE",
        "false,INPUT",
        "false,FULL_COMPACTION",
        "true,NONE",
        "true,INPUT",
        "true,FULL_COMPACTION"
    })
    public void testSingleCommitUser(boolean failing, String changelogProducer) throws Exception {
        testRandomConcurrentNoConflict(
                1, failing, CoreOptions.ChangelogProducer.valueOf(changelogProducer));
    }

    @ParameterizedTest
    @CsvSource({
        "false,NONE",
        "false,INPUT",
        "false,FULL_COMPACTION",
        "true,NONE",
        "true,INPUT",
        "true,FULL_COMPACTION"
    })
    public void testManyCommitUsersNoConflict(boolean failing, String changelogProducer)
            throws Exception {
        testRandomConcurrentNoConflict(
                ThreadLocalRandom.current().nextInt(3) + 2,
                failing,
                CoreOptions.ChangelogProducer.valueOf(changelogProducer));
    }

    @ParameterizedTest
    @CsvSource({
        "false,NONE",
        "false,INPUT",
        "false,FULL_COMPACTION",
        "true,NONE",
        "true,INPUT",
        "true,FULL_COMPACTION"
    })
    public void testManyCommitUsersWithConflict(boolean failing, String changelogProducer)
            throws Exception {
        testRandomConcurrentWithConflict(
                ThreadLocalRandom.current().nextInt(3) + 2,
                failing,
                CoreOptions.ChangelogProducer.valueOf(changelogProducer));
    }

    @Test
    public void testLatestHint() throws Exception {
        testRandomConcurrentNoConflict(1, false, CoreOptions.ChangelogProducer.NONE);
        SnapshotManager snapshotManager = createStore(false, 1).snapshotManager();
        Path snapshotDir = snapshotManager.snapshotDirectory();
        Path latest = new Path(snapshotDir, SnapshotManager.LATEST);

        assertThat(new LocalFileIO().exists(latest)).isTrue();

        Long latestId = snapshotManager.latestSnapshotId();

        // remove latest hint file
        LocalFileIO.create().delete(latest, false);

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latestId);
    }

    @Test
    public void testFilterCommittedAfterExpire() throws Exception {
        testRandomConcurrentNoConflict(1, false, CoreOptions.ChangelogProducer.NONE);
        // remove first snapshot to mimic expiration
        TestFileStore store = createStore(false);
        SnapshotManager snapshotManager = store.snapshotManager();
        Path firstSnapshotPath = snapshotManager.snapshotPath(Snapshot.FIRST_SNAPSHOT_ID);
        LocalFileIO.create().deleteQuietly(firstSnapshotPath);
        // this test succeeds if this call does not fail
        store.newCommit(UUID.randomUUID().toString()).filterCommitted(Collections.singleton(999L));
    }

    @Test
    public void testFilterAllCommits() throws Exception {
        testRandomConcurrentNoConflict(1, false, CoreOptions.ChangelogProducer.NONE);
        TestFileStore store = createStore(false);
        SnapshotManager snapshotManager = store.snapshotManager();
        long latestSnapshotId = snapshotManager.latestSnapshotId();

        LinkedHashSet<Long> commitIdentifiers = new LinkedHashSet<>();
        String user = "";
        for (long id = Snapshot.FIRST_SNAPSHOT_ID; id <= latestSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            commitIdentifiers.add(snapshot.commitIdentifier());
            user = snapshot.commitUser();
        }

        // all commit identifiers should be filtered out
        Set<Long> remaining = store.newCommit(user).filterCommitted(commitIdentifiers);
        assertThat(remaining).isEmpty();
    }

    protected void testRandomConcurrentNoConflict(
            int numThreads, boolean failing, CoreOptions.ChangelogProducer changelogProducer)
            throws Exception {
        // prepare test data
        Map<BinaryRow, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");

        List<Map<BinaryRow, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRow, List<KeyValue>> entry : data.entrySet()) {
            dataPerThread
                    .get(ThreadLocalRandom.current().nextInt(numThreads))
                    .put(entry.getKey(), entry.getValue());
        }

        testRandomConcurrent(
                dataPerThread,
                changelogProducer == CoreOptions.ChangelogProducer.NONE,
                failing,
                changelogProducer);
    }

    protected void testRandomConcurrentWithConflict(
            int numThreads, boolean failing, CoreOptions.ChangelogProducer changelogProducer)
            throws Exception {
        // prepare test data
        Map<BinaryRow, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");

        List<Map<BinaryRow, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRow, List<KeyValue>> entry : data.entrySet()) {
            for (KeyValue kv : entry.getValue()) {
                dataPerThread
                        .get(Math.abs(kv.key().hashCode()) % numThreads)
                        .computeIfAbsent(entry.getKey(), p -> new ArrayList<>())
                        .add(kv);
            }
        }

        testRandomConcurrent(dataPerThread, false, failing, changelogProducer);
    }

    private void testRandomConcurrent(
            List<Map<BinaryRow, List<KeyValue>>> dataPerThread,
            boolean enableOverwrite,
            boolean failing,
            CoreOptions.ChangelogProducer changelogProducer)
            throws Exception {
        // concurrent commits
        List<TestCommitThread> threads = new ArrayList<>();
        for (Map<BinaryRow, List<KeyValue>> data : dataPerThread) {
            TestCommitThread thread =
                    new TestCommitThread(
                            TestKeyValueGenerator.KEY_TYPE,
                            TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                            enableOverwrite,
                            data,
                            createStore(failing, 1, changelogProducer),
                            createStore(false, 1, changelogProducer));
            thread.start();
            threads.add(thread);
        }

        TestFileStore store = createStore(false, 1, changelogProducer);

        // calculate expected results
        List<KeyValue> threadResults = new ArrayList<>();
        for (TestCommitThread thread : threads) {
            thread.join();
            threadResults.addAll(thread.getResult());
        }
        Map<BinaryRow, BinaryRow> expected = store.toKvMap(threadResults);

        // read actual data and compare
        Long snapshotId = store.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();
        List<KeyValue> actualKvs = store.readKvsFromSnapshot(snapshotId);
        gen.sort(actualKvs);
        logData(() -> actualKvs, "raw read results");
        Map<BinaryRow, BinaryRow> actual = store.toKvMap(actualKvs);
        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);

        // read changelog and compare
        if (changelogProducer != CoreOptions.ChangelogProducer.NONE) {
            List<KeyValue> actualChangelog = store.readAllChangelogUntilSnapshot(snapshotId);
            logData(() -> actualChangelog, "raw changelog results");
            Map<BinaryRow, BinaryRow> actualChangelogMap = store.toKvMap(actualChangelog);
            logData(() -> kvMapToKvList(actualChangelogMap), "actual changelog map");
            assertThat(actualChangelogMap).isEqualTo(expected);

            if (changelogProducer == CoreOptions.ChangelogProducer.FULL_COMPACTION) {
                validateFullChangelog(actualChangelog);
            }
        }
    }

    private void validateFullChangelog(List<KeyValue> changelog) {
        Map<BinaryRow, KeyValue> kvMap = new HashMap<>();
        Map<BinaryRow, RowKind> kindMap = new HashMap<>();
        for (KeyValue kv : changelog) {
            BinaryRow key = TestKeyValueGenerator.KEY_SERIALIZER.toBinaryRow(kv.key()).copy();
            switch (kv.valueKind()) {
                case INSERT:
                    assertThat(kvMap).doesNotContainKey(key);
                    if (kindMap.containsKey(key)) {
                        assertThat(kindMap.get(key)).isEqualTo(RowKind.DELETE);
                    }
                    kvMap.put(key, kv);
                    kindMap.put(key, RowKind.INSERT);
                    break;
                case UPDATE_AFTER:
                    assertThat(kvMap).doesNotContainKey(key);
                    assertThat(kindMap.get(key)).isEqualTo(RowKind.UPDATE_BEFORE);
                    kvMap.put(key, kv);
                    kindMap.put(key, RowKind.UPDATE_AFTER);
                    break;
                case UPDATE_BEFORE:
                case DELETE:
                    assertThat(kvMap).containsKey(key);
                    assertThat(kv.value()).isEqualTo(kvMap.get(key).value());
                    assertThat(kindMap.get(key)).isIn(RowKind.INSERT, RowKind.UPDATE_AFTER);
                    kvMap.remove(key);
                    kindMap.put(key, kv.valueKind());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + kv.valueKind().name());
            }
        }
    }

    @Test
    public void testOverwritePartialCommit() throws Exception {
        Map<BinaryRow, List<KeyValue>> data1 =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data1.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "data1");

        TestFileStore store = createStore(false);
        store.commitData(
                data1.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                gen::getPartition,
                kv -> 0);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String dtToOverwrite =
                new ArrayList<>(data1.keySet())
                        .get(random.nextInt(data1.size()))
                        .getString(0)
                        .toString();
        Map<String, String> partitionToOverwrite = new HashMap<>();
        partitionToOverwrite.put("dt", dtToOverwrite);
        if (LOG.isDebugEnabled()) {
            LOG.debug("dtToOverwrite " + dtToOverwrite);
        }

        Map<BinaryRow, List<KeyValue>> data2 =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        // remove all records not belonging to dtToOverwrite
        data2.entrySet().removeIf(e -> !dtToOverwrite.equals(e.getKey().getString(0).toString()));
        logData(
                () ->
                        data2.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "data2");
        List<Snapshot> overwriteSnapshots =
                store.overwriteData(
                        data2.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                        gen::getPartition,
                        kv -> 0,
                        partitionToOverwrite);
        assertThat(overwriteSnapshots.get(0).commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);

        List<KeyValue> expectedKvs = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<KeyValue>> entry : data1.entrySet()) {
            if (dtToOverwrite.equals(entry.getKey().getString(0).toString())) {
                continue;
            }
            expectedKvs.addAll(entry.getValue());
        }
        data2.values().forEach(expectedKvs::addAll);
        gen.sort(expectedKvs);
        Map<BinaryRow, BinaryRow> expected = store.toKvMap(expectedKvs);

        List<KeyValue> actualKvs =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId());
        gen.sort(actualKvs);
        Map<BinaryRow, BinaryRow> actual = store.toKvMap(actualKvs);

        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testSnapshotAddLogOffset() throws Exception {
        TestFileStore store = createStore(false, 2);

        // commit 1
        Map<Integer, Long> offsets = new HashMap<>();
        offsets.put(0, 1L);
        offsets.put(1, 3L);
        Snapshot snapshot =
                store.commitData(generateDataList(10), gen::getPartition, kv -> 0, offsets).get(0);
        assertThat(snapshot.logOffsets()).isEqualTo(offsets);

        // commit 2
        offsets = new HashMap<>();
        offsets.put(1, 8L);
        snapshot =
                store.commitData(generateDataList(10), gen::getPartition, kv -> 0, offsets).get(0);
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(0, 1L);
        expected.put(1, 8L);
        assertThat(snapshot.logOffsets()).isEqualTo(expected);
    }

    @Test
    public void testSnapshotRecordCount() throws Exception {
        TestFileStore store = createStore(false);

        // commit 1
        Snapshot snapshot1 =
                store.commitData(
                                generateDataList(10),
                                gen::getPartition,
                                kv -> 0,
                                Collections.emptyMap())
                        .get(0);
        long deltaRecordCount1 = snapshot1.deltaRecordCount();
        assertThat(deltaRecordCount1).isNotEqualTo(0L);
        assertThat(snapshot1.totalRecordCount()).isEqualTo(deltaRecordCount1);
        assertThat(snapshot1.changelogRecordCount()).isEqualTo(0L);

        // commit 2
        Snapshot snapshot2 =
                store.commitData(
                                generateDataList(20),
                                gen::getPartition,
                                kv -> 0,
                                Collections.emptyMap())
                        .get(0);
        long deltaRecordCount2 = snapshot2.deltaRecordCount();
        assertThat(deltaRecordCount2).isNotEqualTo(0L);
        assertThat(snapshot2.totalRecordCount())
                .isEqualTo(snapshot1.totalRecordCount() + deltaRecordCount2);
        assertThat(snapshot2.changelogRecordCount()).isEqualTo(0L);

        // commit 3
        Snapshot snapshot3 =
                store.commitData(
                                generateDataList(30),
                                gen::getPartition,
                                kv -> 0,
                                Collections.emptyMap())
                        .get(0);
        long deltaRecordCount3 = snapshot3.deltaRecordCount();
        assertThat(deltaRecordCount3).isNotEqualTo(0L);
        assertThat(snapshot3.totalRecordCount())
                .isEqualTo(snapshot2.totalRecordCount() + deltaRecordCount3);
        assertThat(snapshot3.changelogRecordCount()).isEqualTo(0L);
    }

    @Test
    public void testCommitEmpty() throws Exception {
        TestFileStore store = createStore(false, 2);
        Snapshot snapshot =
                store.commitData(
                                generateDataList(10),
                                gen::getPartition,
                                kv -> 0,
                                Collections.emptyMap())
                        .get(0);

        // not commit empty new files
        store.commitDataImpl(
                Collections.emptyList(),
                gen::getPartition,
                kv -> 0,
                false,
                null,
                null,
                Collections.emptyList(),
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
        assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(snapshot.id());

        // commit empty new files
        store.commitDataImpl(
                Collections.emptyList(),
                gen::getPartition,
                kv -> 0,
                false,
                null,
                null,
                Collections.emptyList(),
                (commit, committable) -> {
                    commit.ignoreEmptyCommit(false);
                    commit.commit(committable, Collections.emptyMap());
                });
        assertThat(store.snapshotManager().latestSnapshotId()).isEqualTo(snapshot.id() + 1);
    }

    @Test
    public void testCommitOldSnapshotAgain() throws Exception {
        TestFileStore store = createStore(false, 2);
        List<ManifestCommittable> committables = new ArrayList<>();

        // commit 3 snapshots
        for (int i = 0; i < 3; i++) {
            store.commitDataImpl(
                    generateDataList(10),
                    gen::getPartition,
                    kv -> 0,
                    false,
                    (long) i,
                    null,
                    Collections.emptyList(),
                    (commit, committable) -> {
                        commit.commit(committable, Collections.emptyMap());
                        committables.add(committable);
                    });
        }

        // commit the first snapshot again, should throw exception due to conflicts
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(
                            () ->
                                    store.newCommit()
                                            .commit(
                                                    committables.get(0),
                                                    Collections.emptyMap(),
                                                    true))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Give up committing.");
        }
    }

    @Test
    public void testCommitWatermarkWithValue() throws Exception {
        TestFileStore store = createStore(false, 2);

        // first with watermark
        Snapshot snapshot =
                store.commitDataWatermark(generateDataList(10), gen::getPartition, 1024L).get(0);
        assertThat(snapshot.watermark()).isEqualTo(1024);
    }

    @Test
    public void testCommitWatermark() throws Exception {
        TestFileStore store = createStore(false, 2);

        // first with null
        Snapshot snapshot =
                store.commitDataWatermark(generateDataList(10), gen::getPartition, null).get(0);
        assertThat(snapshot.watermark()).isEqualTo(null);

        // with watermark
        snapshot = store.commitDataWatermark(generateDataList(10), gen::getPartition, 1024L).get(0);
        assertThat(snapshot.watermark()).isEqualTo(1024);

        // lower watermark, the watermark should remain unchanged
        snapshot = store.commitDataWatermark(generateDataList(10), gen::getPartition, 600L).get(0);
        assertThat(snapshot.watermark()).isEqualTo(1024);

        // null watermark, the watermark should remain unchanged
        snapshot = store.commitDataWatermark(generateDataList(10), gen::getPartition, null).get(0);
        assertThat(snapshot.watermark()).isEqualTo(1024);

        // bigger watermark, the watermark should be updated
        snapshot = store.commitDataWatermark(generateDataList(10), gen::getPartition, 2048L).get(0);
        assertThat(snapshot.watermark()).isEqualTo(2048);
    }

    @Test
    public void testDropPartitions() throws Exception {
        // generate and commit initial data
        Map<BinaryRow, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(50, 1000));
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "data");

        TestFileStore store = createStore(false);
        store.commitData(
                data.values().stream().flatMap(Collection::stream).collect(Collectors.toList()),
                gen::getPartition,
                kv -> 0,
                Collections.singletonMap(0, 1L));

        // generate partitions to be dropped
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int partitionsToDrop = random.nextInt(data.size()) + 1;
        boolean specifyHr = random.nextBoolean();
        int index = random.nextInt(data.size() - partitionsToDrop + 1);
        List<Map<String, String>> partitions = new ArrayList<>();
        for (int i = 0; i < partitionsToDrop; i++) {
            Map<String, String> partition = new HashMap<>();
            // partition 'dt'
            partition.put("dt", new ArrayList<>(data.keySet()).get(index).getString(0).toString());
            // partition 'hr'
            if (specifyHr && random.nextBoolean()) {
                partition.put(
                        "hr", String.valueOf(new ArrayList<>(data.keySet()).get(index).getInt(1)));
            }
            index++;

            partitions.add(partition);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "partitionsToDrop "
                            + partitions.stream()
                                    .map(Objects::toString)
                                    .collect(Collectors.joining(",")));
        }

        Snapshot snapshot = store.dropPartitions(partitions);

        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);

        // check data
        org.apache.paimon.predicate.Predicate partitionFilter =
                partitions.stream()
                        .map(
                                partition ->
                                        PredicateBuilder.partition(
                                                partition, TestKeyValueGenerator.DEFAULT_PART_TYPE))
                        .reduce(PredicateBuilder::or)
                        .get();

        List<KeyValue> expectedKvs = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<KeyValue>> entry : data.entrySet()) {
            if (partitionFilter.test(entry.getKey())) {
                continue;
            }
            expectedKvs.addAll(entry.getValue());
        }
        gen.sort(expectedKvs);
        Map<BinaryRow, BinaryRow> expected = store.toKvMap(expectedKvs);

        List<KeyValue> actualKvs =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId());
        gen.sort(actualKvs);
        Map<BinaryRow, BinaryRow> actual = store.toKvMap(actualKvs);

        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testDropEmptyPartition() throws Exception {
        TestFileStore store = createStore(false);
        assertThatThrownBy(() -> store.dropPartitions(Collections.emptyList()))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Partitions list cannot be empty."));
    }

    @Test
    public void testIndexFiles() throws Exception {
        TestFileStore store = createStore(false, 2);
        IndexFileHandler indexFileHandler = store.newIndexFileHandler();

        KeyValue record1 = gen.next();
        BinaryRow part1 = gen.getPartition(record1);
        KeyValue record2 = record1;
        BinaryRow part2 = part1;
        while (part1.equals(part2)) {
            record2 = gen.next();
            part2 = gen.getPartition(record2);
        }

        // init write
        store.commitDataIndex(
                record1,
                gen::getPartition,
                0,
                indexFileHandler.writeHashIndex(new int[] {1, 2, 5}));
        store.commitDataIndex(
                record1, gen::getPartition, 1, indexFileHandler.writeHashIndex(new int[] {6, 8}));
        store.commitDataIndex(
                record2, gen::getPartition, 2, indexFileHandler.writeHashIndex(new int[] {3, 5}));

        Snapshot snapshot = store.snapshotManager().latestSnapshot();

        // assert part1
        List<IndexManifestEntry> part1Index =
                indexFileHandler.scan(snapshot.id(), HASH_INDEX, part1);
        assertThat(part1Index.size()).isEqualTo(2);

        IndexManifestEntry indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 0).findAny().get();
        assertThat(indexFileHandler.readHashIndexList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(1, 2, 5);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 1).findAny().get();
        assertThat(indexFileHandler.readHashIndexList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(6, 8);

        // assert part2
        List<IndexManifestEntry> part2Index =
                indexFileHandler.scan(snapshot.id(), HASH_INDEX, part2);
        assertThat(part2Index.size()).isEqualTo(1);
        assertThat(part2Index.get(0).bucket()).isEqualTo(2);
        assertThat(indexFileHandler.readHashIndexList(part2Index.get(0).indexFile()))
                .containsExactlyInAnyOrder(3, 5);

        // update part1
        store.commitDataIndex(
                record1, gen::getPartition, 0, indexFileHandler.writeHashIndex(new int[] {1, 4}));
        snapshot = store.snapshotManager().latestSnapshot();

        // assert update part1
        part1Index = indexFileHandler.scan(snapshot.id(), HASH_INDEX, part1);
        assertThat(part1Index.size()).isEqualTo(2);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 0).findAny().get();
        assertThat(indexFileHandler.readHashIndexList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(1, 4);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 1).findAny().get();
        assertThat(indexFileHandler.readHashIndexList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(6, 8);

        // assert scan one bucket
        Optional<IndexFileMeta> file = indexFileHandler.scanHashIndex(snapshot.id(), part1, 0);
        assertThat(file).isPresent();
        assertThat(indexFileHandler.readHashIndexList(file.get())).containsExactlyInAnyOrder(1, 4);

        // overwrite one partition
        store.options().toConfiguration().set(CoreOptions.DYNAMIC_PARTITION_OVERWRITE, true);
        store.overwriteData(
                Collections.singletonList(record1), gen::getPartition, kv -> 0, new HashMap<>());
        snapshot = store.snapshotManager().latestSnapshot();
        file = indexFileHandler.scanHashIndex(snapshot.id(), part1, 0);
        assertThat(file).isEmpty();
        file = indexFileHandler.scanHashIndex(snapshot.id(), part2, 2);
        assertThat(file).isPresent();

        // overwrite all partitions
        store.options().toConfiguration().set(CoreOptions.DYNAMIC_PARTITION_OVERWRITE, false);
        store.overwriteData(
                Collections.singletonList(record1), gen::getPartition, kv -> 0, new HashMap<>());
        snapshot = store.snapshotManager().latestSnapshot();
        file = indexFileHandler.scanHashIndex(snapshot.id(), part2, 2);
        assertThat(file).isEmpty();
    }

    @Test
    public void testWriteStats() throws Exception {
        TestFileStore store = createStore(false, 1, CoreOptions.ChangelogProducer.NONE);
        StatsFileHandler statsFileHandler = store.newStatsFileHandler();
        FileStoreCommitImpl fileStoreCommit = store.newCommit();
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0, Collections.emptyMap());
        Snapshot latestSnapshot = store.snapshotManager().latestSnapshot();

        // Analyze and check
        HashMap<String, ColStats<?>> fakeColStatsMap = new HashMap<>();
        fakeColStatsMap.put("orderId", ColStats.newColStats(3, 10L, 1L, 10L, 0L, 8L, 8L));
        Statistics fakeStats =
                new Statistics(
                        latestSnapshot.id(),
                        latestSnapshot.schemaId(),
                        10L,
                        1000L,
                        fakeColStatsMap);
        fileStoreCommit.commitStatistics(fakeStats, Long.MAX_VALUE);
        Optional<Statistics> readStats = statsFileHandler.readStats();
        assertThat(readStats).isPresent();
        assertThat(readStats.get()).isEqualTo(fakeStats);

        // New snapshot will inherit last snapshot's stats
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0, Collections.emptyMap());
        readStats = statsFileHandler.readStats();
        assertThat(readStats).isPresent();
        assertThat(readStats.get()).isEqualTo(fakeStats);

        // When table schema is modified, new snapshot will not inherit last snapshot's stats
        ArrayList<DataField> newFields =
                new ArrayList<>(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields());
        newFields.add(new DataField(-1, "newField", DataTypes.INT()));
        store.mergeSchema(new RowType(false, newFields), true);
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0, Collections.emptyMap());
        readStats = statsFileHandler.readStats();
        assertThat(readStats).isEmpty();

        // Then we need to analyze again
        latestSnapshot = store.snapshotManager().latestSnapshot();
        fakeColStatsMap = new HashMap<>();
        fakeColStatsMap.put("orderId", ColStats.newColStats(3, 30L, 1L, 30L, 0L, 8L, 8L));
        fakeStats =
                new Statistics(
                        latestSnapshot.id(),
                        latestSnapshot.schemaId(),
                        30L,
                        3000L,
                        fakeColStatsMap);
        fileStoreCommit.commitStatistics(fakeStats, Long.MAX_VALUE);
        readStats = statsFileHandler.readStats();
        assertThat(readStats).isPresent();
        assertThat(readStats.get()).isEqualTo(fakeStats);

        // Analyze without col stats and check
        latestSnapshot = store.snapshotManager().latestSnapshot();
        fakeStats = new Statistics(latestSnapshot.id(), latestSnapshot.schemaId(), 30L, 3000L);
        fileStoreCommit.commitStatistics(fakeStats, Long.MAX_VALUE);
        readStats = statsFileHandler.readStats();
        assertThat(readStats).isPresent();
        assertThat(readStats.get()).isEqualTo(fakeStats);
    }

    @Test
    public void testDVIndexFiles() throws Exception {
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());

        // commit 1
        CommitMessage commitMessage1 =
                store.writeDVIndexFiles(
                        BinaryRow.EMPTY_ROW,
                        0,
                        Collections.singletonMap("f1", Arrays.asList(1, 3)));
        CommitMessage commitMessage2 =
                store.writeDVIndexFiles(
                        BinaryRow.EMPTY_ROW,
                        0,
                        Collections.singletonMap("f2", Arrays.asList(2, 4)));
        store.commit(commitMessage1, commitMessage2);

        // assert 1
        assertThat(store.scanDVIndexFiles(BinaryRow.EMPTY_ROW, 0).size()).isEqualTo(2);
        DeletionVectorsMaintainer maintainer =
                store.createOrRestoreDVMaintainer(BinaryRow.EMPTY_ROW, 0);
        Map<String, DeletionVector> dvs = maintainer.deletionVectors();
        assertThat(dvs.size()).isEqualTo(2);
        assertThat(dvs.get("f2").isDeleted(2)).isTrue();
        assertThat(dvs.get("f2").isDeleted(3)).isFalse();
        assertThat(dvs.get("f2").isDeleted(4)).isTrue();

        // commit 2
        CommitMessage commitMessage3 =
                store.writeDVIndexFiles(
                        BinaryRow.EMPTY_ROW, 0, Collections.singletonMap("f2", Arrays.asList(3)));
        CommitMessage commitMessage4 =
                store.removeIndexFiles(
                        BinaryRow.EMPTY_ROW,
                        0,
                        ((CommitMessageImpl) commitMessage1).indexIncrement().newIndexFiles());
        store.commit(commitMessage3, commitMessage4);

        // assert 2
        assertThat(store.scanDVIndexFiles(BinaryRow.EMPTY_ROW, 0).size()).isEqualTo(2);
        maintainer = store.createOrRestoreDVMaintainer(BinaryRow.EMPTY_ROW, 0);
        dvs = maintainer.deletionVectors();
        assertThat(dvs.size()).isEqualTo(2);
        assertThat(dvs.get("f2").isDeleted(3)).isTrue();
    }

    private TestFileStore createStore(boolean failing) throws Exception {
        return createStore(failing, 1);
    }

    private TestFileStore createStore(boolean failing, int numBucket) throws Exception {
        return createStore(failing, numBucket, CoreOptions.ChangelogProducer.NONE);
    }

    private TestFileStore createStore(
            boolean failing, int numBucket, CoreOptions.ChangelogProducer changelogProducer)
            throws Exception {
        String root =
                failing
                        ? FailingFileIO.getFailingPath(failingName, tempDir.toString())
                        : TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(new LocalFileIO(), path),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                TestKeyValueGenerator.getPrimaryKeys(
                                        TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                                Collections.emptyMap(),
                                null));
        return new TestFileStore.Builder(
                        "avro",
                        root,
                        numBucket,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory(),
                        tableSchema)
                .changelogProducer(changelogProducer)
                .build();
    }

    private List<KeyValue> generateDataList(int numRecords) {
        return generateData(numRecords).values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Map<BinaryRow, List<KeyValue>> generateData(int numRecords) {
        Map<BinaryRow, List<KeyValue>> data = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            KeyValue kv = gen.next();
            data.computeIfAbsent(gen.getPartition(kv), p -> new ArrayList<>()).add(kv);
        }
        return data;
    }

    private List<KeyValue> kvMapToKvList(Map<BinaryRow, BinaryRow> map) {
        return map.entrySet().stream()
                .map(e -> new KeyValue().replace(e.getKey(), -1, RowKind.INSERT, e.getValue()))
                .collect(Collectors.toList());
    }

    private void logData(Supplier<List<KeyValue>> supplier, String name) {
        if (!LOG.isDebugEnabled()) {
            return;
        }

        LOG.debug("========== Beginning of " + name + " ==========");
        for (KeyValue kv : supplier.get()) {
            LOG.debug(
                    kv.toString(
                            TestKeyValueGenerator.KEY_TYPE,
                            TestKeyValueGenerator.DEFAULT_ROW_TYPE));
        }
        LOG.debug("========== End of " + name + " ==========");
    }
}
