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
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.commit.ConflictDetection;
import org.apache.paimon.operation.commit.ManifestEntryChanges;
import org.apache.paimon.operation.commit.RetryCommitResult;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FailingFileIO;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.apache.paimon.utils.HintFileUtils.LATEST;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
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
        Path latest = new Path(snapshotDir, LATEST);

        assertThat(new LocalFileIO().exists(latest)).isTrue();

        Long latestId = snapshotManager.latestSnapshotId();

        // remove latest hint file
        LocalFileIO.create().delete(latest, false);

        assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latestId);
    }

    @Test
    public void testWriteOnlySnapshotSequenceCommitChecksRescaledBucketNumber() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        options.put(CoreOptions.WRITE_SEQUENCE_NUMBER_INIT_MODE.key(), "snapshot");
        options.put(CoreOptions.BUCKET.key(), "2");
        options.put(CoreOptions.BUCKET_KEY.key(), "orderId");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        BinaryRow partition =
                gen.getPartition(gen.nextInsert("20201110", 10, 1L, new int[] {1, 1}, "first"));

        try (FileStoreCommitImpl commit = store.newCommit()) {
            assertThat(
                            commit.tryCommitOnce(
                                            null,
                                            Collections.singletonList(addFile(partition, 1, 2, 0)),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            0,
                                            null,
                                            new HashMap<>(),
                                            Snapshot.CommitKind.APPEND,
                                            false,
                                            null,
                                            false,
                                            null)
                                    .isSuccess())
                    .isTrue();
        }
        assertThat(store.snapshotManager().latestSnapshot().properties())
                .containsKey(SequenceSnapshotProperties.MAX_SEQUENCE_NUMBER);

        Map<String, String> rescaledOptions = new HashMap<>(options);
        rescaledOptions.put(CoreOptions.BUCKET.key(), "4");
        TestAppendFileStore rescaledStore =
                TestAppendFileStore.createAppendStore(tempDir, rescaledOptions);
        try (FileStoreCommitImpl commit = rescaledStore.newCommit()) {
            assertThatThrownBy(
                            () ->
                                    commit.tryCommitOnce(
                                            null,
                                            Collections.singletonList(addFile(partition, 1, 4, 1)),
                                            Collections.emptyList(),
                                            Collections.emptyList(),
                                            1,
                                            null,
                                            new HashMap<>(),
                                            Snapshot.CommitKind.APPEND,
                                            false,
                                            rescaledStore.snapshotManager().latestSnapshot(),
                                            false,
                                            null))
                    .hasMessageContaining("new bucket num 4")
                    .hasMessageContaining("previous bucket num is 2");
        }
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
        try (FileStoreCommit commit = store.newCommit(UUID.randomUUID().toString(), null)) {
            commit.filterCommitted(Collections.singletonList(new ManifestCommittable(999L)));
        }
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
        try (FileStoreCommit commit = store.newCommit(user, null)) {
            assertThat(
                            commit.filterCommitted(
                                    commitIdentifiers.stream()
                                            .sorted()
                                            .map(ManifestCommittable::new)
                                            .collect(Collectors.toList())))
                    .isEmpty();
        }
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
                // overwrite cannot produce changelog
                // so only enable it when changelog producer is none
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
    public void testSnapshotRecordCount() throws Exception {
        TestFileStore store = createStore(false);

        // commit 1
        Snapshot snapshot1 =
                store.commitData(generateDataList(10), gen::getPartition, kv -> 0).get(0);
        long deltaRecordCount1 = snapshot1.deltaRecordCount();
        assertThat(deltaRecordCount1).isNotEqualTo(0L);
        assertThat(snapshot1.totalRecordCount()).isEqualTo(deltaRecordCount1);
        assertThat(snapshot1.changelogRecordCount()).isNull();

        // commit 2
        Snapshot snapshot2 =
                store.commitData(generateDataList(20), gen::getPartition, kv -> 0).get(0);
        long deltaRecordCount2 = snapshot2.deltaRecordCount();
        assertThat(deltaRecordCount2).isNotEqualTo(0L);
        assertThat(snapshot2.totalRecordCount())
                .isEqualTo(snapshot1.totalRecordCount() + deltaRecordCount2);
        assertThat(snapshot2.changelogRecordCount()).isNull();

        // commit 3
        Snapshot snapshot3 =
                store.commitData(generateDataList(30), gen::getPartition, kv -> 0).get(0);
        long deltaRecordCount3 = snapshot3.deltaRecordCount();
        assertThat(deltaRecordCount3).isNotEqualTo(0L);
        assertThat(snapshot3.totalRecordCount())
                .isEqualTo(snapshot2.totalRecordCount() + deltaRecordCount3);
        assertThat(snapshot3.changelogRecordCount()).isNull();
    }

    @Test
    public void testCommitEmpty() throws Exception {
        TestFileStore store = createStore(false, 2);
        Snapshot snapshot =
                store.commitData(generateDataList(10), gen::getPartition, kv -> 0).get(0);

        // not commit empty new files
        store.commitDataImpl(
                Collections.emptyList(),
                gen::getPartition,
                kv -> 0,
                false,
                null,
                null,
                Collections.emptyList(),
                (commit, committable) -> commit.commit(committable, false));
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
                    commit.commit(committable, false);
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
                        commit.commit(committable, false);
                        committables.add(committable);
                    });
        }

        // commit the first snapshot again, should throw exception due to conflicts
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> store.newCommit().commit(committables.get(0), true))
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
                kv -> 0);

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
                                        createPartitionPredicate(
                                                partition,
                                                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                                                CoreOptions.PARTITION_DEFAULT_NAME.defaultValue()))
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
                indexFileHandler
                        .hashIndex(gen.getPartition(record1), 0)
                        .write(new int[] {1, 2, 5}));
        store.commitDataIndex(
                record1,
                gen::getPartition,
                1,
                indexFileHandler.hashIndex(gen.getPartition(record1), 1).write(new int[] {6, 8}));
        store.commitDataIndex(
                record2,
                gen::getPartition,
                2,
                indexFileHandler.hashIndex(gen.getPartition(record2), 2).write(new int[] {3, 5}));

        Snapshot snapshot = store.snapshotManager().latestSnapshot();

        // assert part1
        List<IndexManifestEntry> part1Index =
                indexFileHandler.scanEntries(snapshot, HASH_INDEX, part1);
        assertThat(part1Index.size()).isEqualTo(2);

        IndexManifestEntry indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 0).findAny().get();
        assertThat(indexFileHandler.hashIndex(part1, 0).readList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(1, 2, 5);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 1).findAny().get();
        assertThat(indexFileHandler.hashIndex(part1, 1).readList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(6, 8);

        // assert part2
        List<IndexManifestEntry> part2Index =
                indexFileHandler.scanEntries(snapshot, HASH_INDEX, part2);
        assertThat(part2Index.size()).isEqualTo(1);
        assertThat(part2Index.get(0).bucket()).isEqualTo(2);
        assertThat(indexFileHandler.hashIndex(part2, 2).readList(part2Index.get(0).indexFile()))
                .containsExactlyInAnyOrder(3, 5);

        // update part1
        store.commitDataIndex(
                record1,
                gen::getPartition,
                0,
                indexFileHandler.hashIndex(gen.getPartition(record1), 0).write(new int[] {1, 4}));
        snapshot = store.snapshotManager().latestSnapshot();

        // assert update part1
        part1Index = indexFileHandler.scanEntries(snapshot, HASH_INDEX, part1);
        assertThat(part1Index.size()).isEqualTo(2);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 0).findAny().get();
        assertThat(indexFileHandler.hashIndex(part1, 0).readList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(1, 4);

        indexManifestEntry =
                part1Index.stream().filter(entry -> entry.bucket() == 1).findAny().get();
        assertThat(indexFileHandler.hashIndex(part1, 1).readList(indexManifestEntry.indexFile()))
                .containsExactlyInAnyOrder(6, 8);

        // assert scan one bucket
        Optional<IndexFileMeta> file = indexFileHandler.scanHashIndex(snapshot, part1, 0);
        assertThat(file).isPresent();
        assertThat(indexFileHandler.hashIndex(part1, 0).readList(file.get()))
                .containsExactlyInAnyOrder(1, 4);

        // overwrite one partition
        store.options().toConfiguration().set(CoreOptions.DYNAMIC_PARTITION_OVERWRITE, true);
        store.overwriteData(
                Collections.singletonList(record1), gen::getPartition, kv -> 0, new HashMap<>());
        snapshot = store.snapshotManager().latestSnapshot();
        file = indexFileHandler.scanHashIndex(snapshot, part1, 0);
        assertThat(file).isEmpty();
        file = indexFileHandler.scanHashIndex(snapshot, part2, 2);
        assertThat(file).isPresent();

        // overwrite all partitions
        store.options().toConfiguration().set(CoreOptions.DYNAMIC_PARTITION_OVERWRITE, false);
        store.overwriteData(
                Collections.singletonList(record1), gen::getPartition, kv -> 0, new HashMap<>());
        snapshot = store.snapshotManager().latestSnapshot();
        file = indexFileHandler.scanHashIndex(snapshot, part2, 2);
        assertThat(file).isEmpty();
    }

    @Test
    public void testWriteStats() throws Exception {
        TestFileStore store = createStore(false, 1, CoreOptions.ChangelogProducer.NONE);
        StatsFileHandler statsFileHandler = store.newStatsFileHandler();
        FileStoreCommitImpl fileStoreCommit = store.newCommit();
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0);
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
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0);
        readStats = statsFileHandler.readStats();
        assertThat(readStats).isPresent();
        assertThat(readStats.get()).isEqualTo(fakeStats);

        // When table schema is modified, new snapshot will not inherit last snapshot's stats
        ArrayList<DataField> newFields =
                new ArrayList<>(TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields());
        newFields.add(new DataField(-1, "newField", DataTypes.INT()));
        store.mergeSchema(new RowType(false, newFields), true, true, true);
        store.commitData(generateDataList(10), gen::getPartition, kv -> 0);
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

        fileStoreCommit.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDVIndexFiles(boolean bitmap64) throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTOR_BITMAP64.key(), String.valueOf(bitmap64));
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        BinaryRow partition = gen.getPartition(gen.next());

        // create files
        CommitMessageImpl commitMessage0 =
                store.writeDataFiles(partition, 0, Arrays.asList("f1", "f2"));
        store.commit(commitMessage0);

        // commit 1
        CommitMessageImpl commitMessage1 =
                store.writeDVIndexFiles(
                        partition, 0, Collections.singletonMap("f1", Arrays.asList(1, 3)));
        CommitMessageImpl commitMessage2 =
                store.writeDVIndexFiles(
                        partition, 0, Collections.singletonMap("f2", Arrays.asList(2, 4)));
        store.commit(commitMessage1, commitMessage2);

        // assert 1
        assertThat(store.scanDVIndexFiles(partition, 0).size()).isEqualTo(2);
        BucketedDvMaintainer maintainer = store.createOrRestoreDVMaintainer(partition, 0);
        Map<String, DeletionVector> dvs = maintainer.deletionVectors();
        assertThat(dvs.size()).isEqualTo(2);
        assertThat(dvs.get("f2").isDeleted(2)).isTrue();
        assertThat(dvs.get("f2").isDeleted(3)).isFalse();
        assertThat(dvs.get("f2").isDeleted(4)).isTrue();

        // commit 2
        List<IndexFileMeta> deleted =
                new ArrayList<>(commitMessage1.newFilesIncrement().newIndexFiles());
        deleted.addAll(commitMessage2.newFilesIncrement().newIndexFiles());
        CommitMessage commitMessage3 = store.removeIndexFiles(partition, 0, deleted);
        CommitMessageImpl commitMessage4 =
                store.writeDVIndexFiles(
                        partition, 0, Collections.singletonMap("f2", Arrays.asList(3)));
        store.commit(commitMessage3, commitMessage4);

        // assert 2
        assertThat(store.scanDVIndexFiles(partition, 0).size()).isEqualTo(1);
        maintainer = store.createOrRestoreDVMaintainer(partition, 0);
        dvs = maintainer.deletionVectors();
        assertThat(dvs.size()).isEqualTo(2);
        assertThat(dvs.get("f1").isDeleted(3)).isTrue();
        assertThat(dvs.get("f2").isDeleted(3)).isTrue();
    }

    @Test
    public void testRollbackToAsLatestFileLevelDeleteIsVisibleToStreaming() throws Exception {
        // Contrast with the DV-only case: when a rollback removes whole data files, the delete is
        // file-level (FileKind.DELETE) and IS visible to streaming readers, no DV needed.
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, new HashMap<>());
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1 (target): f1
        store.commit(store.writeDataFiles(partition, 0, Collections.singletonList("f1")));
        Snapshot target = store.snapshotManager().latestSnapshot();

        // snapshot 2 (latest): add f2
        store.commit(store.writeDataFiles(partition, 0, Collections.singletonList("f2")));

        // roll back to snapshot 1 — f2 must be removed
        try (FileStoreCommitImpl commit = store.newCommit()) {
            assertThat(commit.rollbackToAsLatest(target)).isTrue();
        }
        Snapshot rolledBack = store.snapshotManager().latestSnapshot();

        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        FileStoreTable table = FileStoreTableFactory.create(store.fileIO(), new Path(root));
        List<Split> splits =
                table.newSnapshotReader().withSnapshot(rolledBack).readChanges().splits();

        // f2 is retracted (before side) — the file-level delete is visible to streaming
        assertThat(splits).isNotEmpty();
        IncrementalSplit split = (IncrementalSplit) splits.get(0);
        boolean f2Retracted = split.beforeFiles().stream().anyMatch(f -> f.fileName().equals("f2"));
        assertThat(f2Retracted).isTrue();
    }

    @Test
    public void testNormalDeletionVectorDeleteIsInvisibleToStreamingDelta() throws Exception {
        // Baseline (no rollback involved): without a changelog producer, a plain DV-only delete
        // produces an empty data delta, so the streaming delta read sees no change at all. DV
        // deletes are only streamable via a changelog producer, not via the delta/overwrite path.
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: data file f1, no deletion vectors
        store.commit(store.writeDataFiles(partition, 0, Collections.singletonList("f1")));

        // snapshot 2: a DV-only delete on f1 (data file unchanged, only an index increment)
        store.commit(
                store.writeDVIndexFiles(
                        partition, 0, Collections.singletonMap("f1", Arrays.asList(1, 3))));
        Snapshot dvDelete = store.snapshotManager().latestSnapshot();
        assertThat(store.scanDVIndexFiles(partition, 0)).isNotEmpty();

        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        FileStoreTable table = FileStoreTableFactory.create(store.fileIO(), new Path(root));
        List<Split> splits =
                table.newSnapshotReader().withSnapshot(dvDelete).readChanges().splits();
        assertThat(splits).isEmpty();
    }

    @Test
    public void testRollbackToAsLatestDeletionVectorChangeIsInvisibleToStreaming()
            throws Exception {
        // A DV-only rollback changes only the index manifest (the data files are identical), so it
        // produces an empty data delta and is invisible to streaming readers — consistent with a
        // plain DV delete, which is also invisible to streaming (DV deletes are only streamable via
        // a changelog producer, not via the delta/overwrite path).
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
        TestAppendFileStore store = TestAppendFileStore.createAppendStore(tempDir, options);
        BinaryRow partition = gen.getPartition(gen.next());

        // snapshot 1: data file f1, no deletion vectors (the rollback target)
        store.commit(store.writeDataFiles(partition, 0, Collections.singletonList("f1")));
        Snapshot target = store.snapshotManager().latestSnapshot();

        // snapshot 2: DV-only change — add a deletion vector for f1, data file unchanged
        store.commit(
                store.writeDVIndexFiles(
                        partition, 0, Collections.singletonMap("f1", Arrays.asList(1, 3))));
        // sanity check: f1 really has a deletion vector now
        assertThat(store.scanDVIndexFiles(partition, 0)).isNotEmpty();

        // snapshot 3: roll back to snapshot 1
        try (FileStoreCommitImpl commit = store.newCommit()) {
            assertThat(commit.rollbackToAsLatest(target)).isTrue();
        }
        Snapshot rolledBack = store.snapshotManager().latestSnapshot();

        // The rollback's data delta is empty, so the streaming change read produces no splits — the
        // DV-only rollback is invisible to streaming (batch / time-travel reads remain correct
        // since
        // the snapshot points to the target's index manifest).
        String root = TraceableFileIO.SCHEME + "://" + tempDir.toString();
        FileStoreTable table = FileStoreTableFactory.create(store.fileIO(), new Path(root));
        List<Split> splits =
                table.newSnapshotReader().withSnapshot(rolledBack).readChanges().splits();
        assertThat(splits).isEmpty();
    }

    @Test
    public void testManifestCompact() throws Exception {
        TestFileStore store = createStore(false);

        List<KeyValue> keyValues = generateDataList(1);
        BinaryRow partition = gen.getPartition(keyValues.get(0));
        // commit 1
        Snapshot snapshot1 = store.commitData(keyValues, s -> partition, kv -> 0).get(0);
        // commit 2
        Snapshot snapshot2 =
                store.overwriteData(keyValues, s -> partition, kv -> 0, Collections.emptyMap())
                        .get(0);
        // commit 3
        Snapshot snapshot3 =
                store.overwriteData(keyValues, s -> partition, kv -> 0, Collections.emptyMap())
                        .get(0);

        long deleteNum =
                store.manifestListFactory().create().readDataManifests(snapshot3).stream()
                        .mapToLong(ManifestFileMeta::numDeletedFiles)
                        .sum();
        assertThat(deleteNum).isGreaterThan(0);
        store.newCommit().compactManifest();
        Snapshot latest = store.snapshotManager().latestSnapshot();
        assertThat(
                        store.manifestListFactory().create().readDataManifests(latest).stream()
                                .mapToLong(ManifestFileMeta::numDeletedFiles)
                                .sum())
                .isEqualTo(0);
    }

    @Test
    public void testDropStatsForOverwrite() throws Exception {
        TestFileStore store = createStore(false);
        store.options().toConfiguration().set(CoreOptions.MANIFEST_DELETE_FILE_DROP_STATS, true);

        List<KeyValue> keyValues = generateDataList(1);
        BinaryRow partition = gen.getPartition(keyValues.get(0));
        // commit 1
        Snapshot snapshot1 = store.commitData(keyValues, s -> partition, kv -> 0).get(0);
        // overwrite commit 2
        Snapshot snapshot2 =
                store.overwriteData(keyValues, s -> partition, kv -> 0, Collections.emptyMap())
                        .get(0);
        ManifestFile manifestFile = store.manifestFileFactory().create();
        List<ManifestEntry> entries =
                store.manifestListFactory().create().readDataManifests(snapshot2).stream()
                        .flatMap(meta -> manifestFile.read(meta.fileName()).stream())
                        .collect(Collectors.toList());
        for (ManifestEntry manifestEntry : entries) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                assertThat(manifestEntry.file().valueStats()).isEqualTo(EMPTY_STATS);
            }
        }
    }

    @Test
    public void testManifestCompactFull() throws Exception {
        // Disable full compaction by options.
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(),
                                String.valueOf(Long.MAX_VALUE)));

        List<KeyValue> keyValues = generateDataList(1);
        BinaryRow partition = gen.getPartition(keyValues.get(0));
        // commit 1
        Snapshot snapshot = store.commitData(keyValues, s -> partition, kv -> 0).get(0);

        for (int i = 0; i < 100; i++) {
            snapshot =
                    store.overwriteData(keyValues, s -> partition, kv -> 0, Collections.emptyMap())
                            .get(0);
        }

        long deleteNum =
                store.manifestListFactory().create().readDataManifests(snapshot).stream()
                        .mapToLong(ManifestFileMeta::numDeletedFiles)
                        .sum();
        assertThat(deleteNum).isGreaterThan(0);
        store.newCommit().compactManifest();
        Snapshot latest = store.snapshotManager().latestSnapshot();
        assertThat(
                        store.manifestListFactory().create().readDataManifests(latest).stream()
                                .mapToLong(ManifestFileMeta::numDeletedFiles)
                                .sum())
                .isEqualTo(0);
    }

    @Test
    public void testCommitManifestWithProperties() throws Exception {
        TestFileStore store = createStore(false);

        try (FileStoreCommit fileStoreCommit = store.newCommit()) {
            fileStoreCommit.ignoreEmptyCommit(false);

            // commit with empty properties, the properties in snapshot should be null
            ManifestCommittable manifestCommittable = new ManifestCommittable(0);
            fileStoreCommit.commit(manifestCommittable, false);
            Snapshot snapshot = checkNotNull(store.snapshotManager().latestSnapshot());
            assertThat(snapshot.properties()).isNull();

            // commit with non-empty properties
            manifestCommittable = new ManifestCommittable(0);
            manifestCommittable.addProperty("k1", "v1");
            manifestCommittable.addProperty("k2", "v2");
            fileStoreCommit.commit(manifestCommittable, false);
            snapshot = checkNotNull(store.snapshotManager().latestSnapshot());
            Map<String, String> expectedProps = new HashMap<>();
            expectedProps.put("k1", "v1");
            expectedProps.put("k2", "v2");
            Map<String, String> snapshotProps = snapshot.properties();
            assertThat(snapshotProps).isNotNull();
            assertThat(snapshotProps).isEqualTo(expectedProps);
        }
    }

    @Test
    public void testGlobalIndexCommitChecksExistingRowIds() throws Exception {
        TestFileStore store = createRowTrackingDataEvolutionStore();

        List<KeyValue> keyValues = generateDataList(1);
        BinaryRow partition = gen.getPartition(keyValues.get(0));
        Snapshot dataSnapshot = store.commitData(keyValues, s -> partition, kv -> 0).get(0);
        assertThat(dataSnapshot.nextRowId()).isEqualTo(1L);

        try (FileStoreCommitImpl commit = store.newCommit()) {
            commit.commit(indexCommittable(partition, "existing-index", 0, 0), false);
        }

        Snapshot latest = checkNotNull(store.snapshotManager().latestSnapshot());
        assertThat(latest.indexManifest()).isNotNull();
    }

    @Test
    public void testGlobalIndexCommitFailsForMissingRowIds() throws Exception {
        TestFileStore store = createRowTrackingDataEvolutionStore();

        List<KeyValue> keyValues = generateDataList(1);
        BinaryRow partition = gen.getPartition(keyValues.get(0));
        Snapshot dataSnapshot = store.commitData(keyValues, s -> partition, kv -> 0).get(0);
        long missingRowId = checkNotNull(dataSnapshot.nextRowId());

        try (FileStoreCommitImpl commit = store.newCommit()) {
            assertThatThrownBy(
                            () ->
                                    commit.commit(
                                            indexCommittable(
                                                    partition,
                                                    "missing-index",
                                                    missingRowId,
                                                    missingRowId),
                                            false))
                    .hasMessageContaining("Global index row ID existence conflict")
                    .hasMessageContaining("missing-index")
                    .hasMessageContaining("[" + missingRowId + ", " + missingRowId + "]");
        }
    }

    @Test
    public void testCommitTwiceWithDifferentKind() throws Exception {
        TestFileStore store = createStore(false);
        try (FileStoreCommitImpl commit = store.newCommit()) {
            // Append
            Snapshot firstLatest = store.snapshotManager().latestSnapshot();
            commit.tryCommitOnce(
                    null,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    0,
                    null,
                    Collections.emptyMap(),
                    Snapshot.CommitKind.APPEND,
                    false,
                    firstLatest,
                    true,
                    null);
            // Compact
            commit.tryCommitOnce(
                    RetryCommitResult.forCommitFail(
                            firstLatest, Collections.emptyList(), null, null),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    0,
                    null,
                    Collections.emptyMap(),
                    Snapshot.CommitKind.COMPACT,
                    false,
                    store.snapshotManager().latestSnapshot(),
                    true,
                    null);
        }
        long id = store.snapshotManager().latestSnapshot().id();
        assertThat(id).isEqualTo(2);
    }

    @Test
    public void testCommitRetryAfterFalseSuccessDoesNotCleanManifest() throws Exception {
        TestFileStore store = createStore(false);
        KeyValue kv = gen.next();
        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        long identifier = 17L;
        store.commitDataImpl(
                Collections.singletonList(kv),
                gen::getPartition,
                value -> 0,
                false,
                identifier,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ManifestCommittable committable = checkNotNull(committableRef.get());
        String commitUser = "retry-false-success";
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(
                        store,
                        commitUser,
                        new FalseSuccessSnapshotCommit(
                                new RenamingSnapshotCommit(
                                        store.snapshotManager(), Lock.empty())))) {
            commit.commit(committable, false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        assertThat(latestSnapshot.commitUser()).isEqualTo(commitUser);
        assertThat(latestSnapshot.commitIdentifier()).isEqualTo(identifier);
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(1);
    }

    @Test
    public void testCommitRetryReusePreviousManifestMergeResultWhenBeforeStillExists()
            throws Exception {
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1"));

        store.commitData(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                kv -> 0);
        store.commitData(
                Collections.singletonList(gen.nextInsert("20211111", 9, 2L, null, "b")),
                gen::getPartition,
                kv -> 0);

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        false,
                        conflictAttempts(Collections.emptyList()));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-reuse-merge", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.firstAttemptBaseManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(3);
    }

    @Test
    public void testCommitRetrySkipsManifestMergeWhenBeforeExistsNonContiguously()
            throws Exception {
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1"));

        store.commitData(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                kv -> 0);
        store.commitData(
                Collections.singletonList(gen.nextInsert("20211111", 9, 2L, null, "b")),
                gen::getPartition,
                kv -> 0);

        AtomicReference<ManifestCommittable> conflictCommittableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211112", 10, 4L, null, "d")),
                gen::getPartition,
                value -> 0,
                false,
                22L,
                null,
                Collections.emptyList(),
                (commit, committable) -> conflictCommittableRef.set(committable));
        List<ManifestEntry> conflictDeltaFiles =
                tableFilesFrom(checkNotNull(conflictCommittableRef.get()), store.options());

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        false,
                        conflictAttempts(conflictDeltaFiles));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-skip-non-contiguous", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.conflictBaseManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(4);
    }

    @Test
    public void testCommitRetrySkipsManifestMergeWhenPreviousMergeCannotBeReused()
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1");
        TestFileStore store = createStore(false, options);

        store.commitData(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                kv -> 0);
        store.commitData(
                Collections.singletonList(gen.nextInsert("20211111", 9, 2L, null, "b")),
                gen::getPartition,
                kv -> 0);

        Snapshot latestBeforeRetry = checkNotNull(store.snapshotManager().latestSnapshot());
        assertThat(store.manifestListFactory().create().readDataManifests(latestBeforeRetry))
                .hasSize(2);

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        true,
                        conflictAttempts(Collections.emptyList()));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-reuse-merge", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(snapshotCommit.conflictBaseManifests()).hasSize(2);
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.conflictBaseManifests());
        assertThat(finalBaseManifests).isNotEqualTo(snapshotCommit.firstAttemptBaseManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(3);
    }

    @Test
    public void testCommitRetrySkipsManifestMergeAcrossMultipleRetries() throws Exception {
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1"));

        store.commitData(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                kv -> 0);
        store.commitData(
                Collections.singletonList(gen.nextInsert("20211111", 9, 2L, null, "b")),
                gen::getPartition,
                kv -> 0);

        AtomicReference<ManifestCommittable> firstConflictCommittableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211112", 10, 4L, null, "d")),
                gen::getPartition,
                value -> 0,
                false,
                22L,
                null,
                Collections.emptyList(),
                (commit, committable) -> firstConflictCommittableRef.set(committable));
        List<ManifestEntry> firstConflictDeltaFiles =
                tableFilesFrom(checkNotNull(firstConflictCommittableRef.get()), store.options());

        AtomicReference<ManifestCommittable> secondConflictCommittableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211113", 11, 5L, null, "e")),
                gen::getPartition,
                value -> 0,
                false,
                24L,
                null,
                Collections.emptyList(),
                (commit, committable) -> secondConflictCommittableRef.set(committable));
        List<ManifestEntry> secondConflictDeltaFiles =
                tableFilesFrom(checkNotNull(secondConflictCommittableRef.get()), store.options());

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        false,
                        conflictAttempts(firstConflictDeltaFiles, secondConflictDeltaFiles));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-reuse-multiple", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.conflictBaseManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(5);
    }

    @Test
    public void testCommitRetryFromEmptyTableWithConcurrentFirstSnapshot() throws Exception {
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1"));

        AtomicReference<ManifestCommittable> conflictCommittableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211112", 10, 4L, null, "d")),
                gen::getPartition,
                value -> 0,
                false,
                22L,
                null,
                Collections.emptyList(),
                (commit, committable) -> conflictCommittableRef.set(committable));
        List<ManifestEntry> conflictDeltaFiles =
                tableFilesFrom(checkNotNull(conflictCommittableRef.get()), store.options());

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        false,
                        conflictAttempts(conflictDeltaFiles));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-reuse-empty-table", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.conflictDeltaManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id())).hasSize(2);
    }

    @Test
    public void testCommitRetrySkipsManifestMergePreservesDeleteOrder() throws Exception {
        TestFileStore store =
                createStore(
                        false,
                        Collections.singletonMap(
                                CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE.key(), "1"));

        store.commitData(
                Collections.singletonList(gen.nextInsert("20211110", 8, 1L, null, "a")),
                gen::getPartition,
                kv -> 0);
        store.commitData(
                Collections.singletonList(gen.nextInsert("20211111", 9, 2L, null, "b")),
                gen::getPartition,
                kv -> 0);

        Snapshot latestBeforeRetry = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> manifestsBeforeRetry =
                store.manifestListFactory().create().readDataManifests(latestBeforeRetry);
        assertThat(manifestsBeforeRetry).hasSize(2);
        ManifestEntry firstEntry =
                store.manifestFileFactory()
                        .create()
                        .read(manifestsBeforeRetry.get(0).fileName())
                        .get(0);
        ManifestEntry deleteFirstEntry =
                ManifestEntry.create(
                        FileKind.DELETE,
                        firstEntry.partition(),
                        firstEntry.bucket(),
                        firstEntry.totalBuckets(),
                        firstEntry.file());

        AtomicReference<ManifestCommittable> committableRef = new AtomicReference<>();
        store.commitDataImpl(
                Collections.singletonList(gen.nextInsert("20211110", 9, 3L, null, "c")),
                gen::getPartition,
                value -> 0,
                false,
                23L,
                null,
                Collections.emptyList(),
                (commit, committable) -> committableRef.set(committable));

        ConflictingSnapshotCommit snapshotCommit =
                new ConflictingSnapshotCommit(
                        new RenamingSnapshotCommit(store.snapshotManager(), Lock.empty()),
                        store.snapshotManager(),
                        store.manifestListFactory().create(),
                        store.manifestFileFactory().create(),
                        false,
                        conflictAttempts(Collections.singletonList(deleteFirstEntry)));
        try (FileStoreCommitImpl commit =
                newCommitWithSnapshotCommit(store, "retry-reuse-delete-order", snapshotCommit)) {
            commit.commit(checkNotNull(committableRef.get()), false);
        }

        Snapshot latestSnapshot = checkNotNull(store.snapshotManager().latestSnapshot());
        List<ManifestFileMeta> finalBaseManifests =
                store.manifestListFactory()
                        .create()
                        .read(
                                latestSnapshot.baseManifestList(),
                                latestSnapshot.baseManifestListSize());
        assertThat(finalBaseManifests)
                .containsExactlyElementsOf(snapshotCommit.conflictBaseManifests());
        assertThat(store.readKvsFromSnapshot(latestSnapshot.id()))
                .extracting(kv -> kv.value().getString(6).toString())
                .containsExactlyInAnyOrder("b", "c");
    }

    private FileStoreCommitImpl newCommitWithSnapshotCommit(
            TestFileStore store, String commitUser, SnapshotCommit snapshotCommit) {
        return newCommitWithSnapshotCommit(
                store,
                commitUser,
                snapshotCommit,
                store.options(),
                store.options().dataEvolutionEnabled());
    }

    private ManifestEntry addFile(
            BinaryRow partition, int bucket, int totalBuckets, long maxSequenceNumber) {
        return ManifestEntry.create(
                FileKind.ADD,
                partition,
                bucket,
                totalBuckets,
                DataFileMeta.forAppend(
                        String.format("test-%d.orc", maxSequenceNumber),
                        1,
                        1,
                        EMPTY_STATS,
                        maxSequenceNumber,
                        maxSequenceNumber,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null));
    }

    private FileStoreCommitImpl newCommitWithSnapshotCommit(
            TestFileStore store,
            String commitUser,
            SnapshotCommit snapshotCommit,
            CoreOptions options,
            boolean dataEvolutionEnabled) {
        String tableName = store.options().path().getName();
        return new FileStoreCommitImpl(
                snapshotCommit,
                store.fileIO(),
                new SchemaManager(store.fileIO(), store.options().path()),
                tableName,
                commitUser,
                store.partitionType(),
                options,
                store.pathFactory(),
                store.snapshotManager(),
                store.manifestFileFactory(),
                store.manifestListFactory(),
                store.indexManifestFileFactory(),
                store::newScan,
                store.newStatsFileHandler(),
                store.bucketMode(),
                Collections.emptyList(),
                Collections.emptyList(),
                scanner ->
                        new ConflictDetection(
                                tableName,
                                commitUser,
                                store.partitionType(),
                                store.pathFactory(),
                                store.newKeyComparator(),
                                store.bucketMode(),
                                options.deletionVectorsEnabled(),
                                dataEvolutionEnabled,
                                options.pkClusteringOverride(),
                                store.newIndexFileHandler(),
                                store.snapshotManager(),
                                scanner),
                null);
    }

    private ManifestCommittable indexCommittable(
            BinaryRow partition, String fileName, long rowRangeStart, long rowRangeEnd) {
        ManifestCommittable committable = new ManifestCommittable(0);
        committable.addFileCommittable(
                new CommitMessageImpl(
                        partition,
                        0,
                        null,
                        DataIncrement.indexIncrement(
                                Collections.singletonList(
                                        new IndexFileMeta(
                                                "btree",
                                                fileName,
                                                1,
                                                1,
                                                new GlobalIndexMeta(
                                                        rowRangeStart, rowRangeEnd, 0, null, null),
                                                null))),
                        CompactIncrement.emptyIncrement()));
        return committable;
    }

    private static List<ManifestEntry> tableFilesFrom(
            ManifestCommittable committable, CoreOptions options) {
        ManifestEntryChanges changes = new ManifestEntryChanges(options.bucket());
        committable.fileCommittables().forEach(changes::collect);
        return new ArrayList<>(changes.appendTableFiles);
    }

    @SafeVarargs
    private static List<List<ManifestEntry>> conflictAttempts(List<ManifestEntry>... attempts) {
        return Arrays.asList(attempts);
    }

    private static class ConflictingSnapshotCommit implements SnapshotCommit {

        private final SnapshotCommit delegate;
        private final SnapshotManager snapshotManager;
        private final ManifestList manifestList;
        private final ManifestFile manifestFile;
        private final boolean mergeConflictManifests;
        private final List<List<ManifestEntry>> conflictDeltaFilesByAttempt;
        private int commitAttempt = 0;
        private final List<List<ManifestFileMeta>> attemptBaseManifests;
        private final List<List<ManifestFileMeta>> conflictDeltaManifestsByAttempt;
        private List<ManifestFileMeta> conflictBaseManifests;

        private ConflictingSnapshotCommit(
                SnapshotCommit delegate,
                SnapshotManager snapshotManager,
                ManifestList manifestList,
                ManifestFile manifestFile,
                boolean mergeConflictManifests,
                List<List<ManifestEntry>> conflictDeltaFilesByAttempt) {
            this.delegate = delegate;
            this.snapshotManager = snapshotManager;
            this.manifestList = manifestList;
            this.manifestFile = manifestFile;
            this.mergeConflictManifests = mergeConflictManifests;
            this.conflictDeltaFilesByAttempt = conflictDeltaFilesByAttempt;
            this.attemptBaseManifests = new ArrayList<>();
            this.conflictDeltaManifestsByAttempt = new ArrayList<>();
        }

        @Override
        public boolean commit(
                Snapshot snapshot,
                String branch,
                List<org.apache.paimon.partition.PartitionStatistics> statistics)
                throws Exception {
            if (commitAttempt >= conflictDeltaFilesByAttempt.size()) {
                return delegate.commit(snapshot, branch, statistics);
            }

            List<ManifestEntry> conflictDeltaFiles = conflictDeltaFilesByAttempt.get(commitAttempt);
            commitAttempt++;
            attemptBaseManifests.add(
                    manifestList.read(
                            snapshot.baseManifestList(), snapshot.baseManifestListSize()));

            Snapshot previousSnapshot =
                    snapshot.id() == Snapshot.FIRST_SNAPSHOT_ID
                            ? null
                            : snapshotManager.snapshot(snapshot.id() - 1);
            List<ManifestFileMeta> previousManifests =
                    previousSnapshot == null
                            ? Collections.emptyList()
                            : manifestList.readDataManifests(previousSnapshot);
            conflictBaseManifests =
                    mergeConflictManifests
                            ? rewriteManifests(previousManifests)
                            : previousManifests;
            List<ManifestFileMeta> conflictNewManifests =
                    conflictDeltaFiles.isEmpty()
                            ? Collections.emptyList()
                            : manifestFile.write(conflictDeltaFiles);
            conflictDeltaManifestsByAttempt.add(conflictNewManifests);
            boolean putConflictNewManifestsInBase =
                    !mergeConflictManifests
                            && !conflictNewManifests.isEmpty()
                            && !previousManifests.isEmpty();
            if (putConflictNewManifestsInBase) {
                conflictBaseManifests = new ArrayList<>();
                conflictBaseManifests.add(previousManifests.get(0));
                conflictBaseManifests.addAll(conflictNewManifests);
                conflictBaseManifests.addAll(
                        previousManifests.subList(1, previousManifests.size()));
            }
            Pair<String, Long> conflictBaseManifestList = manifestList.write(conflictBaseManifests);
            long conflictDeltaRecordCount =
                    conflictDeltaFiles.stream()
                            .mapToLong(
                                    entry ->
                                            entry.kind() == FileKind.ADD
                                                    ? entry.file().rowCount()
                                                    : -entry.file().rowCount())
                            .sum();
            Pair<String, Long> conflictDeltaManifestList =
                    manifestList.write(
                            putConflictNewManifestsInBase
                                    ? Collections.emptyList()
                                    : conflictNewManifests);
            Snapshot conflictSnapshot =
                    new Snapshot(
                            snapshot.id(),
                            previousSnapshot == null
                                    ? snapshot.schemaId()
                                    : previousSnapshot.schemaId(),
                            conflictBaseManifestList.getLeft(),
                            conflictBaseManifestList.getRight(),
                            conflictDeltaManifestList.getLeft(),
                            conflictDeltaManifestList.getRight(),
                            null,
                            null,
                            previousSnapshot == null ? null : previousSnapshot.indexManifest(),
                            "conflict-user",
                            Long.MAX_VALUE,
                            Snapshot.CommitKind.ANALYZE,
                            System.currentTimeMillis(),
                            (previousSnapshot == null ? 0L : previousSnapshot.totalRecordCount())
                                    + conflictDeltaRecordCount,
                            conflictDeltaRecordCount,
                            null,
                            previousSnapshot == null ? null : previousSnapshot.watermark(),
                            previousSnapshot == null ? null : previousSnapshot.statistics(),
                            previousSnapshot == null ? null : previousSnapshot.properties(),
                            previousSnapshot == null ? null : previousSnapshot.nextRowId());
            assertThat(delegate.commit(conflictSnapshot, branch, Collections.emptyList())).isTrue();
            return false;
        }

        private List<ManifestFileMeta> rewriteManifests(List<ManifestFileMeta> manifests) {
            if (manifests.isEmpty()) {
                return Collections.emptyList();
            }

            List<ManifestFileMeta> rewrittenManifests = new ArrayList<>();
            for (ManifestFileMeta manifest : manifests) {
                rewrittenManifests.addAll(
                        manifestFile.write(manifestFile.read(manifest.fileName())));
            }
            return rewrittenManifests;
        }

        private List<ManifestFileMeta> firstAttemptBaseManifests() {
            return attemptBaseManifests(0);
        }

        private List<ManifestFileMeta> attemptBaseManifests(int attempt) {
            assertThat(attemptBaseManifests).hasSizeGreaterThan(attempt);
            return attemptBaseManifests.get(attempt);
        }

        private List<ManifestFileMeta> conflictBaseManifests() {
            return checkNotNull(conflictBaseManifests);
        }

        private List<ManifestFileMeta> conflictDeltaManifests() {
            assertThat(conflictDeltaManifestsByAttempt).isNotEmpty();
            return conflictDeltaManifestsByAttempt.get(conflictDeltaManifestsByAttempt.size() - 1);
        }

        private List<ManifestFileMeta> conflictDeltaManifests(int attempt) {
            assertThat(conflictDeltaManifestsByAttempt).hasSizeGreaterThan(attempt);
            return conflictDeltaManifestsByAttempt.get(attempt);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    private static class FalseSuccessSnapshotCommit implements SnapshotCommit {

        private final SnapshotCommit delegate;
        private boolean firstCommit = true;

        private FalseSuccessSnapshotCommit(SnapshotCommit delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean commit(
                Snapshot snapshot,
                String branch,
                List<org.apache.paimon.partition.PartitionStatistics> statistics)
                throws Exception {
            boolean committed = delegate.commit(snapshot, branch, statistics);
            if (firstCommit) {
                firstCommit = false;
                assertThat(committed).isTrue();
                return false;
            }
            return committed;
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    private TestFileStore createStore(boolean failing, Map<String, String> options)
            throws Exception {
        return createStore(failing, 1, CoreOptions.ChangelogProducer.NONE, options);
    }

    private TestFileStore createRowTrackingDataEvolutionStore() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return createStore(false, -1, CoreOptions.ChangelogProducer.NONE, options);
    }

    private TestFileStore createStore(boolean failing) throws Exception {
        return createStore(failing, 1);
    }

    private TestFileStore createStore(boolean failing, int numBucket) throws Exception {
        return createStore(
                failing, numBucket, CoreOptions.ChangelogProducer.NONE, Collections.emptyMap());
    }

    private TestFileStore createStore(
            boolean failing, int numBucket, CoreOptions.ChangelogProducer changelogProducer)
            throws Exception {
        return createStore(failing, numBucket, changelogProducer, Collections.emptyMap());
    }

    private TestFileStore createStore(
            boolean failing,
            int numBucket,
            CoreOptions.ChangelogProducer changelogProducer,
            Map<String, String> options)
            throws Exception {
        String root =
                failing
                        ? FailingFileIO.getFailingPath(failingName, tempDir.toString())
                        : TraceableFileIO.SCHEME + "://" + tempDir.toString();
        Path path = new Path(tempDir.toUri());
        List<String> primaryKeys =
                Boolean.parseBoolean(options.get(CoreOptions.ROW_TRACKING_ENABLED.key()))
                        ? Collections.emptyList()
                        : TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED);
        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(new LocalFileIO(), path),
                        new Schema(
                                TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                                TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                                primaryKeys,
                                options,
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
