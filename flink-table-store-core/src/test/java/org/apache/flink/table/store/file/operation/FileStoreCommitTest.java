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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.manifest.ManifestCommittable;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.FailingAtomicRenameFileSystem;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.file.utils.TestAtomicRenameFileSystem;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        FailingAtomicRenameFileSystem.reset(failingName, 100, 100);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSingleCommitUser(boolean failing) throws Exception {
        testRandomConcurrentNoConflict(1, failing);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testManyCommitUsersNoConflict(boolean failing) throws Exception {
        testRandomConcurrentNoConflict(ThreadLocalRandom.current().nextInt(3) + 2, failing);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testManyCommitUsersWithConflict(boolean failing) throws Exception {
        testRandomConcurrentWithConflict(ThreadLocalRandom.current().nextInt(3) + 2, failing);
    }

    @Test
    public void testLatestHint() throws Exception {
        testRandomConcurrentNoConflict(1, false);
        SnapshotManager snapshotManager = createStore(false, 1).snapshotManager();
        Path snapshotDir = snapshotManager.snapshotDirectory();
        Path latest = new Path(snapshotDir, SnapshotManager.LATEST);

        assertThat(latest.getFileSystem().exists(latest)).isTrue();

        Long latestId = snapshotManager.findLatest();

        // remove latest hint file
        latest.getFileSystem().delete(latest, false);

        assertThat(snapshotManager.findLatest()).isEqualTo(latestId);
    }

    @Test
    public void testFilterCommittedAfterExpire() throws Exception {
        testRandomConcurrentNoConflict(1, false);
        // remove first snapshot to mimic expiration
        TestFileStore store = createStore(false);
        SnapshotManager snapshotManager = store.snapshotManager();
        Path firstSnapshotPath = snapshotManager.snapshotPath(Snapshot.FIRST_SNAPSHOT_ID);
        FileUtils.deleteOrWarn(firstSnapshotPath);
        // this test succeeds if this call does not fail
        store.newCommit(UUID.randomUUID().toString())
                .filterCommitted(Collections.singletonList(new ManifestCommittable("dummy")));
    }

    protected void testRandomConcurrentNoConflict(int numThreads, boolean failing)
            throws Exception {
        // prepare test data
        Map<BinaryRowData, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");

        List<Map<BinaryRowData, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data.entrySet()) {
            dataPerThread
                    .get(ThreadLocalRandom.current().nextInt(numThreads))
                    .put(entry.getKey(), entry.getValue());
        }

        testRandomConcurrent(dataPerThread, true, failing);
    }

    protected void testRandomConcurrentWithConflict(int numThreads, boolean failing)
            throws Exception {
        // prepare test data
        Map<BinaryRowData, List<KeyValue>> data =
                generateData(ThreadLocalRandom.current().nextInt(1000) + 1);
        logData(
                () ->
                        data.values().stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList()),
                "input");

        List<Map<BinaryRowData, List<KeyValue>>> dataPerThread = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            dataPerThread.add(new HashMap<>());
        }
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data.entrySet()) {
            for (KeyValue kv : entry.getValue()) {
                dataPerThread
                        .get(Math.abs(kv.key().hashCode()) % numThreads)
                        .computeIfAbsent(entry.getKey(), p -> new ArrayList<>())
                        .add(kv);
            }
        }

        testRandomConcurrent(dataPerThread, false, failing);
    }

    private void testRandomConcurrent(
            List<Map<BinaryRowData, List<KeyValue>>> dataPerThread,
            boolean enableOverwrite,
            boolean failing)
            throws Exception {
        // concurrent commits
        List<TestCommitThread> threads = new ArrayList<>();
        for (Map<BinaryRowData, List<KeyValue>> data : dataPerThread) {
            TestCommitThread thread =
                    new TestCommitThread(
                            TestKeyValueGenerator.KEY_TYPE,
                            TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                            enableOverwrite,
                            data,
                            createStore(failing),
                            createStore(false));
            thread.start();
            threads.add(thread);
        }

        TestFileStore store = createStore(false);

        // calculate expected results
        List<KeyValue> threadResults = new ArrayList<>();
        for (TestCommitThread thread : threads) {
            thread.join();
            threadResults.addAll(thread.getResult());
        }
        Map<BinaryRowData, BinaryRowData> expected = store.toKvMap(threadResults);

        // read actual data and compare
        Long snapshotId = store.snapshotManager().latestSnapshotId();
        assertThat(snapshotId).isNotNull();
        List<KeyValue> actualKvs = store.readKvsFromSnapshot(snapshotId);
        gen.sort(actualKvs);
        logData(() -> actualKvs, "raw read results");
        Map<BinaryRowData, BinaryRowData> actual = store.toKvMap(actualKvs);
        logData(() -> kvMapToKvList(expected), "expected");
        logData(() -> kvMapToKvList(actual), "actual");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testOverwritePartialCommit() throws Exception {
        Map<BinaryRowData, List<KeyValue>> data1 =
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

        Map<BinaryRowData, List<KeyValue>> data2 =
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
        for (Map.Entry<BinaryRowData, List<KeyValue>> entry : data1.entrySet()) {
            if (dtToOverwrite.equals(entry.getKey().getString(0).toString())) {
                continue;
            }
            expectedKvs.addAll(entry.getValue());
        }
        data2.values().forEach(expectedKvs::addAll);
        gen.sort(expectedKvs);
        Map<BinaryRowData, BinaryRowData> expected = store.toKvMap(expectedKvs);

        List<KeyValue> actualKvs =
                store.readKvsFromSnapshot(store.snapshotManager().latestSnapshotId());
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = store.toKvMap(actualKvs);

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
        assertThat(snapshot.getLogOffsets()).isEqualTo(offsets);

        // commit 2
        offsets = new HashMap<>();
        offsets.put(1, 8L);
        snapshot =
                store.commitData(generateDataList(10), gen::getPartition, kv -> 0, offsets).get(0);
        Map<Integer, Long> expected = new HashMap<>();
        expected.put(0, 1L);
        expected.put(1, 8L);
        assertThat(snapshot.getLogOffsets()).isEqualTo(expected);
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
                (commit, committable) -> commit.commit(committable, Collections.emptyMap()));
        assertThat(store.snapshotManager().findLatest()).isEqualTo(snapshot.id());

        // commit empty new files
        store.commitDataImpl(
                Collections.emptyList(),
                gen::getPartition,
                kv -> 0,
                false,
                null,
                (commit, committable) -> {
                    commit.withCreateEmptyCommit(true);
                    commit.commit(committable, Collections.emptyMap());
                });
        assertThat(store.snapshotManager().findLatest()).isEqualTo(snapshot.id() + 1);
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
                    String.valueOf(i),
                    (commit, committable) -> {
                        commit.commit(committable, Collections.emptyMap());
                        committables.add(committable);
                    });
        }

        // commit the first snapshot again, should throw exception due to conflicts
        for (int i = 0; i < 3; i++) {
            RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    store.newCommit()
                                            .commit(committables.get(0), Collections.emptyMap()),
                            "Expecting RuntimeException, but nothing is thrown.");
            assertThat(e).hasMessageContaining("Give up committing.");
        }
    }

    private TestFileStore createStore(boolean failing) throws Exception {
        return createStore(failing, 1);
    }

    private TestFileStore createStore(boolean failing, int numBucket) throws Exception {
        String root =
                failing
                        ? FailingAtomicRenameFileSystem.getFailingPath(
                                failingName, tempDir.toString())
                        : TestAtomicRenameFileSystem.SCHEME + "://" + tempDir.toString();
        SchemaManager schemaManager = new SchemaManager(new Path(tempDir.toUri()));
        schemaManager.commitNewVersion(
                new UpdateSchema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.emptyMap(),
                        null));
        return TestFileStore.create(
                "avro",
                root,
                numBucket,
                TestKeyValueGenerator.DEFAULT_PART_TYPE,
                TestKeyValueGenerator.KEY_TYPE,
                TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                new DeduplicateMergeFunction());
    }

    private List<KeyValue> generateDataList(int numRecords) {
        return generateData(numRecords).values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private Map<BinaryRowData, List<KeyValue>> generateData(int numRecords) {
        Map<BinaryRowData, List<KeyValue>> data = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            KeyValue kv = gen.next();
            data.computeIfAbsent(gen.getPartition(kv), p -> new ArrayList<>()).add(kv);
        }
        return data;
    }

    private List<KeyValue> kvMapToKvList(Map<BinaryRowData, BinaryRowData> map) {
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
