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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.SnapshotFinder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreExpireImpl}. */
public class FileStoreExpireTest {

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private TestFileStore store;
    private FileStorePathFactory pathFactory;

    @BeforeEach
    public void beforeEach() throws IOException {
        gen = new TestKeyValueGenerator();
        store =
                TestFileStore.create(
                        "avro",
                        tempDir.toString(),
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        new DeduplicateMergeFunction());
        pathFactory = store.pathFactory();
    }

    @AfterEach
    public void afterEach() throws IOException {
        store.assertCleaned();
    }

    @Test
    public void testNoSnapshot() {
        FileStoreExpire expire = store.newExpire(1, 3, Long.MAX_VALUE);
        expire.expire();

        assertThat(pathFactory.latestSnapshotId()).isNull();
    }

    @Test
    public void testNotEnoughSnapshots() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(2, allData, snapshotPositions);
        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        FileStoreExpire expire = store.newExpire(1, latestSnapshotId + 1, Long.MAX_VALUE);
        expire.expire();

        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testNeverExpire() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        FileStoreExpire expire = store.newExpire(1, Integer.MAX_VALUE, Long.MAX_VALUE);
        expire.expire();

        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testNumRetainedMin() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numRetainedMin = random.nextInt(5) + 1;
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(numRetainedMin + random.nextInt(5), allData, snapshotPositions);
        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        Thread.sleep(100);
        FileStoreExpire expire = store.newExpire(numRetainedMin, Integer.MAX_VALUE, 1);
        expire.expire();

        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i <= latestSnapshotId - numRetainedMin; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isFalse();
        }
        for (int i = latestSnapshotId - numRetainedMin + 1; i <= latestSnapshotId; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testExpireWithNumber() throws Exception {
        FileStoreExpire expire = store.newExpire(1, 3, Long.MAX_VALUE);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            commit(ThreadLocalRandom.current().nextInt(5) + 1, allData, snapshotPositions);
            expire.expire();

            int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
            FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
            for (int j = 1; j <= latestSnapshotId; j++) {
                if (j > latestSnapshotId - 3) {
                    assertThat(fs.exists(pathFactory.toSnapshotPath(j))).isTrue();
                    assertSnapshot(j, allData, snapshotPositions);
                } else {
                    assertThat(fs.exists(pathFactory.toSnapshotPath(j))).isFalse();
                }
            }
        }

        // validate earliest hint file

        Path snapshotDir = pathFactory.snapshotDirectory();
        Path earliest = new Path(snapshotDir, SnapshotFinder.EARLIEST);

        assertThat(earliest.getFileSystem().exists(earliest)).isTrue();

        Long earliestId = SnapshotFinder.findEarliest(snapshotDir);

        // remove earliest hint file
        earliest.getFileSystem().delete(earliest, false);

        assertThat(SnapshotFinder.findEarliest(snapshotDir)).isEqualTo(earliestId);
    }

    @Test
    public void testExpireWithTime() throws Exception {
        FileStoreExpire expire = store.newExpire(1, Integer.MAX_VALUE, 1000);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        Thread.sleep(1500);
        commit(5, allData, snapshotPositions);
        long expireMillis = System.currentTimeMillis();
        // expire twice to check for idempotence
        expire.expire();
        expire.expire();

        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i <= latestSnapshotId; i++) {
            Path snapshotPath = pathFactory.toSnapshotPath(i);
            if (fs.exists(snapshotPath)) {
                assertThat(Snapshot.fromPath(snapshotPath).timeMillis())
                        .isBetween(expireMillis - 1000, expireMillis);
                assertSnapshot(i, allData, snapshotPositions);
            }
        }
    }

    private void commit(int numCommits, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        for (int i = 0; i < numCommits; i++) {
            int numRecords = ThreadLocalRandom.current().nextInt(100) + 1;
            List<KeyValue> data = new ArrayList<>();
            for (int j = 0; j < numRecords; j++) {
                data.add(gen.next());
            }
            allData.addAll(data);
            List<Snapshot> snapshots = store.commitData(data, gen::getPartition, kv -> 0);
            for (int j = 0; j < snapshots.size(); j++) {
                snapshotPositions.add(allData.size());
            }
        }
    }

    private void assertSnapshot(
            int snapshotId, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        Map<BinaryRowData, BinaryRowData> expected =
                store.toKvMap(allData.subList(0, snapshotPositions.get(snapshotId - 1)));
        List<KeyValue> actualKvs =
                store.readKvsFromManifestEntries(
                        store.newScan().withSnapshot(snapshotId).plan().files());
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = store.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
    }
}
