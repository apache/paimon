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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;

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

    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    FileStoreCommitTest.class.getClassLoader(), "avro", new Configuration());

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private FileStorePathFactory pathFactory;

    @BeforeEach
    public void beforeEach() throws IOException {
        gen = new TestKeyValueGenerator();
        pathFactory = OperationTestUtils.createPathFactory(false, tempDir.toString());
        Path root = new Path(tempDir.toString());
        root.getFileSystem().mkdirs(new Path(root + "/snapshot"));
    }

    @Test
    public void testNoSnapshot() {
        FileStoreExpire expire =
                OperationTestUtils.createExpire(3, Long.MAX_VALUE, avro, pathFactory);
        expire.expire();

        assertThat(pathFactory.latestSnapshotId()).isNull();
    }

    @Test
    public void testNotEnoughSnapshots() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(2, allData, snapshotPositions);
        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        FileStoreExpire expire =
                OperationTestUtils.createExpire(
                        latestSnapshotId + 1, Long.MAX_VALUE, avro, pathFactory);
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
        FileStoreExpire expire =
                OperationTestUtils.createExpire(
                        Integer.MAX_VALUE, Long.MAX_VALUE, avro, pathFactory);
        expire.expire();

        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testKeepAtLeastOneSnapshot() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(3, allData, snapshotPositions);
        int latestSnapshotId = pathFactory.latestSnapshotId().intValue();
        Thread.sleep(100);
        FileStoreExpire expire =
                OperationTestUtils.createExpire(Integer.MAX_VALUE, 1, avro, pathFactory);
        expire.expire();

        FileSystem fs = pathFactory.toSnapshotPath(latestSnapshotId).getFileSystem();
        for (int i = 1; i < latestSnapshotId; i++) {
            assertThat(fs.exists(pathFactory.toSnapshotPath(i))).isFalse();
        }
        assertThat(fs.exists(pathFactory.toSnapshotPath(latestSnapshotId))).isTrue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
    }

    @Test
    public void testExpireWithNumber() throws Exception {
        FileStoreExpire expire =
                OperationTestUtils.createExpire(3, Long.MAX_VALUE, avro, pathFactory);

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
    }

    @Test
    public void testExpireWithTime() throws Exception {
        FileStoreExpire expire =
                OperationTestUtils.createExpire(Integer.MAX_VALUE, 1000, avro, pathFactory);

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
            List<Snapshot> snapshots =
                    OperationTestUtils.commitData(
                            data, gen::getPartition, kv -> 0, avro, pathFactory);
            for (int j = 0; j < snapshots.size(); j++) {
                snapshotPositions.add(allData.size());
            }
        }
    }

    private void assertSnapshot(
            int snapshotId, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        Map<BinaryRowData, BinaryRowData> expected =
                OperationTestUtils.toKvMap(
                        allData.subList(0, snapshotPositions.get(snapshotId - 1)));
        List<KeyValue> actualKvs =
                OperationTestUtils.readKvsFromManifestEntries(
                        OperationTestUtils.createScan(avro, pathFactory)
                                .withSnapshot(snapshotId)
                                .plan()
                                .files(),
                        avro,
                        pathFactory);
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = OperationTestUtils.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
    }
}
