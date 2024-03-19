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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExpireSnapshotsImpl}. After expiration, only useful files should be retained.
 */
public class CleanedFileStoreExpireTest extends FileStoreExpireTestBase {

    @AfterEach
    public void afterEach() throws IOException {
        store.assertCleaned();
    }

    @Test
    public void testExpireExtraFiles() throws IOException {
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) store.newExpire(1, 3, Long.MAX_VALUE);

        // write test files
        BinaryRow partition = gen.getPartition(gen.next());
        Path bucketPath = store.pathFactory().bucketPath(partition, 0);
        Path myDataFile = new Path(bucketPath, "myDataFile");
        new LocalFileIO().writeFileUtf8(myDataFile, "1");
        Path extra1 = new Path(bucketPath, "extra1");
        fileIO.writeFileUtf8(extra1, "2");
        Path extra2 = new Path(bucketPath, "extra2");
        fileIO.writeFileUtf8(extra2, "3");

        // create DataFileMeta and ManifestEntry
        List<String> extraFiles = Arrays.asList("extra1", "extra2");
        DataFileMeta dataFile =
                new DataFileMeta(
                        "myDataFile",
                        1,
                        1,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        null,
                        null,
                        0,
                        1,
                        0,
                        0,
                        extraFiles,
                        Timestamp.now(),
                        0L);
        ManifestEntry add = new ManifestEntry(FileKind.ADD, partition, 0, 1, dataFile);
        ManifestEntry delete = new ManifestEntry(FileKind.DELETE, partition, 0, 1, dataFile);

        // expire
        expire.snapshotDeletion().cleanUnusedDataFile(Arrays.asList(add, delete));

        // check
        assertThat(fileIO.exists(myDataFile)).isFalse();
        assertThat(fileIO.exists(extra1)).isFalse();
        assertThat(fileIO.exists(extra2)).isFalse();
    }

    @Test
    public void testNoSnapshot() {
        ExpireSnapshots expire = store.newExpire(1, 3, Long.MAX_VALUE);
        expire.expire();

        assertThat(snapshotManager.latestSnapshotId()).isNull();
    }

    @Test
    public void testNotEnoughSnapshots() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(2, allData, snapshotPositions);
        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        ExpireSnapshots expire = store.newExpire(1, latestSnapshotId + 1, Long.MAX_VALUE);
        expire.expire();

        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testNeverExpire() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        ExpireSnapshots expire = store.newExpire(1, Integer.MAX_VALUE, Long.MAX_VALUE);
        expire.expire();

        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
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
        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        Thread.sleep(100);
        ExpireSnapshots expire = store.newExpire(numRetainedMin, Integer.MAX_VALUE, 1);
        expire.expire();

        for (int i = 1; i <= latestSnapshotId - numRetainedMin; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        for (int i = latestSnapshotId - numRetainedMin + 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }
    }

    @Test
    public void testExpireWithNumber() throws Exception {
        ExpireSnapshots expire = store.newExpire(1, 3, Long.MAX_VALUE);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            commit(ThreadLocalRandom.current().nextInt(5) + 1, allData, snapshotPositions);
            expire.expire();

            int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
            for (int j = 1; j <= latestSnapshotId; j++) {
                if (j > latestSnapshotId - 3) {
                    assertThat(snapshotManager.snapshotExists(j)).isTrue();
                    assertSnapshot(j, allData, snapshotPositions);
                } else {
                    assertThat(snapshotManager.snapshotExists(j)).isFalse();
                }
            }
        }

        // validate earliest hint file

        Path snapshotDir = snapshotManager.snapshotDirectory();
        Path earliest = new Path(snapshotDir, SnapshotManager.EARLIEST);

        assertThat(fileIO.exists(earliest)).isTrue();

        Long earliestId = snapshotManager.earliestSnapshotId();

        // remove earliest hint file
        fileIO.delete(earliest, false);

        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(earliestId);
    }

    @Test
    public void testExpireWithTime() throws Exception {
        ExpireSnapshots expire = store.newExpire(1, Integer.MAX_VALUE, 1000);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        Thread.sleep(1500);
        commit(5, allData, snapshotPositions);
        long expireMillis = System.currentTimeMillis();
        // expire twice to check for idempotence
        expire.olderThanMills(expireMillis - 1000).expire();
        expire.olderThanMills(expireMillis - 1000).expire();

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            if (snapshotManager.snapshotExists(i)) {
                assertThat(snapshotManager.snapshot(i).timeMillis())
                        .isBetween(expireMillis - 1000, expireMillis);
                assertSnapshot(i, allData, snapshotPositions);
            }
        }
    }

    @Test
    public void testExpireWithUpgradedFile() throws Exception {
        // write & commit data
        List<KeyValue> data = FileStoreTestUtils.partitionedData(5, gen, "0401", 8);
        BinaryRow partition = gen.getPartition(data.get(0));
        RecordWriter<KeyValue> writer = FileStoreTestUtils.writeData(store, data, partition, 0);
        Map<BinaryRow, Map<Integer, RecordWriter<KeyValue>>> writers =
                Collections.singletonMap(partition, Collections.singletonMap(0, writer));
        FileStoreTestUtils.commitData(store, 0, writers);

        // check
        List<ManifestEntry> entries = store.newScan().plan().files();
        assertThat(entries.size()).isEqualTo(1);
        ManifestEntry entry = entries.get(0);
        assertThat(entry.file().level()).isEqualTo(0);
        Path dataFilePath1 =
                new Path(store.pathFactory().bucketPath(partition, 0), entry.file().fileName());
        FileStoreTestUtils.assertPathExists(fileIO, dataFilePath1);

        // compact & commit
        writer.compact(true);
        writer.sync();
        FileStoreTestUtils.commitData(store, 1, writers);

        // check
        entries = store.newScan().plan().files(FileKind.ADD);
        assertThat(entries.size()).isEqualTo(1);
        entry = entries.get(0);
        // data file has been upgraded due to compact
        assertThat(entry.file().level()).isEqualTo(5);
        Path dataFilePath2 =
                new Path(store.pathFactory().bucketPath(partition, 0), entry.file().fileName());
        assertThat(dataFilePath1).isEqualTo(dataFilePath2);
        FileStoreTestUtils.assertPathExists(fileIO, dataFilePath2);

        // the data file still exists after expire
        ExpireSnapshots expire = store.newExpire(1, 1, Long.MAX_VALUE);
        expire.expire();
        FileStoreTestUtils.assertPathExists(fileIO, dataFilePath2);
    }
}
