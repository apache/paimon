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
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.TestFileStore;
import org.apache.flink.table.store.file.TestKeyValueGenerator;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.manifest.FileKind;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.file.utils.FileUtils;
import org.apache.flink.table.store.file.utils.SnapshotManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.data.binary.BinaryRowDataUtil.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileStoreExpireImpl}. */
public class FileStoreExpireTest {

    private TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    private TestFileStore store;
    private SnapshotManager snapshotManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        gen = new TestKeyValueGenerator();
        store = createStore();
        snapshotManager = store.snapshotManager();
        SchemaManager schemaManager = new SchemaManager(new Path(tempDir.toUri()));
        schemaManager.commitNewVersion(
                new UpdateSchema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.emptyMap(),
                        null));
    }

    private TestFileStore createStore() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        return new TestFileStore.Builder(
                        "avro",
                        tempDir.toString(),
                        1,
                        TestKeyValueGenerator.DEFAULT_PART_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE,
                        TestKeyValueGenerator.TestKeyValueFieldsExtractor.EXTRACTOR,
                        DeduplicateMergeFunction.factory())
                .changelogProducer(changelogProducer)
                .build();
    }

    @AfterEach
    public void afterEach() throws IOException {
        store.assertCleaned();
    }

    @Test
    public void testExpireExtraFiles() throws IOException {
        FileStoreExpireImpl expire = store.newExpire(1, 3, Long.MAX_VALUE);

        // write test files
        BinaryRowData partition = gen.getPartition(gen.next());
        Path bucketPath = store.pathFactory().bucketPath(partition, 0);
        Path myDataFile = new Path(bucketPath, "myDataFile");
        FileUtils.writeFileUtf8(myDataFile, "1");
        Path extra1 = new Path(bucketPath, "extra1");
        FileUtils.writeFileUtf8(extra1, "2");
        Path extra2 = new Path(bucketPath, "extra2");
        FileUtils.writeFileUtf8(extra2, "3");

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
                        extraFiles);
        ManifestEntry add = new ManifestEntry(FileKind.ADD, partition, 0, 1, dataFile);
        ManifestEntry delete = new ManifestEntry(FileKind.DELETE, partition, 0, 1, dataFile);

        // expire
        expire.expireMergeTreeFiles(Arrays.asList(add, delete));

        // check
        FileSystem fs = myDataFile.getFileSystem();
        assertThat(fs.exists(myDataFile)).isFalse();
        assertThat(fs.exists(extra1)).isFalse();
        assertThat(fs.exists(extra2)).isFalse();
    }

    @Test
    public void testNoSnapshot() {
        FileStoreExpire expire = store.newExpire(1, 3, Long.MAX_VALUE);
        expire.expire();

        assertThat(snapshotManager.latestSnapshotId()).isNull();
    }

    @Test
    public void testNotEnoughSnapshots() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(2, allData, snapshotPositions);
        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        FileStoreExpire expire = store.newExpire(1, latestSnapshotId + 1, Long.MAX_VALUE);
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
        FileStoreExpire expire = store.newExpire(1, Integer.MAX_VALUE, Long.MAX_VALUE);
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
        FileStoreExpire expire = store.newExpire(numRetainedMin, Integer.MAX_VALUE, 1);
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
        FileStoreExpire expire = store.newExpire(1, 3, Long.MAX_VALUE);

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

        assertThat(earliest.getFileSystem().exists(earliest)).isTrue();

        Long earliestId = snapshotManager.earliestSnapshotId();

        // remove earliest hint file
        earliest.getFileSystem().delete(earliest, false);

        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(earliestId);
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

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            if (snapshotManager.snapshotExists(i)) {
                assertThat(snapshotManager.snapshot(i).timeMillis())
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
                        store.newScan().withSnapshot(snapshotId).plan().files(), false);
        gen.sort(actualKvs);
        Map<BinaryRowData, BinaryRowData> actual = store.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
    }
}
