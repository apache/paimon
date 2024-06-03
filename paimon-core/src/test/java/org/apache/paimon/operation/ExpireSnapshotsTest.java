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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link ExpireSnapshotsImpl}. */
public class ExpireSnapshotsTest {

    protected final FileIO fileIO = new LocalFileIO();
    protected TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    protected TestFileStore store;
    protected SnapshotManager snapshotManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        gen = new TestKeyValueGenerator();
        store = createStore();
        snapshotManager = store.snapshotManager();
        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        Collections.emptyMap(),
                        null));
    }

    @Test
    public void testExpireWithMissingFiles() throws Exception {
        ExpireSnapshots expire = store.newExpire(1, 1, 1);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);

        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        Set<Path> filesInUse = store.getFilesInUse(latestSnapshotId);
        List<Path> unusedFileList =
                Files.walk(Paths.get(tempDir.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("snapshot"))
                        .filter(p -> !p.getFileName().toString().startsWith("schema"))
                        .map(p -> new Path(p.toString()))
                        .filter(p -> !filesInUse.contains(p))
                        .collect(Collectors.toList());

        // shuffle list
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = unusedFileList.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            Collections.swap(unusedFileList, i, j);
        }

        // delete some unused files
        int numFilesToDelete = random.nextInt(unusedFileList.size());
        for (int i = 0; i < numFilesToDelete; i++) {
            fileIO.deleteQuietly(unusedFileList.get(i));
        }

        expire.expire();

        for (int i = 1; i < latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        assertThat(snapshotManager.snapshotExists(latestSnapshotId)).isTrue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
    }

    @Test
    public void testMixedSnapshotAndTagDeletion() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        commit(random.nextInt(10) + 30, allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        TagManager tagManager = store.newTagManager();

        // create tags for each snapshot
        for (int id = 1; id <= latestSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            tagManager.createTag(
                    snapshot,
                    "tag" + id,
                    store.options().tagDefaultTimeRetained(),
                    Collections.emptyList());
        }

        // randomly expire snapshots
        int expired = random.nextInt(latestSnapshotId / 2) + 1;
        int retained = latestSnapshotId - expired;
        store.newExpire(retained, retained, Long.MAX_VALUE).expire();

        // randomly delete tags
        for (int id = 1; id <= latestSnapshotId; id++) {
            if (random.nextBoolean()) {
                tagManager.deleteTag(
                        "tag" + id,
                        store.newTagDeletion(),
                        snapshotManager,
                        Collections.emptyList());
            }
        }

        // check snapshots and tags
        Set<Snapshot> allSnapshots = new HashSet<>();
        snapshotManager.snapshots().forEachRemaining(allSnapshots::add);
        allSnapshots.addAll(tagManager.taggedSnapshots());

        for (Snapshot snapshot : allSnapshots) {
            assertSnapshot(snapshot, allData, snapshotPositions);
        }
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
                        0L,
                        null,
                        FileSource.APPEND);
        ManifestEntry add = new ManifestEntry(FileKind.ADD, partition, 0, 1, dataFile);
        ManifestEntry delete = new ManifestEntry(FileKind.DELETE, partition, 0, 1, dataFile);

        // expire
        expire.snapshotDeletion().cleanUnusedDataFile(Arrays.asList(add, delete));

        // check
        assertThat(fileIO.exists(myDataFile)).isFalse();
        assertThat(fileIO.exists(extra1)).isFalse();
        assertThat(fileIO.exists(extra2)).isFalse();

        store.assertCleaned();
    }

    @Test
    public void testNoSnapshot() throws IOException {
        ExpireSnapshots expire = store.newExpire(1, 3, Long.MAX_VALUE);
        expire.expire();

        assertThat(snapshotManager.latestSnapshotId()).isNull();

        store.assertCleaned();
    }

    @Test
    public void testNotEnoughSnapshots() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(2, allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        ExpireSnapshots expire = store.newExpire(1, latestSnapshotId + 1, Long.MAX_VALUE);
        expire.expire();

        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }

        store.assertCleaned();
    }

    @Test
    public void testNeverExpire() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        ExpireSnapshots expire = store.newExpire(1, Integer.MAX_VALUE, Long.MAX_VALUE);
        expire.expire();

        for (int i = 1; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }

        store.assertCleaned();
    }

    @Test
    public void testNumRetainedMin() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numRetainedMin = random.nextInt(5) + 1;
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(numRetainedMin + random.nextInt(5), allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
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

        store.assertCleaned();
    }

    @Test
    public void testExpireWithNumber() throws Exception {
        ExpireSnapshots expire = store.newExpire(1, 3, Long.MAX_VALUE);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            commit(ThreadLocalRandom.current().nextInt(5) + 1, allData, snapshotPositions);
            expire.expire();

            int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
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

        store.assertCleaned();
    }

    @Test
    public void testExpireWithTime() throws Exception {
        ExpireConfig.Builder builder = ExpireConfig.builder();
        builder.snapshotRetainMin(1)
                .snapshotRetainMax(Integer.MAX_VALUE)
                .snapshotTimeRetain(Duration.ofMillis(1000));
        ExpireSnapshots expire = store.newExpire(builder.build(), true);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        Thread.sleep(1500);
        commit(5, allData, snapshotPositions);
        long expireMillis = System.currentTimeMillis();
        // expire twice to check for idempotence

        expire.config(builder.snapshotTimeRetain(Duration.ofMillis(1000)).build()).expire();
        expire.config(builder.snapshotTimeRetain(Duration.ofMillis(1000)).build()).expire();

        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            if (snapshotManager.snapshotExists(i)) {
                assertThat(snapshotManager.snapshot(i).timeMillis())
                        .isBetween(expireMillis - 1000, expireMillis);
                assertSnapshot(i, allData, snapshotPositions);
            }
        }

        store.assertCleaned();
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

        store.assertCleaned();
    }

    @Test
    public void testChangelogOutLivedSnapshot() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(10, allData, snapshotPositions);
        ExpireConfig config =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(2)
                        .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                        .changelogRetainMax(3)
                        .build();

        ExpireSnapshots snapshot = store.newExpire(config, true);
        ExpireSnapshots changelog = store.newChangelogExpire(config, true);
        // expire twice to check for idempotence
        snapshot.expire();
        snapshot.expire();

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        int earliestSnapshotId = snapshotManager.earliestSnapshotId().intValue();
        int latestLongLivedChangelogId = snapshotManager.latestLongLivedChangelogId().intValue();
        int earliestLongLivedChangelogId =
                snapshotManager.earliestLongLivedChangelogId().intValue();

        // 2 snapshot in /snapshot
        assertThat(latestSnapshotId - earliestSnapshotId).isEqualTo(1);
        assertThat(earliestLongLivedChangelogId).isEqualTo(1);
        // The changelog id and snapshot id is continuous
        assertThat(earliestSnapshotId - latestLongLivedChangelogId).isEqualTo(1);

        changelog.expire();
        changelog.expire();

        assertThat(snapshotManager.latestSnapshotId().intValue()).isEqualTo(latestSnapshotId);
        assertThat(snapshotManager.earliestSnapshotId().intValue()).isEqualTo(earliestSnapshotId);
        assertThat(snapshotManager.latestLongLivedChangelogId())
                .isEqualTo(snapshotManager.earliestSnapshotId() - 1);
        assertThat(snapshotManager.earliestLongLivedChangelogId())
                .isEqualTo(snapshotManager.earliestSnapshotId() - 1);
        store.assertCleaned();
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
                        DeduplicateMergeFunction.factory(),
                        null)
                .changelogProducer(changelogProducer)
                .build();
    }

    protected void commit(int numCommits, List<KeyValue> allData, List<Integer> snapshotPositions)
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

    protected void assertSnapshot(
            int snapshotId, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        assertSnapshot(snapshotManager.snapshot(snapshotId), allData, snapshotPositions);
    }

    protected void assertSnapshot(
            Snapshot snapshot, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        int snapshotId = (int) snapshot.id();
        Map<BinaryRow, BinaryRow> expected =
                store.toKvMap(allData.subList(0, snapshotPositions.get(snapshotId - 1)));
        List<KeyValue> actualKvs =
                store.readKvsFromManifestEntries(
                        store.newScan().withSnapshot(snapshot).plan().files(), false);
        gen.sort(actualKvs);
        Map<BinaryRow, BinaryRow> actual = store.toKvMap(actualKvs);
        assertThat(actual).isEqualTo(expected);
    }
}
