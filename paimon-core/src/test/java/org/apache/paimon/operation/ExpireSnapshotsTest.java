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
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.KeyValue;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TestFileStore;
import org.apache.paimon.TestKeyValueGenerator;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SlowFileIO;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.HintFileUtils.EARLIEST;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link ExpireSnapshotsImpl}. */
public class ExpireSnapshotsTest {

    protected final FileIO fileIO = new LocalFileIO();
    protected TestKeyValueGenerator gen;
    @TempDir java.nio.file.Path tempDir;
    @TempDir java.nio.file.Path tempExternalPath;
    protected TestFileStore store;
    protected SnapshotManager snapshotManager;
    protected ChangelogManager changelogManager;

    @BeforeEach
    public void beforeEach() throws Exception {
        gen = new TestKeyValueGenerator();
        store = createStore();
        snapshotManager = store.snapshotManager();
        changelogManager = store.changelogManager();
        SchemaManager schemaManager = new SchemaManager(fileIO, new Path(tempDir.toUri()));
        schemaManager.createTable(
                new Schema(
                        TestKeyValueGenerator.DEFAULT_ROW_TYPE.getFields(),
                        TestKeyValueGenerator.DEFAULT_PART_TYPE.getFieldNames(),
                        TestKeyValueGenerator.getPrimaryKeys(
                                TestKeyValueGenerator.GeneratorMode.MULTI_PARTITIONED),
                        store.options().toMap(),
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
    public void testExpireWithMissingFilesWithExternalPath() throws Exception {
        String externalPath = "file://" + tempExternalPath.toString();
        store.options().toConfiguration().set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, externalPath);
        store.options()
                .toConfiguration()
                .set(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY,
                        ExternalPathStrategy.ROUND_ROBIN);
        ExpireSnapshots expire = store.newExpire(1, 1, 1);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);

        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        Set<Path> filesInUseTmp = store.getFilesInUse(latestSnapshotId);

        Set<Path> filesInUse =
                filesInUseTmp.stream()
                        .map(p -> new Path(Paths.get(p.toUri().getPath()).toString()))
                        .collect(Collectors.toSet());
        List<Path> unusedFileList =
                Files.walk(Paths.get(tempDir.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("snapshot"))
                        .filter(p -> !p.getFileName().toString().startsWith("schema"))
                        .map(p -> new Path(p.toString()))
                        .filter(p -> !filesInUse.contains(p))
                        .collect(Collectors.toList());
        unusedFileList.addAll(
                Files.walk(Paths.get(tempExternalPath.toString()))
                        .filter(Files::isRegularFile)
                        .filter(p -> !p.getFileName().toString().startsWith("snapshot"))
                        .filter(p -> !p.getFileName().toString().startsWith("schema"))
                        .map(p -> new Path(p.toString()))
                        .filter(p -> !filesInUse.contains(p))
                        .collect(Collectors.toList()));

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
                    Collections.emptyList(),
                    false);
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
        new LocalFileIO().tryToWriteAtomic(myDataFile, "1");
        Path extra1 = new Path(bucketPath, "extra1");
        fileIO.tryToWriteAtomic(extra1, "2");
        Path extra2 = new Path(bucketPath, "extra2");
        fileIO.tryToWriteAtomic(extra2, "3");

        // create DataFileMeta and ManifestEntry
        List<String> extraFiles = Arrays.asList("extra1", "extra2");
        DataFileMeta dataFile =
                DataFileMeta.create(
                        "myDataFile",
                        1,
                        1,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
                        0,
                        0,
                        extraFiles,
                        Timestamp.now(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null);
        ManifestEntry add = ManifestEntry.create(FileKind.ADD, partition, 0, 1, dataFile);
        ManifestEntry delete = ManifestEntry.create(FileKind.DELETE, partition, 0, 1, dataFile);

        // expire
        cleanDeletedDataFiles(expire.snapshotDeletion(), Arrays.asList(add, delete));

        // check
        assertThat(fileIO.exists(myDataFile)).isFalse();
        assertThat(fileIO.exists(extra1)).isFalse();
        assertThat(fileIO.exists(extra2)).isFalse();

        store.assertCleaned();
    }

    @Test
    public void testExpireExtraFilesWithExternalPath() throws IOException {
        String externalPath = "file://" + tempExternalPath.toString();
        store.options().toConfiguration().set(CoreOptions.DATA_FILE_EXTERNAL_PATHS, externalPath);
        store.options()
                .toConfiguration()
                .set(
                        CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY,
                        ExternalPathStrategy.ROUND_ROBIN);
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) store.newExpire(1, 3, Long.MAX_VALUE);
        // write test files
        BinaryRow partition = gen.getPartition(gen.next());

        DataFilePathFactory dataFilePathFactory =
                store.pathFactory().createDataFilePathFactory(partition, 0);
        Path myDataFile = dataFilePathFactory.newPath();
        String fileName = myDataFile.getName();
        new LocalFileIO().tryToWriteAtomic(myDataFile, "1");
        Path extra1 = new Path(myDataFile.getParent(), "extra1");
        fileIO.tryToWriteAtomic(extra1, "2");
        Path extra2 = new Path(myDataFile.getParent(), "extra2");
        fileIO.tryToWriteAtomic(extra2, "3");

        // create DataFileMeta and ManifestEntry
        List<String> extraFiles = Arrays.asList("extra1", "extra2");
        DataFileMeta dataFile =
                DataFileMeta.create(
                        fileName,
                        1,
                        1,
                        EMPTY_ROW,
                        EMPTY_ROW,
                        SimpleStats.EMPTY_STATS,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
                        0,
                        0,
                        extraFiles,
                        Timestamp.now(),
                        0L,
                        null,
                        FileSource.APPEND,
                        null,
                        myDataFile.toString(),
                        null,
                        null);
        ManifestEntry add = ManifestEntry.create(FileKind.ADD, partition, 0, 1, dataFile);
        ManifestEntry delete = ManifestEntry.create(FileKind.DELETE, partition, 0, 1, dataFile);

        // expire
        cleanDeletedDataFiles(expire.snapshotDeletion(), Arrays.asList(add, delete));

        // check
        assertThat(fileIO.exists(myDataFile)).isFalse();
        assertThat(fileIO.exists(extra1)).isFalse();
        assertThat(fileIO.exists(extra2)).isFalse();

        store.assertCleaned();
    }

    private void cleanDeletedDataFiles(
            SnapshotDeletion snapshotDeletion, List<ManifestEntry> dataFileLog) {
        List<ManifestFileMeta> manifests = store.manifestFileFactory().create().write(dataFileLog);
        String manifestList = store.manifestListFactory().create().write(manifests).getLeft();
        Snapshot snapshot = snapshotWithDeltaManifestList(manifestList);

        snapshotDeletion.cleanDataFiles(
                snapshotDeletion.planDeletedInDeltaManifest(snapshot, file -> false));

        for (ManifestFileMeta manifest : manifests) {
            fileIO.deleteQuietly(store.pathFactory().toManifestFilePath(manifest.fileName()));
        }
        fileIO.deleteQuietly(store.pathFactory().toManifestListPath(manifestList));
    }

    private Snapshot snapshotWithDeltaManifestList(String manifestList) {
        return snapshotWithManifestLists(manifestList, null);
    }

    private Snapshot snapshotWithChangelogManifestList(String manifestList) {
        return snapshotWithManifestLists(null, manifestList);
    }

    private Snapshot snapshotWithManifestLists(
            String deltaManifestList, String changelogManifestList) {
        return new Snapshot(
                0,
                0L,
                null,
                null,
                deltaManifestList,
                null,
                changelogManifestList,
                null,
                null,
                "test",
                0L,
                Snapshot.CommitKind.APPEND,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null);
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
    public void testExpireEmptySnapshot() throws Exception {
        Random random = new Random();

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(100, allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();

        List<Thread> s = new ArrayList<>();
        s.add(
                new Thread(
                        () -> {
                            final ExpireSnapshotsImpl expire =
                                    (ExpireSnapshotsImpl) store.newExpire(1, Integer.MAX_VALUE, 1);
                            expire.expireUntil(89, latestSnapshotId);
                        }));
        for (int i = 0; i < 10; i++) {
            final ExpireSnapshotsImpl expire =
                    (ExpireSnapshotsImpl) store.newExpire(1, Integer.MAX_VALUE, 1);
            s.add(
                    new Thread(
                            () -> {
                                int start = random.nextInt(latestSnapshotId - 10);
                                int end = start + random.nextInt(10);
                                expire.expireUntil(start, end);
                            }));
        }

        Assertions.assertThatCode(
                        () -> {
                            s.forEach(Thread::start);
                            s.forEach(
                                    tt -> {
                                        try {
                                            tt.join();
                                        } catch (InterruptedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                        })
                .doesNotThrowAnyException();
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
        Path earliest = new Path(snapshotDir, EARLIEST);

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
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) store.newExpire(builder.build());

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        for (int i = 1; i <= 5; i++) {
            rewriteSnapshotTime(i, 0);
        }
        commit(5, allData, snapshotPositions);
        for (int i = 6; i <= 10; i++) {
            rewriteSnapshotTime(i, 2000);
        }

        // expire at time 2500, olderThanMills = 1500
        expire.setCurrentTimeMillis(() -> 2500L);
        // expire twice to check for idempotence
        expire.config(builder.snapshotTimeRetain(Duration.ofMillis(1000)).build()).expire();
        expire.config(builder.snapshotTimeRetain(Duration.ofMillis(1000)).build()).expire();

        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        // snapshots 1-4 should be expired, snapshot 5 is retained because its next
        // snapshot (6) is within the time window
        for (int i = 1; i <= 4; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        for (int i = 5; i <= latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }

        store.assertCleaned();
    }

    @Test
    public void testExpireCollectsSnapshotsConcurrently() throws Exception {
        store.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        ExpireConfig config = expireAllButLatestConfig();

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(5, allData, snapshotPositions);
        for (int i = 1; i <= 5; i++) {
            rewriteSnapshotTime(i, 0);
        }

        BlockingSnapshotManager blockingSnapshotManager =
                new BlockingSnapshotManager(snapshotManager, 1, 5);
        ExpireSnapshotsImpl expire =
                new ExpireSnapshotsImpl(
                        blockingSnapshotManager,
                        changelogManager,
                        store.newSnapshotDeletion(),
                        store.newTagManager());
        expire.config(config);
        expire.setCurrentTimeMillis(() -> 1000L);

        expire.expire();

        assertThat(blockingSnapshotManager.maxActiveReads()).isGreaterThan(1);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
        store.assertCleaned();
    }

    @Test
    public void testExpirePlansDataFilesConcurrently() throws Exception {
        store.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(6, allData, snapshotPositions);
        SnapshotManager snapshotManager = store.snapshotManager();
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            rewriteSnapshotTime(i, 0);
        }

        BlockingSnapshotDeletion snapshotDeletion =
                new BlockingSnapshotDeletion(store, 2, latestSnapshotId);
        snapshotDeletion.blockDataFilePlans();
        ExpireSnapshotsImpl expire =
                newExpireWithSnapshotDeletion(store, snapshotManager, snapshotDeletion);
        expire.config(expireAllButLatestConfig());
        expire.setCurrentTimeMillis(() -> 1000L);

        expire.expire();

        assertThat(snapshotDeletion.maxActiveDataFilePlans()).isGreaterThan(1);
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
        store.assertCleaned();
    }

    @Test
    public void testExpirePlansChangelogFilesConcurrently() throws Exception {
        TestFileStore inputStore = createStore(CoreOptions.ChangelogProducer.INPUT);
        inputStore.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(inputStore, 6, allData, snapshotPositions);
        SnapshotManager snapshotManager = inputStore.snapshotManager();
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            rewriteSnapshotTime(inputStore.fileIO(), snapshotManager, i, 0);
        }

        Set<String> changelogManifestLists = new HashSet<>();
        for (int i = 1; i < latestSnapshotId; i++) {
            String changelogManifestList = snapshotManager.snapshot(i).changelogManifestList();
            if (changelogManifestList != null) {
                changelogManifestLists.add(changelogManifestList);
            }
        }
        assertThat(changelogManifestLists.size()).isGreaterThan(1);

        BlockingSnapshotDeletion snapshotDeletion =
                new BlockingSnapshotDeletion(inputStore, 1, latestSnapshotId - 1);
        snapshotDeletion.blockChangelogPlans(changelogManifestLists);
        ExpireSnapshotsImpl expire =
                newExpireWithSnapshotDeletion(inputStore, snapshotManager, snapshotDeletion);
        expire.config(expireAllButLatestConfig());
        expire.setCurrentTimeMillis(() -> 1000L);

        expire.expire();

        assertThat(snapshotDeletion.maxActiveChangelogPlans()).isGreaterThan(1);
        assertSnapshot(inputStore, latestSnapshotId, allData, snapshotPositions);
        inputStore.assertCleaned();
    }

    @Test
    public void testExpirePlansManifestsConcurrentlyWithSkippingSet() throws Exception {
        store.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(6, allData, snapshotPositions);
        SnapshotManager snapshotManager = store.snapshotManager();
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            rewriteSnapshotTime(i, 0);
        }

        BlockingSnapshotDeletion snapshotDeletion =
                new BlockingSnapshotDeletion(store, 1, latestSnapshotId - 1);
        snapshotDeletion.blockManifestPlans();
        ExpireSnapshotsImpl expire =
                newExpireWithSnapshotDeletion(store, snapshotManager, snapshotDeletion);
        expire.config(expireAllButLatestConfig());
        expire.setCurrentTimeMillis(() -> 1000L);

        expire.expire();

        assertThat(snapshotDeletion.maxActiveManifestPlans()).isGreaterThan(1);
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
        store.assertCleaned();
    }

    @Test
    public void testExpireWithTagsAndConcurrentPlanningKeepsTaggedSnapshotsReadable()
            throws Exception {
        store.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(8, allData, snapshotPositions);
        int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        for (int i = 1; i <= latestSnapshotId; i++) {
            rewriteSnapshotTime(i, 0);
        }

        TagManager tagManager = store.newTagManager();
        tagManager.createTag(
                snapshotManager.snapshot(3),
                "tag3",
                store.options().tagDefaultTimeRetained(),
                Collections.emptyList(),
                false);
        tagManager.createTag(
                snapshotManager.snapshot(6),
                "tag6",
                store.options().tagDefaultTimeRetained(),
                Collections.emptyList(),
                false);

        ExpireSnapshotsImpl expire =
                (ExpireSnapshotsImpl) store.newExpire(expireAllButLatestConfig());
        expire.setCurrentTimeMillis(() -> 1000L);
        expire.expire();

        for (int i = 1; i < latestSnapshotId; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
        assertSnapshot(tagManager.getOrThrow("tag3").trimToSnapshot(), allData, snapshotPositions);
        assertSnapshot(tagManager.getOrThrow("tag6").trimToSnapshot(), allData, snapshotPositions);
    }

    @Test
    public void testTagManagerReadsTagsConcurrentlyWithObjectStoreFileIO() throws Exception {
        TestFileStore slowStore = createSlowStore();
        slowStore.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        try {
            List<KeyValue> allData = new ArrayList<>();
            List<Integer> snapshotPositions = new ArrayList<>();
            commit(slowStore, 6, allData, snapshotPositions);

            SnapshotManager snapshotManager = slowStore.snapshotManager();
            int latestSnapshotId = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
            TagManager tagManager = slowStore.newTagManager();
            for (int i = 1; i <= latestSnapshotId; i++) {
                tagManager.createTag(
                        snapshotManager.snapshot(i),
                        "tag" + i,
                        slowStore.options().tagDefaultTimeRetained(),
                        Collections.emptyList(),
                        false);
            }

            SlowFileIO.reset();
            SlowFileIO.setDelayMillis(20);

            List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

            assertThat(taggedSnapshots).hasSize(latestSnapshotId);
            for (int i = 1; i <= latestSnapshotId; i++) {
                assertThat(taggedSnapshots.get(i - 1).id()).isEqualTo(i);
            }
            assertThat(SlowFileIO.delayedOperations()).isGreaterThan(0);
            assertThat(SlowFileIO.maxActiveOperations()).isGreaterThan(1);
        } finally {
            SlowFileIO.reset();
        }
    }

    @Test
    public void testPlanUnusedDataFilesCancelsDeletionWhenManifestFileMissing() throws Exception {
        ManifestFileMeta missingManifest =
                new ManifestFileMeta(
                        "missing-manifest",
                        1,
                        0,
                        1,
                        SimpleStats.EMPTY_STATS,
                        0,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        String manifestList =
                store.manifestListFactory()
                        .create()
                        .write(Arrays.asList(missingManifest))
                        .getLeft();

        CapturingSnapshotDeletion snapshotDeletion = new CapturingSnapshotDeletion(store);
        assertThat(
                        snapshotDeletion.planDeletedInDeltaManifest(
                                snapshotWithDeltaManifestList(manifestList), entry -> false))
                .isEmpty();
    }

    @Test
    public void testExpireWithSlowObjectStoreFileIO() throws Exception {
        TestFileStore slowStore = createSlowStore();
        slowStore.options().toConfiguration().set(CoreOptions.FILE_OPERATION_THREAD_NUM, 4);

        try {
            List<KeyValue> allData = new ArrayList<>();
            List<Integer> snapshotPositions = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                commit(slowStore, 3, allData, snapshotPositions);
            }

            SnapshotManager slowSnapshotManager = slowStore.snapshotManager();
            int latestSnapshotId =
                    requireNonNull(slowSnapshotManager.latestSnapshotId()).intValue();
            for (int i = 1; i <= latestSnapshotId; i++) {
                rewriteSnapshotTime(slowStore.fileIO(), slowSnapshotManager, i, 0);
            }

            SlowFileIO.reset();
            SlowFileIO.setDelayMillis(20);
            ExpireConfig config =
                    ExpireConfig.builder()
                            .snapshotRetainMin(1)
                            .snapshotRetainMax(Integer.MAX_VALUE)
                            .snapshotTimeRetain(Duration.ofMillis(1))
                            .build();
            ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) slowStore.newExpire(config);
            expire.setCurrentTimeMillis(() -> 1000L);

            expire.expire();

            assertThat(SlowFileIO.delayedOperations()).isGreaterThan(0);
            assertThat(SlowFileIO.maxActiveOperations()).isGreaterThan(1);
            assertSnapshot(slowStore, latestSnapshotId, allData, snapshotPositions);
        } finally {
            SlowFileIO.reset();
        }
    }

    @Test
    public void testExpireWithTimeProtectsEachSnapshot() throws Exception {
        // Even with a small retainMin, each snapshot should be protected by
        // snapshotTimeRetain: a snapshot can only be expired when its next
        // snapshot has been alive longer than snapshotTimeRetain.
        ExpireConfig.Builder builder = ExpireConfig.builder();
        builder.snapshotRetainMin(1)
                .snapshotRetainMax(Integer.MAX_VALUE)
                .snapshotTimeRetain(Duration.ofMillis(5000));
        ExpireSnapshotsImpl expire = (ExpireSnapshotsImpl) store.newExpire(builder.build());

        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();

        // create 5 snapshots quickly
        commit(5, allData, snapshotPositions);
        for (int i = 1; i <= 5; i++) {
            rewriteSnapshotTime(i, 0);
        }

        // expire immediately - no snapshot should be expired because each
        // snapshot's next snapshot is still within the time window
        expire.setCurrentTimeMillis(() -> 100L);
        expire.config(builder.build()).expire();

        for (int i = 1; i <= 5; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isTrue();
            assertSnapshot(i, allData, snapshotPositions);
        }

        // create one more snapshot so snapshot 5 has a "next"
        commit(1, allData, snapshotPositions);
        rewriteSnapshotTime(6, 6000);

        // expire again - now snapshots 1-4 can be expired (their next snapshots
        // are older than 5000ms), but snapshot 5 is still protected because its
        // next snapshot (6) was just created
        expire.setCurrentTimeMillis(() -> 6500L);
        expire.config(builder.build()).expire();

        for (int i = 1; i <= 4; i++) {
            assertThat(snapshotManager.snapshotExists(i)).isFalse();
        }
        assertThat(snapshotManager.snapshotExists(5)).isTrue();
        assertThat(snapshotManager.snapshotExists(6)).isTrue();
        assertSnapshot(5, allData, snapshotPositions);
        assertSnapshot(6, allData, snapshotPositions);

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

    @RepeatedTest(5)
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

        ExpireSnapshots snapshot = store.newExpire(config);
        ExpireSnapshots changelog = store.newChangelogExpire(config);
        // expire twice to check for idempotence
        snapshot.expire();
        snapshot.expire();

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        int earliestSnapshotId = snapshotManager.earliestSnapshotId().intValue();
        int latestLongLivedChangelogId = changelogManager.latestLongLivedChangelogId().intValue();
        int earliestLongLivedChangelogId =
                changelogManager.earliestLongLivedChangelogId().intValue();

        // 2 snapshot in /snapshot
        assertThat(latestSnapshotId - earliestSnapshotId).isEqualTo(1);
        assertThat(earliestLongLivedChangelogId).isEqualTo(1);
        // The changelog id and snapshot id is continuous
        assertThat(earliestSnapshotId - latestLongLivedChangelogId).isEqualTo(1);

        changelog.expire();
        changelog.expire();

        assertThat(snapshotManager.latestSnapshotId().intValue()).isEqualTo(latestSnapshotId);
        assertThat(snapshotManager.earliestSnapshotId().intValue()).isEqualTo(earliestSnapshotId);
        assertThat(changelogManager.latestLongLivedChangelogId())
                .isEqualTo(snapshotManager.earliestSnapshotId() - 1);
        assertThat(changelogManager.earliestLongLivedChangelogId())
                .isEqualTo(snapshotManager.earliestSnapshotId() - 1);
        store.assertCleaned();
    }

    @Test
    public void testManifestFileSkippingSetFileNotFoundException() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(10, allData, snapshotPositions);

        Snapshot snapshot2 = snapshotManager.snapshot(2);
        TagManager tagManager = store.newTagManager();
        tagManager.createTag(snapshot2, "tag2", null, Collections.emptyList(), false);

        // delete manifest list file for tag2 to cause FileNotFoundException
        Path toDelete =
                store.pathFactory().manifestListFactory().toPath(snapshot2.baseManifestList());
        fileIO.deleteQuietly(toDelete);

        ExpireConfig config =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                        .build();

        store.newExpire(config).expire();

        int latestSnapshotId = snapshotManager.latestSnapshotId().intValue();
        assertSnapshot(latestSnapshotId, allData, snapshotPositions);
    }

    @Test
    public void testConsumerChangelogOnly() throws Exception {
        List<KeyValue> allData = new ArrayList<>();
        List<Integer> snapshotPositions = new ArrayList<>();
        commit(10, allData, snapshotPositions);

        // create a consumer at snapshot 3
        ConsumerManager consumerManager = new ConsumerManager(fileIO, new Path(tempDir.toUri()));
        consumerManager.resetConsumer("myConsumer", new Consumer(3));

        // without consumerChangelogOnly, consumer should prevent snapshot expiration
        ExpireConfig configDefault =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                        .build();
        store.newExpire(configDefault).expire();

        // earliest snapshot should be 3 (protected by consumer)
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(3L);

        // with consumerChangelogOnly=true, consumer should NOT prevent snapshot expiration
        // but changelog decoupled so changelogs are created
        ExpireConfig configChangelogOnly =
                ExpireConfig.builder()
                        .snapshotRetainMin(1)
                        .snapshotRetainMax(1)
                        .snapshotTimeRetain(Duration.ofMillis(Long.MAX_VALUE))
                        .changelogRetainMax(Integer.MAX_VALUE)
                        .consumerChangelogOnly(true)
                        .build();
        store.newExpire(configChangelogOnly).expire();

        int latestSnapshotId2 = requireNonNull(snapshotManager.latestSnapshotId()).intValue();
        // earliest snapshot should be latestSnapshotId (consumer no longer protects snapshots)
        assertThat(snapshotManager.earliestSnapshotId()).isEqualTo((long) latestSnapshotId2);
        assertSnapshot(latestSnapshotId2, allData, snapshotPositions);

        // changelog expiration should still be protected by consumer
        ExpireSnapshots changelogExpire = store.newChangelogExpire(configChangelogOnly);
        changelogExpire.expire();

        // earliest changelog should be 3 (still protected by consumer)
        Long earliestChangelogId = changelogManager.earliestLongLivedChangelogId();
        assertThat(earliestChangelogId).isNotNull();
        assertThat(earliestChangelogId).isEqualTo(3L);

        // clean up consumer file so assertCleaned passes
        consumerManager.deleteConsumer("myConsumer");
        store.assertCleaned();
    }

    private TestFileStore createStore() {
        return createStore(tempDir.toString());
    }

    private TestFileStore createSlowStore() {
        return createStore(SlowFileIO.SCHEME + "://" + tempDir.toString());
    }

    private TestFileStore createStore(String root) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        CoreOptions.ChangelogProducer changelogProducer;
        if (random.nextBoolean()) {
            changelogProducer = CoreOptions.ChangelogProducer.INPUT;
        } else {
            changelogProducer = CoreOptions.ChangelogProducer.NONE;
        }

        return createStore(root, changelogProducer);
    }

    private TestFileStore createStore(CoreOptions.ChangelogProducer changelogProducer) {
        return createStore(tempDir.toString(), changelogProducer);
    }

    private TestFileStore createStore(
            String root, CoreOptions.ChangelogProducer changelogProducer) {
        return new TestFileStore.Builder(
                        "avro",
                        root,
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

    private ExpireConfig expireAllButLatestConfig() {
        return ExpireConfig.builder()
                .snapshotRetainMin(1)
                .snapshotRetainMax(Integer.MAX_VALUE)
                .snapshotTimeRetain(Duration.ofMillis(1))
                .build();
    }

    private ExpireSnapshotsImpl newExpireWithSnapshotDeletion(
            TestFileStore store,
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion) {
        return new ExpireSnapshotsImpl(
                snapshotManager, store.changelogManager(), snapshotDeletion, store.newTagManager());
    }

    private void rewriteSnapshotTime(long snapshotId, long newTimeMillis) throws IOException {
        rewriteSnapshotTime(fileIO, snapshotManager, snapshotId, newTimeMillis);
    }

    private void rewriteSnapshotTime(
            FileIO fileIO, SnapshotManager snapshotManager, long snapshotId, long newTimeMillis)
            throws IOException {
        String oldJson = fileIO.readFileUtf8(snapshotManager.snapshotPath(snapshotId));
        ObjectNode node = (ObjectNode) JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(oldJson);
        node.put("timeMillis", newTimeMillis);
        String newJson = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.writeValueAsString(node);
        fileIO.overwriteFileUtf8(snapshotManager.snapshotPath(snapshotId), newJson);
        snapshotManager.invalidateCache();
    }

    private static class BlockingSnapshotManager extends SnapshotManager {

        private final long minBlockedSnapshotId;
        private final long maxBlockedSnapshotId;
        private final CountDownLatch releaseReads = new CountDownLatch(1);
        private final AtomicInteger activeReads = new AtomicInteger();
        private final AtomicInteger maxActiveReads = new AtomicInteger();

        private BlockingSnapshotManager(
                SnapshotManager snapshotManager,
                long minBlockedSnapshotId,
                long maxBlockedSnapshotId) {
            super(
                    snapshotManager.fileIO(),
                    snapshotManager.tablePath(),
                    snapshotManager.branch(),
                    null,
                    null);
            this.minBlockedSnapshotId = minBlockedSnapshotId;
            this.maxBlockedSnapshotId = maxBlockedSnapshotId;
        }

        @Override
        public Snapshot tryGetSnapshot(long snapshotId) throws java.io.FileNotFoundException {
            if (snapshotId < minBlockedSnapshotId
                    || snapshotId > maxBlockedSnapshotId
                    || releaseReads.getCount() == 0) {
                return super.tryGetSnapshot(snapshotId);
            }

            int active = activeReads.incrementAndGet();
            updateMaxActiveReads(active);
            if (active > 1) {
                releaseReads.countDown();
            }
            try {
                if (!releaseReads.await(5, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Snapshots were not read concurrently.");
                }
                return super.tryGetSnapshot(snapshotId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                activeReads.decrementAndGet();
            }
        }

        private void updateMaxActiveReads(int active) {
            int current;
            do {
                current = maxActiveReads.get();
                if (active <= current) {
                    return;
                }
            } while (!maxActiveReads.compareAndSet(current, active));
        }

        private int maxActiveReads() {
            return maxActiveReads.get();
        }
    }

    private static class BlockingSnapshotDeletion extends SnapshotDeletion {

        private final long minBlockedSnapshotId;
        private final long maxBlockedSnapshotId;
        private final ConcurrentCallTracker dataFilePlans =
                new ConcurrentCallTracker("Data file plans were not created concurrently.");
        private final ConcurrentCallTracker changelogPlans =
                new ConcurrentCallTracker("Changelog plans were not created concurrently.");
        private final ConcurrentCallTracker manifestPlans =
                new ConcurrentCallTracker("Manifest plans were not created concurrently.");

        private boolean blockDataFilePlans;
        private boolean blockManifestPlans;
        private Set<String> blockedChangelogManifestLists = Collections.emptySet();

        private BlockingSnapshotDeletion(
                TestFileStore store, long minBlockedSnapshotId, long maxBlockedSnapshotId) {
            super(
                    store.fileIO(),
                    store.pathFactory(),
                    store.manifestFileFactory().create(),
                    store.manifestListFactory().create(),
                    store.newIndexFileHandler(),
                    store.newStatsFileHandler(),
                    store.options().changelogProducer() != CoreOptions.ChangelogProducer.NONE,
                    store.options().cleanEmptyDirectories(),
                    store.options().fileOperationThreadNum());
            this.minBlockedSnapshotId = minBlockedSnapshotId;
            this.maxBlockedSnapshotId = maxBlockedSnapshotId;
        }

        private void blockDataFilePlans() {
            blockDataFilePlans = true;
        }

        private void blockChangelogPlans(Set<String> changelogManifestLists) {
            blockedChangelogManifestLists = changelogManifestLists;
        }

        private void blockManifestPlans() {
            blockManifestPlans = true;
        }

        @Override
        public List<Path> planDeletedInDeltaManifest(
                Snapshot snapshot, Predicate<ExpireFileEntry> skipper) {
            if (blockDataFilePlans && shouldBlock(snapshot.id())) {
                dataFilePlans.awaitConcurrentCall();
            }
            return super.planDeletedInDeltaManifest(snapshot, skipper);
        }

        @Override
        public List<Path> planAddedInChangelogManifest(Snapshot snapshot) {
            if (blockedChangelogManifestLists.contains(snapshot.changelogManifestList())) {
                changelogPlans.awaitConcurrentCall();
            }
            return super.planAddedInChangelogManifest(snapshot);
        }

        @Override
        public List<Runnable> planManifestsCleaner(Snapshot snapshot, Set<String> skippingSet) {
            if (blockManifestPlans && shouldBlock(snapshot.id())) {
                manifestPlans.awaitConcurrentCall();
            }
            return super.planManifestsCleaner(snapshot, skippingSet);
        }

        private boolean shouldBlock(long snapshotId) {
            return snapshotId >= minBlockedSnapshotId && snapshotId <= maxBlockedSnapshotId;
        }

        private int maxActiveDataFilePlans() {
            return dataFilePlans.maxActiveCalls();
        }

        private int maxActiveChangelogPlans() {
            return changelogPlans.maxActiveCalls();
        }

        private int maxActiveManifestPlans() {
            return manifestPlans.maxActiveCalls();
        }
    }

    private static class CapturingSnapshotDeletion extends SnapshotDeletion {

        private final List<List<Object>> deleteBatches = new ArrayList<>();

        private CapturingSnapshotDeletion(TestFileStore store) {
            super(
                    store.fileIO(),
                    store.pathFactory(),
                    store.manifestFileFactory().create(),
                    store.manifestListFactory().create(),
                    store.newIndexFileHandler(),
                    store.newStatsFileHandler(),
                    store.options().changelogProducer() != CoreOptions.ChangelogProducer.NONE,
                    store.options().cleanEmptyDirectories(),
                    store.options().fileOperationThreadNum());
        }

        @Override
        protected <F> void executeAll(
                Collection<F> files, java.util.function.Consumer<F> deletion) {
            if (!files.isEmpty()) {
                deleteBatches.add(new ArrayList<>(files));
            }
        }

        private void assertDeleteBatchesDeduplicated() {
            assertThat(deleteBatches).isNotEmpty();
            for (List<Object> batch : deleteBatches) {
                assertThat(new HashSet<>(batch)).hasSize(batch.size());
            }
        }

        private void reset() {
            deleteBatches.clear();
        }
    }

    private static class ConcurrentCallTracker {

        private final String timeoutMessage;
        private final CountDownLatch releaseCalls = new CountDownLatch(1);
        private final AtomicInteger activeCalls = new AtomicInteger();
        private final AtomicInteger maxActiveCalls = new AtomicInteger();

        private ConcurrentCallTracker(String timeoutMessage) {
            this.timeoutMessage = timeoutMessage;
        }

        private void awaitConcurrentCall() {
            int active = activeCalls.incrementAndGet();
            updateMaxActiveCalls(active);
            if (active > 1) {
                releaseCalls.countDown();
            }
            try {
                if (!releaseCalls.await(5, TimeUnit.SECONDS)) {
                    throw new RuntimeException(timeoutMessage);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                activeCalls.decrementAndGet();
            }
        }

        private void updateMaxActiveCalls(int active) {
            int current;
            do {
                current = maxActiveCalls.get();
                if (active <= current) {
                    return;
                }
            } while (!maxActiveCalls.compareAndSet(current, active));
        }

        private int maxActiveCalls() {
            return maxActiveCalls.get();
        }
    }

    protected void commit(int numCommits, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        commit(store, numCommits, allData, snapshotPositions);
    }

    protected void commit(
            TestFileStore store,
            int numCommits,
            List<KeyValue> allData,
            List<Integer> snapshotPositions)
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
        assertSnapshot(store, snapshotManager.snapshot(snapshotId), allData, snapshotPositions);
    }

    protected void assertSnapshot(
            TestFileStore store,
            int snapshotId,
            List<KeyValue> allData,
            List<Integer> snapshotPositions)
            throws Exception {
        assertSnapshot(
                store, store.snapshotManager().snapshot(snapshotId), allData, snapshotPositions);
    }

    protected void assertSnapshot(
            Snapshot snapshot, List<KeyValue> allData, List<Integer> snapshotPositions)
            throws Exception {
        assertSnapshot(store, snapshot, allData, snapshotPositions);
    }

    protected void assertSnapshot(
            TestFileStore store,
            Snapshot snapshot,
            List<KeyValue> allData,
            List<Integer> snapshotPositions)
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
