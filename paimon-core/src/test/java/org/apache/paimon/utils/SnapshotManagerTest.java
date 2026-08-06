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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.paimon.SnapshotTest.newChangelogManager;
import static org.apache.paimon.SnapshotTest.newSnapshotManager;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Tests for {@link SnapshotManager}. */
public class SnapshotManagerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRetry() throws IOException {
        SnapshotManager snapshotManager =
                newSnapshotManager(LocalFileIO.create(), new Path(tempDir.toString()));
        File file = new File(tempDir.toFile(), "/snapshot/snapshot-1");
        file.getParentFile().mkdir();
        file.createNewFile();
        assertThatThrownBy(() -> snapshotManager.snapshot(1))
                .hasMessageContaining("Retry fail after 10 times");
    }

    @Test
    public void testSnapshotPath() {
        SnapshotManager snapshotManager =
                newSnapshotManager(LocalFileIO.create(), new Path(tempDir.toString()));
        for (int i = 0; i < 20; i++) {
            assertThat(snapshotManager.snapshotPath(i))
                    .isEqualTo(new Path(tempDir.toString() + "/snapshot/snapshot-" + i));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEarliestSnapshot(boolean isRaceCondition) throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), isRaceCondition);
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        assertThat(snapshotManager.earliestSnapshot().id()).isEqualTo(isRaceCondition ? 1 : 0);
    }

    @Test
    public void testEarliestSnapshotThrowsWhenRetryExhausted() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), true);
        Snapshot snapshot = createSnapshotWithMillis(0, 1684726826L);
        localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(0), snapshot.toJson());

        assertThatThrownBy(snapshotManager::earliestSnapshot)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot find earliest snapshot");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEarlierOrEqualWatermark(boolean isRaceCondition) throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), isRaceCondition);
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        if (isRaceCondition) {
            // The earliest snapshot has expired, so no remaining snapshot carries a
            // watermark earlier than or equal to the requested one
            assertThat(snapshotManager.earlierOrEqualWatermark(millis + 999)).isNull();
        } else {
            assertThat(snapshotManager.earlierOrEqualWatermark(millis + 999).id()).isEqualTo(0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEarlierThanTimeMillis(boolean isRaceCondition) throws IOException {
        long base = System.currentTimeMillis();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numSnapshots = isRaceCondition ? random.nextInt(2, 20) : random.nextInt(1, 20);
        Set<Long> set = new HashSet<>();
        while (set.size() < numSnapshots) {
            set.add(base + random.nextLong(0, 1_000_000));
        }
        List<Long> millis = set.stream().sorted().collect(Collectors.toList());

        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), isRaceCondition);
        int firstSnapshotId = random.nextInt(1, 100);
        for (int i = 0; i < numSnapshots; i++) {
            Snapshot snapshot = createSnapshotWithMillis(firstSnapshotId + i, millis.get(i));
            localFileIO.tryToWriteAtomic(
                    snapshotManager.snapshotPath(firstSnapshotId + i), snapshot.toJson());
        }

        for (int tries = 0; tries < 10; tries++) {
            long time;
            if (random.nextBoolean()) {
                // pick a random time
                time = base + random.nextLong(0, 1_000_000);
            } else {
                // pick a random time equal to one of the snapshots
                time = millis.get(random.nextInt(numSnapshots));
            }
            Long actual =
                    TimeTravelUtil.earlierThanTimeMills(snapshotManager, null, time, false, false);

            if (millis.get(numSnapshots - 1) < time) {
                assertThat(actual).isEqualTo(firstSnapshotId + numSnapshots - 1);
            } else {
                for (int i = 0; i < numSnapshots; i++) {
                    if (millis.get(i) >= time) {
                        if (isRaceCondition && i == 0) {
                            // The first snapshot expired during invocation
                            assertThat(actual).isLessThanOrEqualTo(firstSnapshotId);
                        } else {
                            assertThat(actual).isLessThanOrEqualTo(firstSnapshotId + i - 1);
                        }
                        break;
                    }
                }
            }
        }
    }

    @Test
    public void testEarlierThanTimeMillisThrowsWhenEarliestRetryExhausted() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), true);
        Snapshot snapshot = createSnapshotWithMillis(0, 1684726826L);
        localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(0), snapshot.toJson());

        assertThatThrownBy(
                        () ->
                                TimeTravelUtil.earlierThanTimeMills(
                                        snapshotManager, null, 1684726827L, false, false))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Cannot find earliest snapshot");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEarlierOrEqualTimeMills(boolean isRaceCondition) throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), isRaceCondition);
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        if (isRaceCondition) {
            // The earliest snapshot has expired, so always return the second snapshot, smaller than
            // the second snapshot return null
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis - 1L)).isEqualTo(null);
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 999)).isEqualTo(null);
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1000).timeMillis())
                    .isEqualTo(millis + 1000L);
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1001).timeMillis())
                    .isEqualTo(millis + 1000L);
        } else {
            // there is no snapshot smaller than "millis - 1L" return null
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis - 1L)).isEqualTo(null);

            // smaller than the second snapshot return the first snapshot
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 999).timeMillis())
                    .isEqualTo(millis);

            // equal to the second snapshot return the second snapshot
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1000).timeMillis())
                    .isEqualTo(millis + 1000);
            // larger than the second snapshot return the second snapshot
            assertThat(snapshotManager.earlierOrEqualTimeMills(millis + 1001).timeMillis())
                    .isEqualTo(millis + 1000);
        }
    }

    @Test
    public void testLaterOrEqualTimeMills() throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }
        // smaller than the second snapshot return the second snapshot
        assertThat(snapshotManager.laterOrEqualTimeMills(millis + 999).timeMillis())
                .isEqualTo(millis + 1000);
        // equal to the second snapshot return the second snapshot
        assertThat(snapshotManager.laterOrEqualTimeMills(millis + 1000).timeMillis())
                .isEqualTo(millis + 1000);
        // larger than the second snapshot return the third snapshot
        assertThat(snapshotManager.laterOrEqualTimeMills(millis + 1001).timeMillis())
                .isEqualTo(millis + 2000);

        // larger than the latest snapshot return null
        assertThat(snapshotManager.laterOrEqualTimeMills(millis + 10001)).isNull();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLaterOrEqualWatermark(boolean isRaceCondition) throws IOException {
        long millis = Long.MIN_VALUE;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new TestSnapshotManager(localFileIO, new Path(tempDir.toString()), isRaceCondition);
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis, Long.MIN_VALUE);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }
        // smaller than the second snapshot
        assertThat(snapshotManager.laterOrEqualWatermark(millis + 999)).isNull();
    }

    public static Snapshot createSnapshotWithMillis(long id, long millis) {
        return new Snapshot(
                id,
                0L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0L,
                Snapshot.CommitKind.APPEND,
                millis,
                0L,
                0L,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private Snapshot createSnapshotWithMillis(long id, long millis, @Nullable Long watermark) {
        return new Snapshot(
                id,
                0L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                0L,
                Snapshot.CommitKind.APPEND,
                millis,
                0L,
                0L,
                null,
                watermark,
                null,
                null,
                null,
                null);
    }

    private static Map<Long, Long> watermarkMap(long... idsAndWatermarks) {
        Map<Long, Long> watermarks = new HashMap<>();
        for (int i = 0; i < idsAndWatermarks.length; i += 2) {
            watermarks.put(idsAndWatermarks[i], idsAndWatermarks[i + 1]);
        }
        return watermarks;
    }

    private Changelog createChangelogWithMillis(long id, long millis) {
        return new Changelog(
                new Snapshot(
                        id,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        0L,
                        Snapshot.CommitKind.APPEND,
                        millis,
                        0L,
                        0L,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null));
    }

    @Test
    public void testLatestSnapshotOfUser() throws IOException, InterruptedException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 100 snapshots using user "lastCommitUser"
        for (long i = 1; i <= 100; i++) {
            Snapshot snapshot =
                    new Snapshot(
                            i,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            "lastCommitUser",
                            0L,
                            Snapshot.CommitKind.APPEND,
                            i * 1000,
                            0L,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // read the latest snapshot of user "currentCommitUser"
        AtomicReference<Exception> exception = new AtomicReference<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                snapshotManager.latestSnapshotOfUser("currentCommitUser");
                            } catch (Exception e) {
                                exception.set(e);
                            }
                        });
        thread.start();
        Thread.sleep(100);

        // expire snapshot
        localFileIO.deleteQuietly(snapshotManager.snapshotPath(1));
        thread.join();

        assertThat(exception.get()).isNull();
    }

    @Test
    public void testTraversalSnapshotsFromLatestSafely() throws IOException, InterruptedException {
        FileIO localFileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        SnapshotManager snapshotManager = newSnapshotManager(localFileIO, path);
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot =
                    new Snapshot(
                            i,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            0L,
                            Snapshot.CommitKind.APPEND,
                            i * 1000,
                            0L,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // read all
        List<Long> read = new ArrayList<>();
        snapshotManager.traversalSnapshotsFromLatestSafely(
                snapshot -> {
                    read.add(snapshot.id());
                    return false;
                });
        assertThat(read).containsExactly(9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L);

        // test quit if return true
        snapshotManager.traversalSnapshotsFromLatestSafely(
                snapshot -> {
                    if (snapshot.id() == 5) {
                        return true;
                    } else if (snapshot.id() < 5) {
                        fail("snapshot id %s is less than 5", snapshot.id());
                    }
                    return false;
                });

        // test safely
        Filter<Snapshot> func =
                snapshot -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                    return false;
                };
        AtomicReference<Exception> exception = new AtomicReference<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                snapshotManager.traversalSnapshotsFromLatestSafely(func);
                            } catch (Exception e) {
                                exception.set(e);
                            }
                        });

        thread.start();
        Thread.sleep(100);
        localFileIO.deleteQuietly(snapshotManager.snapshotPath(0));
        thread.join();

        assertThat(exception.get()).isNull();

        // test throw exception
        thread =
                new Thread(
                        () -> {
                            try {
                                snapshotManager.traversalSnapshotsFromLatestSafely(func);
                            } catch (Exception e) {
                                exception.set(e);
                            }
                        });

        thread.start();
        Thread.sleep(100);
        localFileIO.deleteQuietly(snapshotManager.snapshotPath(3));
        thread.join();

        assertThat(exception.get())
                .hasMessageFindingMatch("Snapshot file .* does not exist")
                .hasMessageContaining("dedicated compaction job");
    }

    @Test
    public void testLongLivedChangelog() throws Exception {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        ChangelogManager changelogManager =
                newChangelogManager(localFileIO, new Path(tempDir.toString()));
        long millis = 1L;
        for (long i = 1; i <= 5; i++) {
            Changelog changelog = createChangelogWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(
                    changelogManager.longLivedChangelogPath(i), changelog.toJson());
        }

        for (long i = 6; i <= 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        Assertions.assertThat(changelogManager.earliestLongLivedChangelogId()).isEqualTo(1);
        Assertions.assertThat(changelogManager.latestLongLivedChangelogId()).isEqualTo(5);
        Assertions.assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(6);
        Assertions.assertThat(snapshotManager.latestSnapshotId()).isEqualTo(10);
        Assertions.assertThat(changelogManager.changelog(1)).isNotNull();
    }

    @Test
    public void testCommitChangelogWhenSameChangelogCommitTwice() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        ChangelogManager snapshotManager =
                newChangelogManager(localFileIO, new Path(tempDir.toString()));
        long id = 1L;
        Changelog changelog = createChangelogWithMillis(id, 1L);
        snapshotManager.commitChangelog(changelog, id);
        assertDoesNotThrow(() -> snapshotManager.commitChangelog(changelog, id));
    }

    @Test
    @Timeout(60) // the search must terminate; defective code loops forever here
    public void testLaterOrEqualWatermarkWithInterleavedNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots, only snapshots 0, 4 and 9 carry a watermark
        Map<Long, Long> watermarks = watermarkMap(0, 100, 4, 200, 9, 300);
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // the null-watermark fallback must terminate and return snapshot 4
        assertThat(snapshotManager.laterOrEqualWatermark(150).id()).isEqualTo(4);
        assertThat(snapshotManager.laterOrEqualWatermark(250).id()).isEqualTo(9);
        // larger than the largest watermark returns null
        assertThat(snapshotManager.laterOrEqualWatermark(301)).isNull();
        // smaller than the smallest watermark returns the first snapshot with a watermark
        assertThat(snapshotManager.laterOrEqualWatermark(50).id()).isEqualTo(0);
    }

    @Test
    public void testLaterOrEqualWatermarkExactMatchWithNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 5 snapshots, snapshots 2 and 3 do not carry a watermark
        Map<Long, Long> watermarks = watermarkMap(0, 100, 1, 150, 4, 300);
        for (long i = 0; i < 5; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // the exact match is snapshot 1, not one of the null-watermark snapshots
        assertThat(snapshotManager.laterOrEqualWatermark(150).id()).isEqualTo(1);
    }

    @Test
    public void testLaterOrEqualWatermarkWithAllNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 5 snapshots without watermark
        for (long i = 0; i < 5; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        assertThat(snapshotManager.laterOrEqualWatermark(100)).isNull();
    }

    @Test
    public void testLaterOrEqualWatermarkWithNullWatermarkTail() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 4 snapshots, snapshots 2 and 3 do not carry a watermark
        Map<Long, Long> watermarks = watermarkMap(0, 100, 1, 200);
        for (long i = 0; i < 4; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        assertThat(snapshotManager.laterOrEqualWatermark(150).id()).isEqualTo(1);
        // the null-watermark tail does not extend the watermark range
        assertThat(snapshotManager.laterOrEqualWatermark(250)).isNull();
    }

    @Test
    public void testEarlierOrEqualWatermarkBelowMinimum() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 3 snapshots with dense watermarks
        for (long i = 0; i < 3; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, 100 + i * 100);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // smaller than the smallest watermark returns null
        assertThat(snapshotManager.earlierOrEqualWatermark(50)).isNull();
    }

    @Test
    @Timeout(60) // the search must terminate; defective code loops forever here
    public void testEarlierOrEqualWatermarkWithInterleavedNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots, only snapshots 0, 4 and 9 carry a watermark
        Map<Long, Long> watermarks = watermarkMap(0, 100, 4, 200, 9, 300);
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        assertThat(snapshotManager.earlierOrEqualWatermark(150).id()).isEqualTo(0);
        assertThat(snapshotManager.earlierOrEqualWatermark(250).id()).isEqualTo(4);
        assertThat(snapshotManager.earlierOrEqualWatermark(350).id()).isEqualTo(9);
    }

    @Test
    public void testEarlierOrEqualWatermarkExactMatchWithNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 5 snapshots, snapshots 2 and 3 do not carry a watermark
        Map<Long, Long> watermarks = watermarkMap(0, 100, 1, 150, 4, 300);
        for (long i = 0; i < 5; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // the exact match is snapshot 1, not one of the null-watermark snapshots
        assertThat(snapshotManager.earlierOrEqualWatermark(150).id()).isEqualTo(1);
        // null-watermark snapshots fall back to the nearest earlier watermark
        assertThat(snapshotManager.earlierOrEqualWatermark(200).id()).isEqualTo(1);
    }

    @Test
    public void testEarlierOrEqualWatermarkWithAllNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 5 snapshots without watermark
        for (long i = 0; i < 5; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        assertThat(snapshotManager.earlierOrEqualWatermark(100)).isNull();
    }

    @Test
    public void testWatermarkSearchWithMinValueSentinelAndNullWatermarks() throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // a mixed-engine history: snapshot 0 carries the Long.MIN_VALUE sentinel written by
        // engines without watermark semantics, snapshot 1 carries no watermark at all
        Map<Long, Long> watermarks = watermarkMap(0, Long.MIN_VALUE);
        for (long i = 0; i < 2; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // the Long.MIN_VALUE sentinel means "no watermark" and must never match a query;
        // in particular it must not be returned for a rollback to watermark 0
        assertThat(snapshotManager.earlierOrEqualWatermark(0)).isNull();
        assertThat(snapshotManager.laterOrEqualWatermark(0)).isNull();
    }

    @Test
    public void testWatermarkSearchWithMinValueSentinelMixedWithRealWatermarks()
            throws IOException {
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                newSnapshotManager(localFileIO, new Path(tempDir.toString()));
        // snapshots 0 and 3 carry the Long.MIN_VALUE sentinel, snapshots 1 and 2 carry real
        // watermarks
        Map<Long, Long> watermarks =
                watermarkMap(0, Long.MIN_VALUE, 1, 100, 2, 200, 3, Long.MIN_VALUE);
        for (long i = 0; i < 4; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, 1000 + i * 1000, watermarks.get(i));
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // the sentinel on the latest snapshot must not short-circuit the search
        assertThat(snapshotManager.earlierOrEqualWatermark(100).id()).isEqualTo(1);
        assertThat(snapshotManager.earlierOrEqualWatermark(250).id()).isEqualTo(2);
        assertThat(snapshotManager.laterOrEqualWatermark(150).id()).isEqualTo(2);
        // the leading sentinel must not shadow the real watermarks after it
        assertThat(snapshotManager.laterOrEqualWatermark(50).id()).isEqualTo(1);
        assertThat(snapshotManager.earlierOrEqualWatermark(50)).isNull();
    }

    /**
     * Test {@link SnapshotManager} to mock situations when there is a race condition, that the
     * earliest snapshot is deleted by another thread in the middle of the current thread's
     * invocation.
     */
    private static class TestSnapshotManager extends SnapshotManager {
        private final boolean isRaceCondition;

        private boolean deleteEarliestSnapshot = false;

        public TestSnapshotManager(FileIO fileIO, Path tablePath, boolean isRaceCondition) {
            super(fileIO, tablePath, DEFAULT_MAIN_BRANCH, null, null);
            this.isRaceCondition = isRaceCondition;
        }

        @Override
        public @Nullable Long earliestSnapshotId() {
            Long snapshotId = super.earliestSnapshotId();
            if (isRaceCondition && snapshotId != null && !deleteEarliestSnapshot) {
                Path snapshotPath = snapshotPath(snapshotId);
                try {
                    fileIO().delete(snapshotPath, true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                deleteEarliestSnapshot = true;
            }
            return snapshotId;
        }
    }
}
