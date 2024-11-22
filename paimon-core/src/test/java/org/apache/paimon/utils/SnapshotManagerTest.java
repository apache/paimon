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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link SnapshotManager}. */
public class SnapshotManagerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSnapshotPath() {
        SnapshotManager snapshotManager =
                new SnapshotManager(LocalFileIO.create(), new Path(tempDir.toString()));
        for (int i = 0; i < 20; i++) {
            assertThat(snapshotManager.snapshotPath(i))
                    .isEqualTo(new Path(tempDir.toString() + "/snapshot/snapshot-" + i));
        }
    }

    @Test
    public void testEarlierThanTimeMillis() throws IOException {
        long base = System.currentTimeMillis();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numSnapshots = random.nextInt(1, 20);
        Set<Long> set = new HashSet<>();
        while (set.size() < numSnapshots) {
            set.add(base + random.nextLong(0, 1_000_000));
        }
        List<Long> millis = set.stream().sorted().collect(Collectors.toList());

        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
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
            Long actual = snapshotManager.earlierThanTimeMills(time, false);

            if (millis.get(numSnapshots - 1) < time) {
                assertThat(actual).isEqualTo(firstSnapshotId + numSnapshots - 1);
            } else {
                for (int i = 0; i < numSnapshots; i++) {
                    if (millis.get(i) >= time) {
                        assertThat(actual).isEqualTo(firstSnapshotId + i - 1);
                        break;
                    }
                }
            }
        }
    }

    @Test
    public void testEarlierOrEqualTimeMills() throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        // there is no snapshot smaller than "millis - 1L" return the earliest snapshot
        assertThat(snapshotManager.earlierOrEqualTimeMills(millis - 1L).timeMillis())
                .isEqualTo(millis);

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

    @Test
    public void testLaterOrEqualTimeMills() throws IOException {
        long millis = 1684726826L;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
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

    @Test
    public void testlaterOrEqualWatermark() throws IOException {
        long millis = Long.MIN_VALUE;
        FileIO localFileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 10 snapshots
        for (long i = 0; i < 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis, Long.MIN_VALUE);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }
        // smaller than the second snapshot
        assertThat(snapshotManager.laterOrEqualWatermark(millis + 999)).isNull();
    }

    private Snapshot createSnapshotWithMillis(long id, long millis) {
        return new Snapshot(
                id,
                0L,
                null,
                null,
                null,
                null,
                null,
                0L,
                Snapshot.CommitKind.APPEND,
                millis,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private Snapshot createSnapshotWithMillis(long id, long millis, long watermark) {
        return new Snapshot(
                id,
                0L,
                null,
                null,
                null,
                null,
                null,
                0L,
                Snapshot.CommitKind.APPEND,
                millis,
                null,
                null,
                null,
                null,
                watermark,
                null);
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
                        0L,
                        Snapshot.CommitKind.APPEND,
                        millis,
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
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
        // create 100 snapshots using user "lastCommitUser"
        for (long i = 0; i < 100; i++) {
            Snapshot snapshot =
                    new Snapshot(
                            i,
                            0L,
                            null,
                            null,
                            null,
                            null,
                            "lastCommitUser",
                            0L,
                            Snapshot.CommitKind.APPEND,
                            i * 1000,
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
        localFileIO.deleteQuietly(snapshotManager.snapshotPath(0));
        thread.join();

        assertThat(exception.get()).isNull();
    }

    @Test
    public void testTraversalSnapshotsFromLatestSafely() throws IOException, InterruptedException {
        FileIO localFileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toString());
        SnapshotManager snapshotManager = new SnapshotManager(localFileIO, path);
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
                            0L,
                            Snapshot.CommitKind.APPEND,
                            i * 1000,
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
                new SnapshotManager(localFileIO, new Path(tempDir.toString()));
        long millis = 1L;
        for (long i = 1; i <= 5; i++) {
            Changelog changelog = createChangelogWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(
                    snapshotManager.longLivedChangelogPath(i), changelog.toJson());
        }

        for (long i = 6; i <= 10; i++) {
            Snapshot snapshot = createSnapshotWithMillis(i, millis + i * 1000);
            localFileIO.tryToWriteAtomic(snapshotManager.snapshotPath(i), snapshot.toJson());
        }

        Assertions.assertThat(snapshotManager.earliestLongLivedChangelogId()).isEqualTo(1);
        Assertions.assertThat(snapshotManager.latestChangelogId()).isEqualTo(10);
        Assertions.assertThat(snapshotManager.latestLongLivedChangelogId()).isEqualTo(5);
        Assertions.assertThat(snapshotManager.earliestSnapshotId()).isEqualTo(6);
        Assertions.assertThat(snapshotManager.latestSnapshotId()).isEqualTo(10);
        Assertions.assertThat(snapshotManager.changelog(1)).isNotNull();
    }
}
