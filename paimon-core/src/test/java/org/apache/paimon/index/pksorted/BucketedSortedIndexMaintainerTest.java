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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests bucket-local BTree/Bitmap source maintenance. */
class BucketedSortedIndexMaintainerTest {

    @TempDir java.nio.file.Path tempPath;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    void testRestoreBuildAndSourceRemoval() throws Exception {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads =
                Arrays.asList(payload("old-1", oldSource, 2), payload("old-2", oldSource, 1));
        List<IndexFileMeta> newPayloads =
                Arrays.asList(payload("new-1", newSource, 2), payload("new-2", newSource, 1));
        PkSortedIndexFile indexFile = new PkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        dataFile -> {
                            assertThat(dataFile).isEqualTo(newSource);
                            return newPayloads;
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(oldSource),
                                Collections.singletonList(newSource),
                                Collections.emptyList()),
                        true);

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles())
                .containsExactlyElementsOf(newPayloads);
        assertThat(commit.compactIncrement().get().deletedIndexFiles())
                .containsExactlyElementsOf(oldPayloads);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-2", 3));
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    @Test
    void testRestoreDeletesInvalidPayloadsBeforePublishingReplacement() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        List<IndexFileMeta> invalidPayloads =
                Collections.singletonList(payload("invalid", source, 2));
        List<IndexFileMeta> replacementPayloads =
                Arrays.asList(payload("new-1", source, 2), payload("new-2", source, 1));
        PkSortedIndexFile indexFile = new PkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        dataFile -> replacementPayloads,
                        Collections.singletonList(source),
                        invalidPayloads,
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit repair =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(repair.appendIncrement()).isPresent();
        assertThat(repair.appendIncrement().get().deletedIndexFiles())
                .containsExactlyElementsOf(invalidPayloads);
        assertThat(repair.appendIncrement().get().newIndexFiles())
                .containsExactlyElementsOf(replacementPayloads);

        BucketedSortedIndexMaintainer restored =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        dataFile -> {
                            throw new AssertionError("Covered source must not be rebuilt.");
                        },
                        Collections.singletonList(source),
                        replacementPayloads,
                        executor);
        BucketedSortedIndexMaintainer.SortedIndexCommit stable =
                restored.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        CompactIncrement.emptyIncrement(),
                        false,
                        false);

        assertThat(stable.appendIncrement()).isEmpty();
        assertThat(stable.compactIncrement()).isEmpty();
        assertThat(restored.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
    }

    @Test
    void testBuildFailureDoesNotBlockSourceRemoval() throws Exception {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads =
                Arrays.asList(payload("old-1", oldSource, 2), payload("old-2", oldSource, 1));
        AtomicInteger attempts = new AtomicInteger();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        dataFile -> {
                            attempts.incrementAndGet();
                            throw new IllegalStateException("expected build failure");
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(oldSource),
                                Collections.singletonList(newSource),
                                Collections.emptyList()),
                        true);

        assertThat(attempts).hasValue(3);
        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).isEmpty();
        assertThat(commit.compactIncrement().get().deletedIndexFiles())
                .containsExactlyElementsOf(oldPayloads);
        assertThat(maintainer.state().coveredSourceFiles()).isEmpty();
        assertThat(maintainer.state().uncoveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-2", 3));
    }

    @Test
    void testTransientFailureRetriesAndPublishesWholeGroup() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        List<IndexFileMeta> payloads =
                Arrays.asList(payload("new-1", source, 2), payload("new-2", source, 1));
        AtomicInteger attempts = new AtomicInteger();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        dataFile -> {
                            if (attempts.incrementAndGet() < 3) {
                                throw new IllegalStateException("expected transient failure");
                            }
                            return payloads;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), compactAfter(source), true);

        assertThat(attempts).hasValue(3);
        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles())
                .containsExactlyElementsOf(payloads);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
    }

    @Test
    void testNonBlockingBuildPublishesOnLaterCommit() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        List<IndexFileMeta> payloads =
                Arrays.asList(payload("new-1", source, 2), payload("new-2", source, 1));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        dataFile -> {
                            started.countDown();
                            release.await();
                            completed.countDown();
                            return payloads;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        BucketedSortedIndexMaintainer.SortedIndexCommit first =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), compactAfter(source), false);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(first.compactIncrement()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isTrue();

        release.countDown();
        assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
        BucketedSortedIndexMaintainer.SortedIndexCommit second =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), false);

        assertThat(second.appendIncrement()).isPresent();
        assertThat(second.appendIncrement().get().newIndexFiles())
                .containsExactlyElementsOf(payloads);
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testStaleCompletionIsDeletedBeforeReplacementPublishes() throws Exception {
        DataFileMeta staleSource = dataFile("data-1", 3);
        DataFileMeta activeSource = dataFile("data-2", 3);
        List<IndexFileMeta> stalePayloads =
                Arrays.asList(
                        payload("stale-1", staleSource, 2), payload("stale-2", staleSource, 1));
        List<IndexFileMeta> activePayloads =
                Arrays.asList(
                        payload("active-1", activeSource, 2), payload("active-2", activeSource, 1));
        CountDownLatch staleStarted = new CountDownLatch(1);
        CountDownLatch releaseStale = new CountDownLatch(1);
        CountDownLatch staleCompleted = new CountDownLatch(1);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        dataFile -> {
                            if (dataFile.fileName().equals(staleSource.fileName())) {
                                staleStarted.countDown();
                                releaseStale.await();
                                staleCompleted.countDown();
                                return stalePayloads;
                            }
                            assertThat(dataFile).isEqualTo(activeSource);
                            return activePayloads;
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(staleSource), false);
        assertThat(staleStarted.await(5, TimeUnit.SECONDS)).isTrue();
        maintainer.prepareCommit(
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.singletonList(staleSource),
                        Collections.singletonList(activeSource),
                        Collections.emptyList()),
                false);
        releaseStale.countDown();
        assertThat(staleCompleted.await(5, TimeUnit.SECONDS)).isTrue();

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), true);

        assertThat(indexFile.deleted()).containsExactlyElementsOf(stalePayloads);
        assertThat(commit.appendIncrement()).isPresent();
        assertThat(commit.appendIncrement().get().newIndexFiles())
                .containsExactlyElementsOf(activePayloads);
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-2", 3));
    }

    @Test
    void testRejectedSubmissionLeavesSourceUncovered() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        ExecutorService rejectedExecutor = Executors.newSingleThreadExecutor();
        rejectedExecutor.shutdownNow();
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        dataFile -> Collections.singletonList(payload("new", source, 3)),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        rejectedExecutor);

        BucketedSortedIndexMaintainer.SortedIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(), compactAfter(source), false);

        assertThat(commit.appendIncrement()).isEmpty();
        assertThat(commit.compactIncrement()).isEmpty();
        assertThat(maintainer.buildNotCompleted()).isFalse();
        assertThat(maintainer.state().uncoveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
    }

    @Test
    void testCloseDeletesResultCompletedAfterCancellation() throws Exception {
        DataFileMeta source = dataFile("data-1", 3);
        IndexFileMeta generated = payload("cancelled", source, 3);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        TrackingPkSortedIndexFile indexFile =
                new TrackingPkSortedIndexFile(LocalFileIO.create(), pathFactory());
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        indexFile,
                        dataFile -> {
                            started.countDown();
                            boolean released = false;
                            while (!released) {
                                try {
                                    release.await();
                                    released = true;
                                } catch (InterruptedException ignored) {
                                    // Simulate a builder completing despite cancellation.
                                }
                            }
                            return Collections.singletonList(generated);
                        },
                        Collections.emptyList(),
                        Collections.emptyList(),
                        executor);

        maintainer.prepareCommit(DataIncrement.emptyIncrement(), compactAfter(source), false);
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        maintainer.close();
        release.countDown();

        assertThat(indexFile.awaitDeletion()).isTrue();
        assertThat(indexFile.deleted()).containsExactly(generated);
        assertThat(maintainer.buildNotCompleted()).isFalse();
    }

    @Test
    void testInterruptedCommitRollsBackStateAndCancelsBuild() throws Exception {
        DataFileMeta oldSource = dataFile("data-1", 3);
        DataFileMeta newSource = dataFile("data-2", 3);
        List<IndexFileMeta> oldPayloads =
                Arrays.asList(payload("old-1", oldSource, 2), payload("old-2", oldSource, 1));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        BucketedSortedIndexMaintainer maintainer =
                new BucketedSortedIndexMaintainer(
                        7,
                        "btree",
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        dataFile -> {
                            started.countDown();
                            try {
                                new CountDownLatch(1).await();
                                throw new AssertionError("Build must be cancelled.");
                            } catch (InterruptedException e) {
                                cancelled.countDown();
                                throw e;
                            }
                        },
                        Collections.singletonList(oldSource),
                        oldPayloads,
                        executor);
        CompactIncrement transition =
                new CompactIncrement(
                        Collections.singletonList(oldSource),
                        Collections.singletonList(newSource),
                        Collections.emptyList());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread commitThread =
                new Thread(
                        () -> {
                            try {
                                maintainer.prepareCommit(
                                        DataIncrement.emptyIncrement(), transition, true);
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        });

        commitThread.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        commitThread.interrupt();
        commitThread.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(commitThread.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(InterruptedException.class);
        assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(maintainer.buildNotCompleted()).isFalse();
        assertThat(maintainer.state().coveredSourceFiles())
                .containsExactly(new PrimaryKeyIndexSourceFile("data-1", 3));
        assertThat(maintainer.state().uncoveredSourceFiles()).isEmpty();
    }

    private static CompactIncrement compactAfter(DataFileMeta source) {
        return new CompactIncrement(
                Collections.emptyList(),
                Collections.singletonList(source),
                Collections.emptyList());
    }

    private static IndexFileMeta payload(
            String fileName, DataFileMeta sourceFile, long payloadRowCount) {
        PrimaryKeyIndexSourceFile source =
                new PrimaryKeyIndexSourceFile(sourceFile.fileName(), sourceFile.rowCount());
        return new IndexFileMeta(
                "btree",
                fileName,
                1,
                payloadRowCount,
                new GlobalIndexMeta(
                        0,
                        source.rowCount() - 1,
                        7,
                        null,
                        new byte[] {1},
                        new PrimaryKeyIndexSourceMeta(source).serialize()),
                null);
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        rowCount,
                        SimpleStats.EMPTY_STATS,
                        0,
                        1,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.COMPACT,
                        null,
                        null,
                        null,
                        null)
                .upgrade(1);
    }

    private IndexPathFactory pathFactory() {
        Path directory = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(directory, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(directory, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    private static class TrackingPkSortedIndexFile extends PkSortedIndexFile {

        private final List<IndexFileMeta> deleted = new java.util.ArrayList<>();
        private final CountDownLatch deletion = new CountDownLatch(1);

        private TrackingPkSortedIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
            super(fileIO, pathFactory);
        }

        @Override
        public synchronized void delete(IndexFileMeta file) {
            deleted.add(file);
            deletion.countDown();
        }

        private synchronized List<IndexFileMeta> deleted() {
            return new java.util.ArrayList<>(deleted);
        }

        private boolean awaitDeletion() throws InterruptedException {
            return deletion.await(1, TimeUnit.SECONDS);
        }
    }
}
