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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTableCommitTestUtils.PartialBarrierDeleteFileIO;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.table.format.FormatTableCommitTestUtils.awaitFailure;
import static org.apache.paimon.table.format.FormatTableCommitTestUtils.failureTree;
import static org.apache.paimon.table.format.FormatTableCommitTestUtils.observeContextClassLoaders;
import static org.apache.paimon.table.format.FormatTableCommitTestUtils.rootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests concurrent publication for {@link FormatTableCommit}. */
class FormatTableCommitPublishTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testCatalogManagedBuilderUses64WayPublishByDefault() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        ParallelPublishProbe probe = new ParallelPublishProbe(64);
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            Path partitionPath = new Path(tablePath, "part=p" + i);
            messages.add(
                    new TwoPhaseCommitMessage(
                            new ProbeCommitter(
                                    new Path(partitionPath, "data-new.csv"), probe::publish)));
        }
        FormatTableCommit commit =
                builderAppendCommit(tablePath, fileIO, partitionManager, Collections.emptyMap());
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(probe.awaitFirstWave()).isTrue();
            assertThat(probe.publishCalls()).isEqualTo(64);
            assertThat(probe.awaitUnexpectedExtraPublish()).isFalse();

            probe.releaseFirstWave();
            result.get(10, TimeUnit.SECONDS);
            assertThat(probe.publishCalls()).isEqualTo(65);
            assertThat(probe.maxConcurrentPublishes()).isEqualTo(64);
        } finally {
            probe.releaseFirstWave();
            caller.shutdownNow();
        }
    }

    @Test
    void testCatalogManagedBuilderHonorsConfiguredPublishConcurrency() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        ParallelPublishProbe probe = new ParallelPublishProbe(2);
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path partitionPath = new Path(tablePath, "part=p" + i);
            messages.add(
                    new TwoPhaseCommitMessage(
                            new ProbeCommitter(
                                    new Path(partitionPath, "data-new.csv"), probe::publish)));
        }
        FormatTableCommit commit =
                builderAppendCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM.key(), "2"));
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(probe.awaitFirstWave()).isTrue();
            assertThat(probe.publishCalls()).isEqualTo(2);
            assertThat(probe.awaitUnexpectedExtraPublish()).isFalse();

            probe.releaseFirstWave();
            result.get(10, TimeUnit.SECONDS);
            assertThat(probe.publishCalls()).isEqualTo(3);
            assertThat(probe.maxConcurrentPublishes()).isEqualTo(2);
        } finally {
            probe.releaseFirstWave();
            caller.shutdownNow();
        }
    }

    @Test
    void testConfiguredSerialPublishRunsOnTheCaller() {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        ConcurrentLinkedQueue<Thread> publishingThreads = new ConcurrentLinkedQueue<>();
        List<CommitMessage> messages = publishMessages(tablePath, publishingThreads, true);
        FormatTableCommit commit =
                builderAppendCommit(
                        tablePath,
                        fileIO,
                        partitionManager,
                        Collections.singletonMap(
                                CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM.key(), "1"));
        Thread caller = Thread.currentThread();

        commit.commit(messages);

        assertThat(publishingThreads).hasSize(3).containsOnly(caller);
    }

    @Test
    void testPublishConcurrencyStaysOffOutsideCatalogManagedPartitionedTables() {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Map<String, String> configured64 =
                Collections.singletonMap(
                        CoreOptions.FORMAT_TABLE_COMMIT_PUBLISH_THREAD_NUM.key(), "64");
        Thread caller = Thread.currentThread();

        ConcurrentLinkedQueue<Thread> filesystemThreads = new ConcurrentLinkedQueue<>();
        builderAppendCommit(tablePath, fileIO, null, configured64)
                .commit(publishMessages(tablePath, filesystemThreads, true));
        assertThat(filesystemThreads).hasSize(3).containsOnly(caller);

        ConcurrentLinkedQueue<Thread> unpartitionedThreads = new ConcurrentLinkedQueue<>();
        builderUnpartitionedAppendCommit(
                        new Path(tablePath, "unpartitioned"),
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        configured64)
                .commit(
                        publishMessages(
                                new Path(tablePath, "unpartitioned"), unpartitionedThreads, false));
        assertThat(unpartitionedThreads).hasSize(3).containsOnly(caller);

        FormatTablePartitionManager legacyPartitionManager =
                mock(FormatTablePartitionManager.class);
        FormatTableCommit legacy =
                new FormatTableCommit(
                        tablePath.toString(),
                        Collections.singletonList("part"),
                        fileIO,
                        false,
                        PARTITION_DEFAULT_NAME.defaultValue(),
                        false,
                        Identifier.create("publish_db", "legacy_publish_table"),
                        null,
                        null,
                        null,
                        legacyPartitionManager,
                        /* dynamicPartitionOverwrite */ true);
        ConcurrentLinkedQueue<Thread> legacyThreads = new ConcurrentLinkedQueue<>();
        legacy.commit(publishMessages(tablePath, legacyThreads, true));
        assertThat(legacyThreads).hasSize(3).containsOnly(caller);
    }

    @Test
    void testPublishKeepsSamePartitionOrderedWhileOtherPartitionsOverlap() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path p0 = new Path(tablePath, "part=p0");
        Path p1 = new Path(tablePath, "part=p1");
        CountDownLatch p0FirstStarted = new CountDownLatch(1);
        CountDownLatch p1Started = new CountDownLatch(1);
        CountDownLatch p0SecondStarted = new CountDownLatch(1);
        CountDownLatch releaseP0First = new CountDownLatch(1);
        CountDownLatch releaseP1 = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
        ProbeCommitter p0First =
                new ProbeCommitter(
                        new Path(p0, "data-0.csv"),
                        () -> {
                            events.add("p0-first-start");
                            p0FirstStarted.countDown();
                            awaitPublishLatch(releaseP0First, "first p0 publish release");
                            events.add("p0-first-end");
                        });
        ProbeCommitter p0Second =
                new ProbeCommitter(
                        new Path(p0, "data-1.csv"),
                        () -> {
                            events.add("p0-second");
                            p0SecondStarted.countDown();
                        });
        ProbeCommitter p1Only =
                new ProbeCommitter(
                        new Path(p1, "data-0.csv"),
                        () -> {
                            events.add("p1-start");
                            p1Started.countDown();
                            awaitPublishLatch(releaseP1, "p1 publish release");
                        });
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(p0First),
                        new TwoPhaseCommitMessage(p0Second),
                        new TwoPhaseCommitMessage(p1Only));
        ExecutorService publishExecutor = Executors.newFixedThreadPool(2);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        false,
                        null,
                        1,
                        2,
                        publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(p0FirstStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(p1Started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(p0SecondStarted.getCount()).isOne();

            releaseP0First.countDown();
            releaseP1.countDown();
            result.get(10, TimeUnit.SECONDS);

            assertThat(p0Second.commitCalls()).isOne();
            assertThat(new ArrayList<>(events)).containsSubsequence("p0-first-end", "p0-second");
        } finally {
            releaseP0First.countDown();
            releaseP1.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testCleanupFullyDrainsBeforeConcurrentPublishStarts() throws Exception {
        PartialBarrierDeleteFileIO fileIO = new PartialBarrierDeleteFileIO();
        Path tablePath = new Path(tempDir.toUri());
        Path p0 = new Path(tablePath, "part=p0");
        Path p1 = new Path(tablePath, "part=p1");
        fileIO.writeFile(new Path(p0, "data-000.csv"), "old", false);
        fileIO.writeFile(new Path(p1, "data-001.csv"), "old", false);
        CountDownLatch publishesStarted = new CountDownLatch(2);
        PublishAction publish =
                () -> {
                    if (fileIO.activeDeletes() != 0) {
                        throw new IOException("Publish overlapped overwrite cleanup");
                    }
                    publishesStarted.countDown();
                };
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(new Path(p0, "data-new.csv"), publish)),
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(new Path(p1, "data-new.csv"), publish)));
        ExecutorService publishExecutor = Executors.newFixedThreadPool(2);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        true,
                        null,
                        2,
                        2,
                        publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(fileIO.awaitBothDeletesStarted()).isTrue();
            assertThat(publishesStarted.getCount()).isEqualTo(2);

            fileIO.releaseFirstDelete();
            assertThat(fileIO.awaitFirstDeleteReturned()).isTrue();
            assertThat(result.isDone()).isFalse();
            assertThat(publishesStarted.getCount()).isEqualTo(2);

            fileIO.releaseSecondDelete();
            assertThat(publishesStarted.await(10, TimeUnit.SECONDS)).isTrue();
            result.get(10, TimeUnit.SECONDS);
        } finally {
            fileIO.releaseFirstDelete();
            fileIO.releaseSecondDelete();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testPublishFailureStopsRefillDrainsAcceptedWorkThenAborts() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path p0 = new Path(tablePath, "part=p0");
        Path p1 = new Path(tablePath, "part=p1");
        Path p2 = new Path(tablePath, "part=p2");
        Path p3 = new Path(tablePath, "part=p3");
        PublishExecutorTracker publishExecutor =
                new PublishExecutorTracker(Executors.newFixedThreadPool(3));
        CountDownLatch acceptedPublishesStarted = new CountDownLatch(3);
        CountDownLatch releaseFirstBlockedPublish = new CountDownLatch(1);
        CountDownLatch releaseSecondBlockedPublish = new CountDownLatch(1);
        CountDownLatch firstBlockedPublishReturned = new CountDownLatch(1);
        CountDownLatch secondBlockedPublishReturned = new CountDownLatch(1);
        CountDownLatch discards = new CountDownLatch(5);
        AtomicInteger activePublishes = new AtomicInteger();
        PublishAction discard =
                () -> {
                    if (activePublishes.get() != 0) {
                        throw new IOException("Abort overlapped an accepted publish");
                    }
                    discards.countDown();
                };
        ProbeCommitter failing =
                new ProbeCommitter(
                        new Path(p0, "data-fail.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            acceptedPublishesStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        acceptedPublishesStarted,
                                        "three accepted publishes to start");
                                publishExecutor.markCurrentTaskForCompletion();
                                throw new IOException("publish failed");
                            } finally {
                                activePublishes.decrementAndGet();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter firstBlocked =
                new ProbeCommitter(
                        new Path(p1, "data-blocked-first.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            acceptedPublishesStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        acceptedPublishesStarted,
                                        "three accepted publishes to start");
                                awaitPublishLatch(
                                        releaseFirstBlockedPublish,
                                        "first blocked publish release");
                            } finally {
                                activePublishes.decrementAndGet();
                                firstBlockedPublishReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter secondBlocked =
                new ProbeCommitter(
                        new Path(p2, "data-blocked-second.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            acceptedPublishesStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        acceptedPublishesStarted,
                                        "three accepted publishes to start");
                                awaitPublishLatch(
                                        releaseSecondBlockedPublish,
                                        "second blocked publish release");
                            } finally {
                                activePublishes.decrementAndGet();
                                secondBlockedPublishReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter samePartitionPending =
                new ProbeCommitter(
                        new Path(p1, "data-must-not-start.csv"),
                        () -> {
                            throw new IOException("same-partition refill ran after failure");
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter otherPartitionPending =
                new ProbeCommitter(
                        new Path(p3, "data-must-not-start.csv"),
                        () -> {
                            throw new IOException("new partition ran after failure");
                        },
                        PublishAction.NOOP,
                        discard);
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(failing),
                        new TwoPhaseCommitMessage(firstBlocked),
                        new TwoPhaseCommitMessage(samePartitionPending),
                        new TwoPhaseCommitMessage(secondBlocked),
                        new TwoPhaseCommitMessage(otherPartitionPending));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 3, publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(acceptedPublishesStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(publishExecutor.awaitSelectedTaskCompletion()).isTrue();
            assertThat(samePartitionPending.commitCalls()).isZero();
            assertThat(otherPartitionPending.commitCalls()).isZero();
            assertThat(discards.getCount()).isEqualTo(5);

            releaseFirstBlockedPublish.countDown();
            assertThat(firstBlockedPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(result.isDone()).isFalse();
            assertThat(discards.getCount()).isEqualTo(5);
            assertThat(samePartitionPending.commitCalls()).isZero();
            assertThat(otherPartitionPending.commitCalls()).isZero();

            releaseSecondBlockedPublish.countDown();
            assertThat(secondBlockedPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("publish failed");

            assertThat(discards.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(0);
            assertThat(samePartitionPending.commitCalls()).isZero();
            assertThat(otherPartitionPending.commitCalls()).isZero();
            assertThat(failing.cleanCalls()).isZero();
            assertThat(firstBlocked.cleanCalls()).isZero();
            assertThat(secondBlocked.cleanCalls()).isZero();
            assertThat(samePartitionPending.cleanCalls()).isZero();
            assertThat(otherPartitionPending.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseFirstBlockedPublish.countDown();
            releaseSecondBlockedPublish.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testCallerInterruptStopsPublishRefillDrainsAndRestoresFlag() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        CountDownLatch acceptedPublishesStarted = new CountDownLatch(2);
        CountDownLatch releaseFirstPublish = new CountDownLatch(1);
        CountDownLatch releaseSecondPublish = new CountDownLatch(1);
        CountDownLatch firstPublishReturned = new CountDownLatch(1);
        CountDownLatch secondPublishReturned = new CountDownLatch(1);
        CountDownLatch callerReturned = new CountDownLatch(1);
        CountDownLatch discards = new CountDownLatch(3);
        AtomicInteger activePublishes = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        PublishAction discard =
                () -> {
                    if (activePublishes.get() != 0) {
                        throw new IOException("Abort overlapped an accepted publish");
                    }
                    discards.countDown();
                };
        ProbeCommitter first =
                blockingPublishCommitter(
                        new Path(tablePath, "part=p0/data-first.csv"),
                        acceptedPublishesStarted,
                        releaseFirstPublish,
                        firstPublishReturned,
                        activePublishes,
                        discard);
        ProbeCommitter second =
                blockingPublishCommitter(
                        new Path(tablePath, "part=p1/data-second.csv"),
                        acceptedPublishesStarted,
                        releaseSecondPublish,
                        secondPublishReturned,
                        activePublishes,
                        discard);
        ProbeCommitter pending =
                new ProbeCommitter(
                        new Path(tablePath, "part=p2/data-must-not-start.csv"),
                        () -> {
                            throw new IOException("Publish refilled after caller interruption");
                        },
                        PublishAction.NOOP,
                        discard);
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(first),
                        new TwoPhaseCommitMessage(second),
                        new TwoPhaseCommitMessage(pending));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        ExecutorService publishExecutor = Executors.newFixedThreadPool(2);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        Thread caller =
                new Thread(
                        () -> {
                            try {
                                commit.commit(messages);
                            } catch (Throwable t) {
                                failure.set(t);
                            } finally {
                                interruptRestored.set(Thread.currentThread().isInterrupted());
                                callerReturned.countDown();
                            }
                        },
                        "format-publish-interrupted-caller");

        caller.start();
        try {
            assertThat(acceptedPublishesStarted.await(10, TimeUnit.SECONDS)).isTrue();
            caller.interrupt();

            releaseFirstPublish.countDown();
            assertThat(firstPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(callerReturned.getCount()).isOne();
            assertThat(discards.getCount()).isEqualTo(3);
            assertThat(pending.commitCalls()).isZero();

            releaseSecondPublish.countDown();
            assertThat(secondPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(callerReturned.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(failure.get()).isNotNull();
            assertThat(failureTree(failure.get())).anyMatch(InterruptedException.class::isInstance);
            assertThat(interruptRestored).isTrue();
            assertThat(pending.commitCalls()).isZero();
            assertThat(discards.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(0);
            assertThat(first.cleanCalls()).isZero();
            assertThat(second.cleanCalls()).isZero();
            assertThat(pending.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseFirstPublish.countDown();
            releaseSecondPublish.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(10));
            publishExecutor.shutdownNow();
        }
        assertThat(caller.isAlive()).isFalse();

        assertPendingInterruptStopsSuccessRefill(new Path(tablePath, "completion-interrupt-race"));
    }

    private void assertPendingInterruptStopsSuccessRefill(Path tablePath) throws Exception {
        FileIO fileIO = LocalFileIO.create();
        CountDownLatch secondPublishStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondPublish = new CountDownLatch(1);
        CountDownLatch callerReturned = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<PendingInterruptThread> callerThread = new AtomicReference<>();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        ProbeCommitter first =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-first.csv"),
                        () -> callerThread.get().signalPendingInterrupt());
        ProbeCommitter second =
                new ProbeCommitter(
                        new Path(tablePath, "part=p1/data-second.csv"),
                        () -> {
                            secondPublishStarted.countDown();
                            try {
                                if (!releaseSecondPublish.await(10, TimeUnit.SECONDS)) {
                                    throw new IOException(
                                            "Timed out waiting to release second publish");
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Second publish was interrupted", e);
                            }
                        });
        ProbeCommitter pending =
                new ProbeCommitter(
                        new Path(tablePath, "part=p2/data-must-not-start.csv"),
                        () -> {
                            throw new IOException(
                                    "Publish refilled before pending interrupt check");
                        });
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(first),
                        new TwoPhaseCommitMessage(second),
                        new TwoPhaseCommitMessage(pending));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        ExecutorService publishExecutor = Executors.newFixedThreadPool(2);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        PendingInterruptThread caller =
                new PendingInterruptThread(
                        () -> {
                            try {
                                commit.commit(messages);
                            } catch (Throwable t) {
                                failure.set(t);
                            } finally {
                                interruptRestored.set(Thread.currentThread().isInterrupted());
                                callerReturned.countDown();
                            }
                        },
                        "format-publish-pending-interrupt-caller");
        callerThread.set(caller);

        caller.start();
        try {
            assertThat(secondPublishStarted.await(10, TimeUnit.SECONDS)).isTrue();
            releaseSecondPublish.countDown();
            assertThat(callerReturned.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(failure.get()).isNotNull();
            assertThat(failureTree(failure.get())).anyMatch(InterruptedException.class::isInstance);
            assertThat(interruptRestored).isTrue();
            assertThat(pending.commitCalls()).isZero();
            assertThat(first.cleanCalls()).isZero();
            assertThat(second.cleanCalls()).isZero();
            assertThat(pending.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseSecondPublish.countDown();
            caller.interrupt();
            caller.join(TimeUnit.SECONDS.toMillis(10));
            publishExecutor.shutdownNow();
        }
        assertThat(caller.isAlive()).isFalse();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testSuccessfulPublishBarrierKeepsCleanStatisticsAndCatalogOnCaller() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path p0 = new Path(tablePath, "part=p0");
        Path p1 = new Path(tablePath, "part=p1");
        CountDownLatch p0PublishesReturned = new CountDownLatch(2);
        CountDownLatch p1PublishStarted = new CountDownLatch(1);
        CountDownLatch releaseP1Publish = new CountDownLatch(1);
        AtomicInteger activePublishes = new AtomicInteger();
        AtomicReference<Thread> callerThread = new AtomicReference<>();
        ConcurrentLinkedQueue<Thread> cleanThreads = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Thread> catalogThreads = new ConcurrentLinkedQueue<>();
        PublishAction clean =
                () -> {
                    if (activePublishes.get() != 0) {
                        throw new IOException("Clean overlapped a publish");
                    }
                    cleanThreads.add(Thread.currentThread());
                };
        ProbeCommitter p0First =
                new ProbeCommitter(
                        new Path(p0, "data-0.csv"),
                        p0PublishesReturned::countDown,
                        clean,
                        PublishAction.NOOP);
        ProbeCommitter p0Second =
                new ProbeCommitter(
                        new Path(p0, "data-1.csv"),
                        p0PublishesReturned::countDown,
                        clean,
                        PublishAction.NOOP);
        ProbeCommitter p1Only =
                new ProbeCommitter(
                        new Path(p1, "data-0.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            p1PublishStarted.countDown();
                            try {
                                awaitPublishLatch(releaseP1Publish, "p1 publish release");
                            } finally {
                                activePublishes.decrementAndGet();
                            }
                        },
                        clean,
                        PublishAction.NOOP);
        TrackingTwoPhaseCommitMessage p0FirstMessage =
                new TrackingTwoPhaseCommitMessage(p0First, 3, 30);
        TrackingTwoPhaseCommitMessage p0SecondMessage =
                new TrackingTwoPhaseCommitMessage(p0Second, 4, 40);
        TrackingTwoPhaseCommitMessage p1OnlyMessage =
                new TrackingTwoPhaseCommitMessage(p1Only, 5, 50);
        List<TrackingTwoPhaseCommitMessage> trackedMessages =
                Arrays.asList(p0FirstMessage, p0SecondMessage, p1OnlyMessage);
        List<CommitMessage> messages = new ArrayList<>(trackedMessages);
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        doAnswer(
                        invocation -> {
                            if (activePublishes.get() != 0) {
                                throw new AssertionError("Catalog update overlapped a publish");
                            }
                            catalogThreads.add(Thread.currentThread());
                            return null;
                        })
                .when(partitionManager)
                .createPartitions(anyList(), eq(true), anyList(), eq(false));
        ExecutorService publishExecutor = Executors.newFixedThreadPool(2);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result =
                    caller.submit(
                            () -> {
                                callerThread.set(Thread.currentThread());
                                commit.commit(messages);
                            });

            assertThat(p1PublishStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(p0PublishesReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(1);
            assertThat(result.isDone()).isFalse();
            assertThat(p0First.cleanCalls()).isZero();
            assertThat(p0Second.cleanCalls()).isZero();
            assertThat(p1Only.cleanCalls()).isZero();
            assertThat(catalogThreads).isEmpty();
            assertThat(trackedMessages)
                    .allSatisfy(
                            message -> {
                                assertThat(message.recordCountCalls()).isZero();
                                assertThat(message.fileSizeCalls()).isZero();
                            });

            releaseP1Publish.countDown();
            result.get(10, TimeUnit.SECONDS);

            assertThat(cleanThreads).hasSize(3).containsOnly(callerThread.get());
            assertThat(catalogThreads).containsOnly(callerThread.get());
            assertThat(trackedMessages)
                    .allSatisfy(
                            message -> {
                                assertThat(message.recordCountCalls()).isOne();
                                assertThat(message.fileSizeCalls()).isOne();
                                assertThat(message.statisticsAccessThreads())
                                        .containsOnly(callerThread.get());
                            });
            ArgumentCaptor<List<Map<String, String>>> specs =
                    ArgumentCaptor.forClass((Class) List.class);
            ArgumentCaptor<List<PartitionStatistics>> statistics =
                    ArgumentCaptor.forClass((Class) List.class);
            verify(partitionManager)
                    .createPartitions(specs.capture(), eq(true), statistics.capture(), eq(false));
            assertThat(specs.getValue())
                    .containsExactlyInAnyOrder(
                            Collections.singletonMap("part", "p0"),
                            Collections.singletonMap("part", "p1"));
            PartitionStatistics p0Statistics =
                    statistics.getValue().stream()
                            .filter(stat -> "p0".equals(stat.spec().get("part")))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            PartitionStatistics p1Statistics =
                    statistics.getValue().stream()
                            .filter(stat -> "p1".equals(stat.spec().get("part")))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertThat(p0Statistics.recordCount()).isEqualTo(7);
            assertThat(p0Statistics.fileSizeInBytes()).isEqualTo(70);
            assertThat(p0Statistics.fileCount()).isEqualTo(2);
            assertThat(p1Statistics.recordCount()).isEqualTo(5);
            assertThat(p1Statistics.fileSizeInBytes()).isEqualTo(50);
            assertThat(p1Statistics.fileCount()).isOne();
        } finally {
            releaseP1Publish.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testPublishPropagatesAndRestoresTcclAndInitializesFileIoOnCaller() throws Exception {
        FirstAccessTrackingFileIO fileIO = new FirstAccessTrackingFileIO();
        Path tablePath = new Path(tempDir.toUri());
        PublishExecutorTracker publishExecutor =
                new PublishExecutorTracker(Executors.newFixedThreadPool(2));
        ClassLoader workerLoader = new ClassLoader(null) {};
        ClassLoader callerLoader = new ClassLoader(null) {};
        setContextClassLoaderOnWorkers(publishExecutor, 2, workerLoader);
        ConcurrentLinkedQueue<ClassLoader> observedLoaders = new ConcurrentLinkedQueue<>();
        PublishAction successfulPublish =
                () -> {
                    observedLoaders.add(Thread.currentThread().getContextClassLoader());
                    fileIO.exists(tablePath);
                };
        PublishAction failingPublish =
                () -> {
                    observedLoaders.add(Thread.currentThread().getContextClassLoader());
                    fileIO.exists(tablePath);
                    throw new IOException("tccl publish failure");
                };
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(
                                        new Path(tablePath, "part=p0/data-0.csv"),
                                        successfulPublish)),
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(
                                        new Path(tablePath, "part=p1/data-0.csv"),
                                        failingPublish)));
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        false,
                        null,
                        1,
                        2,
                        publishExecutor);
        Thread caller = Thread.currentThread();
        ClassLoader previousCallerLoader = caller.getContextClassLoader();
        try {
            caller.setContextClassLoader(callerLoader);
            publishExecutor.armFileIoInitializationCheck(fileIO, caller);

            assertThatThrownBy(() -> commit.commit(messages))
                    .hasRootCauseMessage("tccl publish failure");

            assertThat(fileIO.firstAccessThread()).isSameAs(caller);
            assertThat(publishExecutor.fileIoAcceptanceChecks()).isEqualTo(2);
            assertThat(observedLoaders).hasSize(2).containsOnly(callerLoader);
            assertThat(caller.getContextClassLoader()).isSameAs(callerLoader);
            publishExecutor.disarmFileIoInitializationCheck();
            assertThat(observeContextClassLoaders(publishExecutor, 2)).containsOnly(workerLoader);
        } finally {
            caller.setContextClassLoader(previousCallerLoader);
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testPublishFailureUsesLowestInputIndexAndSuppressesLaterAndAbortFailures()
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        PublishExecutorTracker publishExecutor =
                new PublishExecutorTracker(Executors.newFixedThreadPool(2));
        CountDownLatch bothPublishesStarted = new CountDownLatch(2);
        CountDownLatch releaseLowerIndexFailure = new CountDownLatch(1);
        ProbeCommitter lowerIndex =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-low-index.csv"),
                        () -> {
                            bothPublishesStarted.countDown();
                            awaitPublishLatch(
                                    bothPublishesStarted, "both failing publishes to start");
                            awaitPublishLatch(
                                    releaseLowerIndexFailure, "lower-index failure release");
                            throw new IOException("lower-index publish failure");
                        });
        ProbeCommitter higherIndex =
                new ProbeCommitter(
                        new Path(tablePath, "part=p1/data-high-index.csv"),
                        () -> {
                            bothPublishesStarted.countDown();
                            awaitPublishLatch(
                                    bothPublishesStarted, "both failing publishes to start");
                            publishExecutor.markCurrentTaskForCompletion();
                            throw new IOException("higher-index publish failure");
                        },
                        PublishAction.NOOP,
                        () -> {
                            throw new IOException("abort failed after publish failure");
                        });
        ProbeCommitter afterAbortFailure =
                new ProbeCommitter(
                        new Path(tablePath, "part=p2/data-after-abort-failure.csv"),
                        () -> {
                            throw new IOException("Publish refilled after failure");
                        });
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(lowerIndex),
                        new TwoPhaseCommitMessage(higherIndex),
                        new TwoPhaseCommitMessage(afterAbortFailure));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(publishExecutor.awaitSelectedTaskCompletion()).isTrue();
            releaseLowerIndexFailure.countDown();
            ExecutionException failure = awaitFailure(result);
            Throwable primary = rootCause(failure);

            assertThat(primary).isInstanceOf(IOException.class);
            assertThat(primary).hasMessage("lower-index publish failure");
            assertThat(primary.getSuppressed()).hasSize(2);
            assertThat(failureTree(primary.getSuppressed()[0]))
                    .extracting(Throwable::getMessage)
                    .contains("higher-index publish failure");
            assertThat(failureTree(primary.getSuppressed()[1]))
                    .extracting(Throwable::getMessage)
                    .contains("abort failed after publish failure");
            assertThat(lowerIndex.discardCalls()).isOne();
            assertThat(higherIndex.discardCalls()).isOne();
            assertThat(afterAbortFailure.discardCalls()).isOne();
            assertThat(afterAbortFailure.commitCalls()).isZero();
            assertThat(lowerIndex.cleanCalls()).isZero();
            assertThat(higherIndex.cleanCalls()).isZero();
            assertThat(afterAbortFailure.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseLowerIndexFailure.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testSharedPublishExecutorLetsSmallCommitRunBeforeLargeCommitRefill() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        CountDownLatch largeFirstWaveStarted = new CountDownLatch(64);
        CountDownLatch releaseOneLargePublish = new CountDownLatch(1);
        CountDownLatch releaseRemainingLargePublishes = new CountDownLatch(1);
        CountDownLatch largeRefillStarted = new CountDownLatch(1);
        CountDownLatch smallPublishStarted = new CountDownLatch(1);
        CountDownLatch releaseSmallPublish = new CountDownLatch(1);
        CountDownLatch largeCallerReturned = new CountDownLatch(1);
        CountDownLatch smallCallerReturned = new CountDownLatch(1);
        AtomicReference<Throwable> largeFailure = new AtomicReference<>();
        AtomicReference<Throwable> smallFailure = new AtomicReference<>();
        SubmitterTrackingExecutor publishExecutor =
                new SubmitterTrackingExecutor(Executors.newFixedThreadPool(64));
        List<CommitMessage> largeMessages = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            int index = i;
            largeMessages.add(
                    new TwoPhaseCommitMessage(
                            new ProbeCommitter(
                                    new Path(
                                            tablePath,
                                            "part=large-" + index + "/data-" + index + ".csv"),
                                    () -> {
                                        if (index < 64) {
                                            largeFirstWaveStarted.countDown();
                                            awaitPublishLatch(
                                                    largeFirstWaveStarted,
                                                    "large publish first wave");
                                            awaitPublishLatch(
                                                    index == 0
                                                            ? releaseOneLargePublish
                                                            : releaseRemainingLargePublishes,
                                                    "large publish release");
                                        } else {
                                            largeRefillStarted.countDown();
                                            awaitPublishLatch(
                                                    releaseRemainingLargePublishes,
                                                    "large refill release");
                                        }
                                    })));
        }
        List<CommitMessage> smallMessages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(
                                        new Path(tablePath, "part=small-0/data-0.csv"),
                                        () -> {
                                            smallPublishStarted.countDown();
                                            awaitPublishLatch(
                                                    releaseSmallPublish,
                                                    "small publish fairness release");
                                        })),
                        new TwoPhaseCommitMessage(
                                new ProbeCommitter(
                                        new Path(tablePath, "part=small-1/data-0.csv"),
                                        PublishAction.NOOP)));
        FormatTableCommit largeCommit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        false,
                        null,
                        1,
                        64,
                        publishExecutor);
        FormatTableCommit smallCommit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        false,
                        null,
                        1,
                        64,
                        publishExecutor);
        Thread largeCaller =
                commitCaller(
                        "format-publish-large-caller",
                        largeCommit,
                        largeMessages,
                        largeFailure,
                        largeCallerReturned);
        Thread smallCaller =
                commitCaller(
                        "format-publish-small-caller",
                        smallCommit,
                        smallMessages,
                        smallFailure,
                        smallCallerReturned);
        publishExecutor.trackSubmissionsFrom(smallCaller);

        largeCaller.start();
        try {
            assertThat(largeFirstWaveStarted.await(10, TimeUnit.SECONDS)).isTrue();
            smallCaller.start();
            assertThat(publishExecutor.awaitTrackedSubmission()).isTrue();

            releaseOneLargePublish.countDown();
            assertThat(smallPublishStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(largeRefillStarted.getCount()).isOne();

            releaseSmallPublish.countDown();
            releaseRemainingLargePublishes.countDown();
            assertThat(largeCallerReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(smallCallerReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(largeFailure.get()).isNull();
            assertThat(smallFailure.get()).isNull();
        } finally {
            releaseOneLargePublish.countDown();
            releaseSmallPublish.countDown();
            releaseRemainingLargePublishes.countDown();
            largeCaller.interrupt();
            smallCaller.interrupt();
            largeCaller.join(TimeUnit.SECONDS.toMillis(10));
            smallCaller.join(TimeUnit.SECONDS.toMillis(10));
            publishExecutor.shutdownNow();
        }
        assertThat(largeCaller.isAlive()).isFalse();
        assertThat(smallCaller.isAlive()).isFalse();
    }

    @Test
    void testPartialExecutorRejectionDrainsAcceptedPublishBeforeAbort() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        CountDownLatch firstPublishStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstPublish = new CountDownLatch(1);
        CountDownLatch firstPublishReturned = new CountDownLatch(1);
        CountDownLatch higherIndexFailureStarted = new CountDownLatch(1);
        CountDownLatch releaseHigherIndexFailure = new CountDownLatch(1);
        CountDownLatch higherIndexFailureReturned = new CountDownLatch(1);
        CountDownLatch discards = new CountDownLatch(4);
        AtomicInteger activePublishes = new AtomicInteger();
        PublishAction discard =
                () -> {
                    if (activePublishes.get() != 0) {
                        throw new IOException("Abort overlapped accepted publish after rejection");
                    }
                    discards.countDown();
                };
        ProbeCommitter first =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-0.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            firstPublishStarted.countDown();
                            try {
                                awaitPublishLatch(releaseFirstPublish, "accepted publish release");
                            } finally {
                                activePublishes.decrementAndGet();
                                firstPublishReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter rejected =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-1.csv"),
                        () -> {
                            throw new IOException("Rejected publish executed");
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter higherIndexFailure =
                new ProbeCommitter(
                        new Path(tablePath, "part=p1/data-0.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            higherIndexFailureStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        releaseHigherIndexFailure,
                                        "higher-index accepted publish failure release");
                                throw new IOException("higher-index accepted publish failed");
                            } finally {
                                activePublishes.decrementAndGet();
                                higherIndexFailureReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter pending =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-2.csv"),
                        () -> {
                            throw new IOException("Publish refilled after rejection");
                        },
                        PublishAction.NOOP,
                        discard);
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(first),
                        new TwoPhaseCommitMessage(rejected),
                        new TwoPhaseCommitMessage(higherIndexFailure),
                        new TwoPhaseCommitMessage(pending));
        RejectThirdSubmissionExecutor publishExecutor =
                new RejectThirdSubmissionExecutor(Executors.newFixedThreadPool(2));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(firstPublishStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(higherIndexFailureStarted.await(10, TimeUnit.SECONDS)).isTrue();
            releaseFirstPublish.countDown();
            assertThat(firstPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(publishExecutor.awaitRejection()).isTrue();
            assertThat(result.isDone()).isFalse();
            assertThat(discards.getCount()).isEqualTo(4);
            assertThat(rejected.commitCalls()).isZero();
            assertThat(pending.commitCalls()).isZero();

            releaseHigherIndexFailure.countDown();
            assertThat(higherIndexFailureReturned.await(10, TimeUnit.SECONDS)).isTrue();
            ExecutionException failure = awaitFailure(result);

            Throwable primary = rootCause(failure);
            assertThat(primary)
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessage("publish submission rejected");
            assertThat(primary.getSuppressed())
                    .singleElement()
                    .satisfies(
                            suppressed ->
                                    assertThat(suppressed)
                                            .isInstanceOf(IOException.class)
                                            .hasMessage("higher-index accepted publish failed"));
            assertThat(publishExecutor.submissionCalls()).isEqualTo(3);
            assertThat(discards.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(0);
            assertThat(first.cleanCalls()).isZero();
            assertThat(rejected.cleanCalls()).isZero();
            assertThat(higherIndexFailure.cleanCalls()).isZero();
            assertThat(pending.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseFirstPublish.countDown();
            releaseHigherIndexFailure.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }

        assertHigherIndexRejectionIsSuppressedByLowerIndexWorkerFailure(
                new Path(tablePath, "symmetric-ordering"));
    }

    private void assertHigherIndexRejectionIsSuppressedByLowerIndexWorkerFailure(Path tablePath)
            throws Exception {
        FileIO fileIO = LocalFileIO.create();
        CountDownLatch acceptedPublishesStarted = new CountDownLatch(2);
        CountDownLatch releaseLowerIndexFailure = new CountDownLatch(1);
        CountDownLatch releaseSuccessfulPublish = new CountDownLatch(1);
        CountDownLatch lowerIndexFailureReturned = new CountDownLatch(1);
        CountDownLatch successfulPublishReturned = new CountDownLatch(1);
        CountDownLatch discards = new CountDownLatch(3);
        AtomicInteger activePublishes = new AtomicInteger();
        PublishAction discard =
                () -> {
                    if (activePublishes.get() != 0) {
                        throw new IOException(
                                "Abort overlapped accepted publish before symmetric rejection drain");
                    }
                    discards.countDown();
                };
        ProbeCommitter lowerIndexFailure =
                new ProbeCommitter(
                        new Path(tablePath, "part=p0/data-0.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            acceptedPublishesStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        acceptedPublishesStarted,
                                        "both symmetric accepted publishes to start");
                                awaitPublishLatch(
                                        releaseLowerIndexFailure,
                                        "lower-index accepted publish failure release");
                                throw new IOException("lower-index accepted publish failed");
                            } finally {
                                activePublishes.decrementAndGet();
                                lowerIndexFailureReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter successful =
                new ProbeCommitter(
                        new Path(tablePath, "part=p1/data-0.csv"),
                        () -> {
                            activePublishes.incrementAndGet();
                            acceptedPublishesStarted.countDown();
                            try {
                                awaitPublishLatch(
                                        acceptedPublishesStarted,
                                        "both symmetric accepted publishes to start");
                                awaitPublishLatch(
                                        releaseSuccessfulPublish,
                                        "successful publish before higher-index rejection");
                            } finally {
                                activePublishes.decrementAndGet();
                                successfulPublishReturned.countDown();
                            }
                        },
                        PublishAction.NOOP,
                        discard);
        ProbeCommitter rejected =
                new ProbeCommitter(
                        new Path(tablePath, "part=p1/data-1.csv"),
                        () -> {
                            throw new IOException("Higher-index rejected publish executed");
                        },
                        PublishAction.NOOP,
                        discard);
        List<CommitMessage> messages =
                Arrays.asList(
                        new TwoPhaseCommitMessage(lowerIndexFailure),
                        new TwoPhaseCommitMessage(successful),
                        new TwoPhaseCommitMessage(rejected));
        RejectThirdSubmissionExecutor publishExecutor =
                new RejectThirdSubmissionExecutor(Executors.newFixedThreadPool(2));
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath, fileIO, partitionManager, false, null, 1, 2, publishExecutor);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<?> result = caller.submit(() -> commit.commit(messages));

            assertThat(acceptedPublishesStarted.await(10, TimeUnit.SECONDS)).isTrue();
            releaseSuccessfulPublish.countDown();
            assertThat(successfulPublishReturned.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(publishExecutor.awaitRejection()).isTrue();
            assertThat(result.isDone()).isFalse();
            assertThat(discards.getCount()).isEqualTo(3);
            assertThat(rejected.commitCalls()).isZero();

            releaseLowerIndexFailure.countDown();
            assertThat(lowerIndexFailureReturned.await(10, TimeUnit.SECONDS)).isTrue();
            ExecutionException failure = awaitFailure(result);

            Throwable primary = rootCause(failure);
            assertThat(primary)
                    .isInstanceOf(IOException.class)
                    .hasMessage("lower-index accepted publish failed");
            assertThat(primary.getSuppressed())
                    .singleElement()
                    .satisfies(
                            suppressed ->
                                    assertThat(suppressed)
                                            .isInstanceOf(RejectedExecutionException.class)
                                            .hasMessage("publish submission rejected"));
            assertThat(publishExecutor.submissionCalls()).isEqualTo(3);
            assertThat(discards.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(activePublishes).hasValue(0);
            assertThat(lowerIndexFailure.cleanCalls()).isZero();
            assertThat(successful.cleanCalls()).isZero();
            assertThat(rejected.cleanCalls()).isZero();
            verify(partitionManager, never())
                    .createPartitions(anyList(), eq(true), any(), anyBoolean());
        } finally {
            releaseLowerIndexFailure.countDown();
            releaseSuccessfulPublish.countDown();
            caller.shutdownNow();
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testSingleTargetPartitionUsesOrderedCallerFastPath() {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        Path partitionPath = new Path(tablePath, "part=p0");
        Thread caller = Thread.currentThread();
        List<String> order = new ArrayList<>();
        ConcurrentLinkedQueue<Thread> publishingThreads = new ConcurrentLinkedQueue<>();
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int index = i;
            messages.add(
                    new TwoPhaseCommitMessage(
                            new ProbeCommitter(
                                    new Path(partitionPath, "data-" + index + ".csv"),
                                    () -> {
                                        order.add("file-" + index);
                                        publishingThreads.add(Thread.currentThread());
                                    })));
        }
        RejectAllExecutor publishExecutor = new RejectAllExecutor();
        FormatTableCommit commit =
                newPublishCommit(
                        tablePath,
                        fileIO,
                        mock(FormatTablePartitionManager.class),
                        false,
                        null,
                        1,
                        64,
                        publishExecutor);
        try {
            commit.commit(messages);

            assertThat(order).containsExactly("file-0", "file-1", "file-2");
            assertThat(publishingThreads).hasSize(3).containsOnly(caller);
            assertThat(publishExecutor.submissionCalls()).isZero();
        } finally {
            publishExecutor.shutdownNow();
        }
    }

    @Test
    void testDirectPublishConcurrencyRejectsValuesOutsideSupportedRange() {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        ExecutorService publishExecutor = Executors.newSingleThreadExecutor();
        FormatTablePartitionManager partitionManager = mock(FormatTablePartitionManager.class);
        try {
            assertThatThrownBy(
                            () ->
                                    newPublishCommit(
                                            tablePath,
                                            fileIO,
                                            partitionManager,
                                            false,
                                            null,
                                            1,
                                            -1,
                                            publishExecutor))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Format Table publish thread number must be between 1 and 64, but was -1.");
            assertThatThrownBy(
                            () ->
                                    newPublishCommit(
                                            tablePath,
                                            fileIO,
                                            partitionManager,
                                            false,
                                            null,
                                            1,
                                            0,
                                            publishExecutor))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Format Table publish thread number must be between 1 and 64, but was 0.");
            assertThatThrownBy(
                            () ->
                                    newPublishCommit(
                                            tablePath,
                                            fileIO,
                                            partitionManager,
                                            false,
                                            null,
                                            1,
                                            65,
                                            publishExecutor))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            "Format Table publish thread number must be between 1 and 64, but was 65.");
        } finally {
            publishExecutor.shutdownNow();
        }
    }

    private FormatTableCommit builderAppendCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options) {
        return (FormatTableCommit)
                formatTable(tablePath, fileIO, partitionManager, options)
                        .newBatchWriteBuilder()
                        .newCommit();
    }

    private FormatTable formatTable(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options) {
        RowType rowType =
                RowType.builder()
                        .field("part", DataTypes.STRING())
                        .field("id", DataTypes.INT())
                        .build();
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("cleanup_db", "cleanup_table"))
                        .rowType(rowType)
                        .partitionKeys(Collections.singletonList("part"))
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .partitionManager(partitionManager)
                        .build();
        return table;
    }

    private FormatTableCommit builderUnpartitionedAppendCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            Map<String, String> options) {
        FormatTable table =
                FormatTable.builder()
                        .fileIO(fileIO)
                        .identifier(Identifier.create("publish_db", "unpartitioned_publish_table"))
                        .rowType(RowType.builder().field("id", DataTypes.INT()).build())
                        .partitionKeys(Collections.emptyList())
                        .location(tablePath.toString())
                        .format(FormatTable.Format.CSV)
                        .options(options)
                        .partitionManager(partitionManager)
                        .build();
        return (FormatTableCommit) table.newBatchWriteBuilder().newCommit();
    }

    private FormatTableCommit newPublishCommit(
            Path tablePath,
            FileIO fileIO,
            FormatTablePartitionManager partitionManager,
            boolean overwrite,
            Map<String, String> staticPartition,
            int cleanupThreadNum,
            int publishThreadNum,
            ExecutorService publishExecutor) {
        return new FormatTableCommit(
                tablePath.toString(),
                Collections.singletonList("part"),
                fileIO,
                false,
                PARTITION_DEFAULT_NAME.defaultValue(),
                overwrite,
                Identifier.create("publish_db", "publish_table"),
                staticPartition,
                null,
                null,
                partitionManager,
                /* dynamicPartitionOverwrite */ true,
                cleanupThreadNum,
                publishThreadNum,
                publishExecutor);
    }

    private static ProbeCommitter blockingPublishCommitter(
            Path targetPath,
            CountDownLatch acceptedPublishesStarted,
            CountDownLatch releasePublish,
            CountDownLatch publishReturned,
            AtomicInteger activePublishes,
            PublishAction discard) {
        return new ProbeCommitter(
                targetPath,
                () -> {
                    activePublishes.incrementAndGet();
                    acceptedPublishesStarted.countDown();
                    try {
                        awaitPublishLatch(
                                acceptedPublishesStarted, "all accepted publishes to start");
                        awaitPublishLatch(releasePublish, "accepted publish release");
                    } finally {
                        activePublishes.decrementAndGet();
                        publishReturned.countDown();
                    }
                },
                PublishAction.NOOP,
                discard);
    }

    private static Thread commitCaller(
            String name,
            FormatTableCommit commit,
            List<CommitMessage> messages,
            AtomicReference<Throwable> failure,
            CountDownLatch returned) {
        return new Thread(
                () -> {
                    try {
                        commit.commit(messages);
                    } catch (Throwable t) {
                        failure.set(t);
                    } finally {
                        returned.countDown();
                    }
                },
                name);
    }

    private static List<CommitMessage> publishMessages(
            Path tablePath, ConcurrentLinkedQueue<Thread> publishingThreads, boolean partitioned) {
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path parent = partitioned ? new Path(tablePath, "part=p" + i) : tablePath;
            messages.add(
                    new TwoPhaseCommitMessage(
                            new ProbeCommitter(
                                    new Path(parent, "data-" + i + ".csv"),
                                    () -> publishingThreads.add(Thread.currentThread()))));
        }
        return messages;
    }

    private static void awaitPublishLatch(CountDownLatch latch, String description)
            throws IOException {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for " + description);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for " + description, e);
        }
    }

    private static void setContextClassLoaderOnWorkers(
            ExecutorService executor, int workerCount, ClassLoader classLoader) throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(workerCount);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            futures.add(
                    executor.submit(
                            () -> {
                                Thread.currentThread().setContextClassLoader(classLoader);
                                workersStarted.countDown();
                                try {
                                    if (!releaseWorkers.await(10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "Timed out initializing publish executor workers");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(
                                            "Interrupted while initializing publish executor workers",
                                            e);
                                }
                            }));
        }
        try {
            assertThat(workersStarted.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            releaseWorkers.countDown();
        }
        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    private static final class PublishExecutorTracker extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final ThreadLocal<Boolean> selectedTask =
                ThreadLocal.withInitial(() -> Boolean.FALSE);
        private final CountDownLatch selectedTaskCompletion = new CountDownLatch(1);
        private final AtomicReference<FirstAccessTrackingFileIO> trackedFileIO =
                new AtomicReference<>();
        private final AtomicReference<Thread> expectedInitializationThread =
                new AtomicReference<>();
        private final AtomicInteger fileIoAcceptanceChecks = new AtomicInteger();

        private PublishExecutorTracker(ExecutorService delegate) {
            this.delegate = delegate;
        }

        private void markCurrentTaskForCompletion() {
            selectedTask.set(Boolean.TRUE);
        }

        private boolean awaitSelectedTaskCompletion() throws InterruptedException {
            return selectedTaskCompletion.await(10, TimeUnit.SECONDS);
        }

        private void armFileIoInitializationCheck(
                FirstAccessTrackingFileIO fileIO, Thread expectedThread) {
            if (!trackedFileIO.compareAndSet(null, fileIO)
                    || !expectedInitializationThread.compareAndSet(null, expectedThread)) {
                throw new IllegalStateException("FileIO initialization tracking is already armed.");
            }
        }

        private void disarmFileIoInitializationCheck() {
            trackedFileIO.set(null);
            expectedInitializationThread.set(null);
        }

        private int fileIoAcceptanceChecks() {
            return fileIoAcceptanceChecks.get();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            Thread expectedThread = expectedInitializationThread.get();
            if (expectedThread != null) {
                FirstAccessTrackingFileIO fileIO = trackedFileIO.get();
                if (fileIO == null || fileIO.firstAccessThread() != expectedThread) {
                    throw new AssertionError(
                            "Publish task accepted before FileIO initialization on the caller");
                }
            }
            delegate.execute(
                    () -> {
                        try {
                            command.run();
                        } finally {
                            if (selectedTask.get()) {
                                selectedTaskCompletion.countDown();
                            }
                            selectedTask.remove();
                        }
                    });
            if (expectedThread != null) {
                fileIoAcceptanceChecks.incrementAndGet();
            }
        }
    }

    private static final class SubmitterTrackingExecutor extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final AtomicReference<Thread> trackedSubmitter = new AtomicReference<>();
        private final CountDownLatch trackedSubmission = new CountDownLatch(1);

        private SubmitterTrackingExecutor(ExecutorService delegate) {
            this.delegate = delegate;
        }

        private void trackSubmissionsFrom(Thread submitter) {
            if (!trackedSubmitter.compareAndSet(null, submitter)) {
                throw new IllegalStateException("A publish submitter is already being tracked.");
            }
        }

        private boolean awaitTrackedSubmission() throws InterruptedException {
            return trackedSubmission.await(10, TimeUnit.SECONDS);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
            if (Thread.currentThread() == trackedSubmitter.get()) {
                trackedSubmission.countDown();
            }
        }
    }

    private static final class RejectThirdSubmissionExecutor extends AbstractExecutorService {

        private final ExecutorService delegate;
        private final AtomicInteger submissionCalls = new AtomicInteger();
        private final CountDownLatch rejection = new CountDownLatch(1);

        private RejectThirdSubmissionExecutor(ExecutorService delegate) {
            this.delegate = delegate;
        }

        private boolean awaitRejection() throws InterruptedException {
            return rejection.await(10, TimeUnit.SECONDS);
        }

        private int submissionCalls() {
            return submissionCalls.get();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            int call = submissionCalls.incrementAndGet();
            if (call >= 3) {
                rejection.countDown();
                throw new RejectedExecutionException("publish submission rejected");
            }
            delegate.execute(command);
        }
    }

    private static final class RejectAllExecutor extends AbstractExecutorService {

        private final AtomicBoolean shutdown = new AtomicBoolean();
        private final AtomicInteger submissionCalls = new AtomicInteger();

        private int submissionCalls() {
            return submissionCalls.get();
        }

        @Override
        public void shutdown() {
            shutdown.set(true);
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown.set(true);
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown() {
            return shutdown.get();
        }

        @Override
        public boolean isTerminated() {
            return shutdown.get();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown.get();
        }

        @Override
        public void execute(Runnable command) {
            submissionCalls.incrementAndGet();
            throw new AssertionError("Single-target publish submitted to the worker executor");
        }
    }

    @FunctionalInterface
    private interface PublishAction {

        PublishAction NOOP = () -> {};

        void run() throws IOException;
    }

    private static class ProbeCommitter implements TwoPhaseOutputStream.Committer {

        private static final long serialVersionUID = 1L;

        private final Path targetPath;
        private final PublishAction publish;
        private final PublishAction clean;
        private final PublishAction discard;
        private final AtomicInteger commitCalls = new AtomicInteger();
        private final AtomicInteger cleanCalls = new AtomicInteger();
        private final AtomicInteger discardCalls = new AtomicInteger();

        private ProbeCommitter(Path targetPath, PublishAction publish) {
            this(targetPath, publish, PublishAction.NOOP, PublishAction.NOOP);
        }

        private ProbeCommitter(
                Path targetPath,
                PublishAction publish,
                PublishAction clean,
                PublishAction discard) {
            this.targetPath = targetPath;
            this.publish = publish;
            this.clean = clean;
            this.discard = discard;
        }

        @Override
        public void commit(FileIO fileIO) throws IOException {
            commitCalls.incrementAndGet();
            publish.run();
        }

        @Override
        public void discard(FileIO fileIO) throws IOException {
            discardCalls.incrementAndGet();
            discard.run();
        }

        @Override
        public Path targetPath() {
            return targetPath;
        }

        @Override
        public void clean(FileIO fileIO) throws IOException {
            cleanCalls.incrementAndGet();
            clean.run();
        }

        private int commitCalls() {
            return commitCalls.get();
        }

        private int cleanCalls() {
            return cleanCalls.get();
        }

        private int discardCalls() {
            return discardCalls.get();
        }
    }

    private static final class PendingInterruptThread extends Thread {

        private final AtomicBoolean pendingInterrupt = new AtomicBoolean();

        private PendingInterruptThread(Runnable target, String name) {
            super(target, name);
        }

        private void signalPendingInterrupt() {
            pendingInterrupt.set(true);
        }

        @Override
        public boolean isInterrupted() {
            return pendingInterrupt.getAndSet(false) || super.isInterrupted();
        }
    }

    private static class TrackingTwoPhaseCommitMessage extends TwoPhaseCommitMessage {

        private static final long serialVersionUID = 1L;

        private final AtomicInteger recordCountCalls = new AtomicInteger();
        private final AtomicInteger fileSizeCalls = new AtomicInteger();
        private final ConcurrentLinkedQueue<Thread> statisticsAccessThreads =
                new ConcurrentLinkedQueue<>();

        private TrackingTwoPhaseCommitMessage(
                TwoPhaseOutputStream.Committer committer, long recordCount, long fileSizeInBytes) {
            super(committer, recordCount, fileSizeInBytes);
        }

        @Override
        public long recordCount() {
            recordCountCalls.incrementAndGet();
            statisticsAccessThreads.add(Thread.currentThread());
            return super.recordCount();
        }

        @Override
        public long fileSizeInBytes() {
            fileSizeCalls.incrementAndGet();
            statisticsAccessThreads.add(Thread.currentThread());
            return super.fileSizeInBytes();
        }

        private int recordCountCalls() {
            return recordCountCalls.get();
        }

        private int fileSizeCalls() {
            return fileSizeCalls.get();
        }

        private List<Thread> statisticsAccessThreads() {
            return new ArrayList<>(statisticsAccessThreads);
        }
    }

    private static class ParallelPublishProbe {

        private final int firstWaveSize;
        private final CountDownLatch firstWave;
        private final CountDownLatch releaseFirstWave = new CountDownLatch(1);
        private final CountDownLatch unexpectedExtraPublish = new CountDownLatch(1);
        private final AtomicInteger publishCalls = new AtomicInteger();
        private final AtomicInteger activePublishes = new AtomicInteger();
        private final AtomicInteger maxConcurrentPublishes = new AtomicInteger();

        private ParallelPublishProbe(int firstWaveSize) {
            this.firstWaveSize = firstWaveSize;
            this.firstWave = new CountDownLatch(firstWaveSize);
        }

        private void publish() throws IOException {
            int call = publishCalls.incrementAndGet();
            int active = activePublishes.incrementAndGet();
            maxConcurrentPublishes.updateAndGet(previous -> Math.max(previous, active));
            try {
                if (call <= firstWaveSize) {
                    firstWave.countDown();
                } else {
                    unexpectedExtraPublish.countDown();
                }
                awaitPublishLatch(releaseFirstWave, "publish first-wave release");
            } finally {
                activePublishes.decrementAndGet();
            }
        }

        private boolean awaitFirstWave() throws InterruptedException {
            return firstWave.await(10, TimeUnit.SECONDS);
        }

        private boolean awaitUnexpectedExtraPublish() throws InterruptedException {
            return unexpectedExtraPublish.await(300, TimeUnit.MILLISECONDS);
        }

        private void releaseFirstWave() {
            releaseFirstWave.countDown();
        }

        private int publishCalls() {
            return publishCalls.get();
        }

        private int maxConcurrentPublishes() {
            return maxConcurrentPublishes.get();
        }
    }

    private static class FirstAccessTrackingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final AtomicReference<Thread> firstAccessThread = new AtomicReference<>();

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            recordAccess();
            return super.getFileStatus(path);
        }

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            recordAccess();
            return super.listStatus(path);
        }

        @Override
        public boolean exists(Path path) throws IOException {
            recordAccess();
            return super.exists(path);
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            recordAccess();
            return super.delete(path, recursive);
        }

        @Override
        public boolean mkdirs(Path path) throws IOException {
            recordAccess();
            return super.mkdirs(path);
        }

        @Override
        public boolean rename(Path src, Path dst) throws IOException {
            recordAccess();
            return super.rename(src, dst);
        }

        private void recordAccess() {
            firstAccessThread.compareAndSet(null, Thread.currentThread());
        }

        private Thread firstAccessThread() {
            return firstAccessThread.get();
        }
    }

    private abstract static class SortedLocalFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            Arrays.sort(statuses, Comparator.comparing(status -> status.getPath().toString()));
            return statuses;
        }

        protected static void await(CountDownLatch latch, String description) throws IOException {
            try {
                if (!latch.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for " + description);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for " + description, e);
            }
        }
    }
}
