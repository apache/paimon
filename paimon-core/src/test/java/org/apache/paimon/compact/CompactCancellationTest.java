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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.io.DataFileTestUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the output of a compaction whose result is discarded does not become an orphan file.
 */
public class CompactCancellationTest {

    private static final DataFileMeta INPUT = newFile("input", 0, 0, 10, 1);
    private static final DataFileMeta OUTPUT = newFile("output", 1, 0, 10, 1);
    private static final DataFileMeta CHANGELOG = newFile("changelog", 0, 0, 10, 1);

    private ExecutorService executor;

    @BeforeEach
    public void before() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void after() {
        executor.shutdownNow();
    }

    @Test
    public void testCancelledTaskDeletesItsOutput() throws Exception {
        TestTask task = new TestTask(result -> result.merge(rewriteResult()));
        task.cancel();

        assertThat(task.call().after()).isEmpty();
        assertThat(task.completedResult()).isNull();
        assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
    }

    @Test
    public void testFailedTaskDeletesOutputOfFinishedSteps() {
        TestTask task =
                new TestTask(
                        result -> {
                            result.merge(rewriteResult());
                            throw new IllegalStateException("failed in a later step");
                        });

        assertThatThrownBy(task::call).hasMessageContaining("failed in a later step");
        assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
    }

    @Test
    public void testUpgradedFileIsNotDeleted() {
        // an upgraded file is the very same physical file as its input, it is still required by
        // previous snapshots
        TestTask task =
                new TestTask(
                        result -> {
                            result.merge(new CompactResult(INPUT, INPUT.upgrade(3)));
                            throw new IllegalStateException("boom");
                        });

        assertThatThrownBy(task::call).hasMessageContaining("boom");
        assertThat(task.deleted).isEmpty();
    }

    @Test
    public void testDeletionFileIsCleanedUp() throws Exception {
        TestDeletionFile deletionFile = new TestDeletionFile();
        TestTask task = new TestTask(result -> result.setDeletionFile(deletionFile));
        task.cancel();

        task.call();
        assertThat(deletionFile.cleaned).isTrue();
    }

    @Test
    public void testFinishedResultIsNotLostWhenCancellationWinsTheRace() throws Exception {
        TestManager manager = new TestManager();
        TestTask task = new TestTask(result -> result.merge(rewriteResult()));
        manager.submit(task);
        task.awaitExit();

        // the task has finished, but FutureTask dropped its result because cancellation won the
        // race against the completion of the task
        manager.simulateCancellation = true;
        Optional<CompactResult> result = manager.getCompactionResult(true);

        assertThat(result).isPresent();
        assertThat(names(result.get().after())).containsExactly("output");
        // the caller is now aware of the files, the task must not have deleted them
        assertThat(task.deleted).isEmpty();
    }

    @Test
    public void testCancelledTaskCleansUpAfterTheCallerGaveUpOnTheResult() throws Exception {
        TestManager manager = new TestManager();
        CountDownLatch blocked = new CountDownLatch(1);
        TestTask task =
                new TestTask(
                        result -> {
                            result.merge(rewriteResult());
                            blocked.countDown();
                            // interrupted by the cancellation below
                            Thread.sleep(Long.MAX_VALUE);
                        });
        manager.submit(task);
        blocked.await();

        manager.simulateCancellation = true;
        manager.cancelCompaction();

        // the caller cannot wait for a task which may be doing a long piece of CPU work
        assertThat(manager.getCompactionResult(true)).isEmpty();

        // so the task itself is responsible for the files it has produced
        task.awaitExit();
        assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
    }

    @Test
    public void testCancellationArrivingWhenTheTaskIsAboutToPublishItsResult() throws Exception {
        TestManager manager = new TestManager();
        CountDownLatch submitted = new CountDownLatch(1);
        AtomicReference<Optional<CompactResult>> reported = new AtomicReference<>();
        TestTask task =
                new TestTask(
                        result -> {
                            assertThat(submitted.await(1, TimeUnit.MINUTES)).isTrue();
                            result.merge(rewriteResult());
                        });
        // the writer cancels the compaction and gives up on its result in the very moment the
        // task is about to publish it
        task.beforePublish =
                () -> {
                    manager.cancelCompaction();
                    reported.set(manager.getCompactionResult(true));
                };

        manager.submit(task);
        submitted.countDown();
        task.awaitExit();

        // nobody else can account for the files, so the task must have deleted them
        assertThat(reported.get()).isEmpty();
        assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
    }

    @Test
    public void testOutputIsNeverLostWhenCancellationRacesWithCompletion() throws Exception {
        for (int i = 0; i < 500; i++) {
            TestManager manager = new TestManager();
            CountDownLatch started = new CountDownLatch(1);
            int spins = i % 8;
            TestTask task =
                    new TestTask(
                            result -> {
                                started.countDown();
                                // shift the phase between the two threads to hit different
                                // interleavings around the publication of the result
                                for (int j = 0; j < spins; j++) {
                                    Thread.yield();
                                }
                                result.merge(rewriteResult());
                            });
            manager.submit(task);
            // cancelling a task which the executor has not picked up yet keeps it from running
            // at all, so wait until it is really in flight
            assertThat(started.await(1, TimeUnit.MINUTES)).isTrue();

            manager.cancelCompaction();
            Optional<CompactResult> reported = manager.getCompactionResult(true);
            task.awaitExit();

            // whoever ends up owning the files, they are never left behind unnoticed: the
            // caller either learns about them or the task has deleted them itself
            boolean reportedToCaller = reported.isPresent() && !reported.get().after().isEmpty();
            if (reportedToCaller) {
                assertThat(names(reported.get().after())).containsExactly("output");
                assertThat(task.deleted).isEmpty();
            } else {
                assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
            }
        }
    }

    @Test
    public void testCleanupIsNotSkippedByTheCancellationInterrupt() throws Exception {
        TestManager manager = new TestManager();
        CountDownLatch blocked = new CountDownLatch(1);
        TestTask task =
                new TestTask(
                        result -> {
                            result.merge(rewriteResult());
                            blocked.countDown();
                            // a task busy with CPU work observes the interruption without
                            // clearing it, just like a file system whose RPC fails while the
                            // interrupt flag is set
                            long deadline = System.currentTimeMillis() + 60_000;
                            while (!Thread.currentThread().isInterrupted()
                                    && System.currentTimeMillis() < deadline) {
                                Thread.yield();
                            }
                        });
        manager.submit(task);
        assertThat(blocked.await(1, TimeUnit.MINUTES)).isTrue();

        manager.cancelCompaction();
        task.awaitExit();

        // deleting files goes through the file IO, whose calls fail immediately on an
        // interrupted thread, so the flag must be cleared for the cleanup
        assertThat(task.interruptedDuringCleanup).isFalse();
        assertThat(deletedNames(task)).containsExactlyInAnyOrder("output", "changelog");
        // and restored afterwards, the interruption must not be swallowed
        assertThat(task.interruptedAfterCall).isTrue();
    }

    private static CompactResult rewriteResult() {
        return new CompactResult(
                singletonList(INPUT), singletonList(OUTPUT), singletonList(CHANGELOG));
    }

    private static List<String> deletedNames(TestTask task) {
        return names(task.deleted);
    }

    private static List<String> names(List<DataFileMeta> files) {
        return files.stream().map(DataFileMeta::fileName).collect(Collectors.toList());
    }

    /** A body of a {@link CompactTask} which is allowed to fail. */
    @FunctionalInterface
    private interface TaskBody {
        void run(CompactResult produced) throws Exception;
    }

    /** An action running at a given point of a {@link CompactTask}, allowed to fail. */
    @FunctionalInterface
    private interface Hook {
        void run() throws Exception;
    }

    private static class TestTask extends CompactTask {

        private final TaskBody body;
        private final CountDownLatch exited = new CountDownLatch(1);
        private final List<DataFileMeta> deleted = new ArrayList<>();

        @Nullable private volatile Hook beforePublish = null;
        private volatile boolean interruptedDuringCleanup = false;
        private volatile boolean interruptedAfterCall = false;

        private TestTask(TaskBody body) {
            super(null, "");
            this.body = body;
        }

        @Override
        public CompactResult call() throws Exception {
            try {
                return super.call();
            } finally {
                interruptedAfterCall = Thread.currentThread().isInterrupted();
                exited.countDown();
            }
        }

        @Override
        protected boolean publish(CompactResult result) {
            Hook hook = beforePublish;
            if (hook != null) {
                try {
                    hook.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return super.publish(result);
        }

        private void awaitExit() throws InterruptedException {
            assertThat(exited.await(1, TimeUnit.MINUTES)).isTrue();
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            body.run(produced());
            return produced();
        }

        @Override
        protected void deleteProduced(List<DataFileMeta> files) {
            interruptedDuringCleanup = Thread.currentThread().isInterrupted();
            deleted.addAll(files);
        }
    }

    private class TestManager extends CompactFutureManager {

        private volatile boolean simulateCancellation = false;

        private void submit(CompactTask task) {
            submitTask(executor, task);
        }

        @Override
        protected CompactResult obtainCompactResult()
                throws InterruptedException, ExecutionException {
            if (simulateCancellation) {
                throw new CancellationException();
            }
            return super.obtainCompactResult();
        }

        @Override
        public Optional<CompactResult> getCompactionResult(boolean blocking)
                throws ExecutionException, InterruptedException {
            return innerGetCompactionResult(blocking);
        }

        @Override
        public boolean shouldWaitForLatestCompaction() {
            return false;
        }

        @Override
        public boolean shouldWaitForPreparingCheckpoint() {
            return false;
        }

        @Override
        public void addNewFile(DataFileMeta file) {}

        @Override
        public Collection<DataFileMeta> allFiles() {
            return new ArrayList<>();
        }

        @Override
        public void triggerCompaction(boolean fullCompaction) {}

        @Override
        public void close() throws IOException {}
    }

    private static class TestDeletionFile implements CompactDeletionFile {

        private boolean cleaned = false;

        @Override
        public Optional<org.apache.paimon.index.IndexFileMeta> getOrCompute() {
            return Optional.empty();
        }

        @Override
        public CompactDeletionFile mergeOldFile(CompactDeletionFile old) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clean() {
            cleaned = true;
        }
    }
}
