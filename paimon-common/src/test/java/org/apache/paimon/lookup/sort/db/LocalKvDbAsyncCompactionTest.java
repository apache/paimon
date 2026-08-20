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

package org.apache.paimon.lookup.sort.db;

import org.apache.paimon.compression.CompressOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for asynchronous compaction and its synchronous fallback in {@link LocalKvDb}. */
public class LocalKvDbAsyncCompactionTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testAutomaticCompactionRunsInBackground() throws Exception {
        ManuallyTriggeredExecutor compactionExecutor = new ManuallyTriggeredExecutor();
        try (LocalKvDb db = createDb("background", compactionExecutor)) {
            putAndFlush(db, "shared", "v1");
            putAndFlush(db, "shared", "v2");
            putAndFlush(db, "shared", "v3");

            assertThat(compactionExecutor.numQueuedTasks()).isOne();
            assertThat(db.getLevelFileCount(0)).isEqualTo(3);
            assertThat(get(db, "shared")).isEqualTo("v3");
            assertThat(scan(db, "sha", "shb")).containsExactly("shared=v3");

            compactionExecutor.runNext();
            db.awaitCompaction();

            assertThat(db.getLevelFileCount(0)).isZero();
            assertThat(get(db, "shared")).isEqualTo("v3");
            assertThat(scan(db, "sha", "shb")).containsExactly("shared=v3");
        }
    }

    @Test
    void testRangeIteratorPreventsCompactionFromDeletingItsFiles() throws Exception {
        ManuallyTriggeredExecutor compactionExecutor = new ManuallyTriggeredExecutor();
        File directory = new File(tempDir.toFile(), "range-compaction");
        try (LocalKvDb db = createDb("range-compaction", compactionExecutor)) {
            putAndFlush(db, "shared", "v1");
            putAndFlush(db, "shared", "v2");
            putAndFlush(db, "shared", "v3");

            LocalKvDb.RangeIterator iterator =
                    db.rangeIterator("sha".getBytes(UTF_8), "shb".getBytes(UTF_8));
            ExecutorService compactionRunner = Executors.newSingleThreadExecutor();
            Future<?> compactionFuture = compactionRunner.submit(compactionExecutor::runNext);
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (sstFileCount(directory) < 4 && System.nanoTime() < deadline) {
                    Thread.yield();
                }

                assertThat(sstFileCount(directory)).isGreaterThanOrEqualTo(4);
                assertThat(compactionFuture).isNotDone();
                assertThat(iterator.advanceNext()).isTrue();
                assertThat(new String(iterator.getValue().copyBytes(), UTF_8)).isEqualTo("v3");
            } finally {
                iterator.close();
                compactionFuture.get(10, TimeUnit.SECONDS);
                compactionRunner.shutdownNow();
            }

            db.awaitCompaction();
            assertThat(scan(db, "sha", "shb")).containsExactly("shared=v3");
        }
    }

    @Test
    void testAutomaticCompactionRunsSynchronouslyByDefault() throws Exception {
        try (LocalKvDb db = createSyncDb("synchronous")) {
            putAndFlush(db, "shared", "v1");
            putAndFlush(db, "shared", "v2");
            putAndFlush(db, "shared", "v3");

            assertThat(db.getLevelFileCount(0)).isZero();
            assertThat(get(db, "shared")).isEqualTo("v3");
        }
    }

    @Test
    void testSynchronousCompactionFailureIsPropagatedByFlush() throws Exception {
        LocalKvDb db = createSyncDb("synchronous-failure");
        putAndFlush(db, "shared", "v1");
        putAndFlush(db, "shared", "v2");

        File[] sstFiles = new File(tempDir.toFile(), "synchronous-failure").listFiles();
        assertThat(sstFiles).isNotNull().hasSize(2);
        assertThat(sstFiles[0].delete()).isTrue();

        assertThatThrownBy(() -> putAndFlush(db, "shared", "v3")).isInstanceOf(IOException.class);
        assertThatThrownBy(db::close).isInstanceOf(IOException.class);
    }

    @Test
    void testCloseWaitsForPendingCompaction() throws Exception {
        ManuallyTriggeredExecutor compactionExecutor = new ManuallyTriggeredExecutor();
        LocalKvDb db = createDb("close", compactionExecutor);
        putAndFlush(db, "a", "1");
        putAndFlush(db, "b", "2");
        putAndFlush(db, "c", "3");

        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        CountDownLatch closeStarted = new CountDownLatch(1);
        Future<?> closeFuture =
                closeExecutor.submit(
                        () -> {
                            closeStarted.countDown();
                            db.close();
                            return null;
                        });
        try {
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(closeFuture).isNotDone();

            compactionExecutor.runNext();
            closeFuture.get(10, TimeUnit.SECONDS);

            assertThat(compactionExecutor.isShutdown()).isFalse();
        } finally {
            closeExecutor.shutdownNow();
            if (!closeFuture.isDone()) {
                compactionExecutor.runNextIfPresent();
                closeFuture.get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void testBackgroundCompactionFailureIsPropagated() throws Exception {
        ManuallyTriggeredExecutor compactionExecutor = new ManuallyTriggeredExecutor();
        LocalKvDb db = createDb("failure", compactionExecutor);
        putAndFlush(db, "shared", "v1");
        putAndFlush(db, "shared", "v2");
        putAndFlush(db, "shared", "v3");

        File[] sstFiles = new File(tempDir.toFile(), "failure").listFiles();
        assertThat(sstFiles).isNotNull().hasSize(3);
        assertThat(sstFiles[0].delete()).isTrue();

        compactionExecutor.runNext();

        assertThatThrownBy(db::awaitCompaction).isInstanceOf(IOException.class);
        assertThatThrownBy(db::close).isInstanceOf(IOException.class);
        assertThat(compactionExecutor.isShutdown()).isFalse();
    }

    @Test
    void testTooManyLevelZeroFilesApplyBackpressure() throws Exception {
        ManuallyTriggeredExecutor compactionExecutor = new ManuallyTriggeredExecutor();
        try (LocalKvDb db = createDb("backpressure", compactionExecutor)) {
            putAndFlush(db, "a", "1");
            putAndFlush(db, "b", "2");
            putAndFlush(db, "c", "3");
            putAndFlush(db, "d", "4");
            putAndFlush(db, "e", "5");

            ExecutorService writerExecutor = Executors.newSingleThreadExecutor();
            Future<?> blockedWrite =
                    writerExecutor.submit(
                            () -> {
                                putAndFlush(db, "f", "6");
                                return null;
                            });
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (db.getLevelFileCount(0) < 6 && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertThat(db.getLevelFileCount(0)).isEqualTo(6);
                assertThat(blockedWrite).isNotDone();

                compactionExecutor.runNext();
                blockedWrite.get(10, TimeUnit.SECONDS);
                db.awaitCompaction();

                assertThat(db.getLevelFileCount(0)).isZero();
                assertThat(get(db, "f")).isEqualTo("6");
            } finally {
                writerExecutor.shutdownNow();
            }
        }
    }

    private LocalKvDb createDb(String name, ExecutorService compactionExecutor) {
        return LocalKvDb.builder(new File(tempDir.toFile(), name))
                .memTableFlushThreshold(1024 * 1024)
                .blockSize(256)
                .level0FileNumCompactTrigger(3)
                .compressOptions(new CompressOptions("none", 1))
                .compactionExecutor(compactionExecutor)
                .build();
    }

    private LocalKvDb createSyncDb(String name) {
        return LocalKvDb.builder(new File(tempDir.toFile(), name))
                .memTableFlushThreshold(1024 * 1024)
                .blockSize(256)
                .level0FileNumCompactTrigger(3)
                .compressOptions(new CompressOptions("none", 1))
                .build();
    }

    private static void putAndFlush(LocalKvDb db, String key, String value) throws IOException {
        db.put(key.getBytes(UTF_8), value.getBytes(UTF_8));
        db.flush();
    }

    private static String get(LocalKvDb db, String key) throws IOException {
        byte[] value = db.get(key.getBytes(UTF_8));
        return value == null ? null : new String(value, UTF_8);
    }

    private static List<String> scan(LocalKvDb db, String from, String to) throws IOException {
        List<String> result = new ArrayList<>();
        for (Map.Entry<byte[], byte[]> entry :
                db.rangeScan(from.getBytes(UTF_8), to.getBytes(UTF_8))) {
            result.add(
                    new String(entry.getKey(), UTF_8) + "=" + new String(entry.getValue(), UTF_8));
        }
        return result;
    }

    private static int sstFileCount(File directory) {
        File[] files = directory.listFiles((ignored, name) -> name.endsWith(".db"));
        return files == null ? 0 : files.length;
    }

    private static class ManuallyTriggeredExecutor extends AbstractExecutorService {

        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private boolean shutdown;

        @Override
        public synchronized void execute(Runnable command) {
            if (shutdown) {
                throw new IllegalStateException("Executor is shut down.");
            }
            tasks.add(command);
        }

        synchronized int numQueuedTasks() {
            return tasks.size();
        }

        void runNext() {
            Runnable task;
            synchronized (this) {
                task = tasks.poll();
            }
            assertThat(task).as("queued compaction task").isNotNull();
            task.run();
        }

        void runNextIfPresent() {
            Runnable task;
            synchronized (this) {
                task = tasks.poll();
            }
            if (task != null) {
                task.run();
            }
        }

        @Override
        public synchronized void shutdown() {
            shutdown = true;
        }

        @Override
        public synchronized java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            java.util.List<Runnable> remaining = new java.util.ArrayList<>(tasks);
            tasks.clear();
            return remaining;
        }

        @Override
        public synchronized boolean isShutdown() {
            return shutdown;
        }

        @Override
        public synchronized boolean isTerminated() {
            return shutdown && tasks.isEmpty();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return isTerminated();
        }
    }
}
