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

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Shared causal fixtures for Format Table commit tests. */
final class FormatTableCommitTestUtils {

    private FormatTableCommitTestUtils() {}

    static List<ClassLoader> observeContextClassLoaders(ExecutorService executor, int workerCount)
            throws Exception {
        CountDownLatch workersStarted = new CountDownLatch(workerCount);
        CountDownLatch releaseWorkers = new CountDownLatch(1);
        ConcurrentLinkedQueue<ClassLoader> classLoaders = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            futures.add(
                    executor.submit(
                            () -> {
                                classLoaders.add(Thread.currentThread().getContextClassLoader());
                                workersStarted.countDown();
                                try {
                                    if (!releaseWorkers.await(10, TimeUnit.SECONDS)) {
                                        throw new AssertionError(
                                                "Timed out observing cleanup executor workers");
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new AssertionError(
                                            "Interrupted while observing cleanup executor workers",
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
        return new ArrayList<>(classLoaders);
    }

    static ExecutionException awaitFailure(Future<?> future) throws Exception {
        try {
            future.get(10, TimeUnit.SECONDS);
            throw new AssertionError("Expected cleanup commit to fail");
        } catch (ExecutionException expected) {
            return expected;
        }
    }

    static Throwable rootCause(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }

    static List<Throwable> failureTree(Throwable throwable) {
        List<Throwable> failures = new ArrayList<>();
        collectFailures(throwable, failures);
        return failures;
    }

    private static void collectFailures(Throwable throwable, List<Throwable> failures) {
        if (throwable == null) {
            return;
        }
        failures.add(throwable);
        for (Throwable suppressed : throwable.getSuppressed()) {
            collectFailures(suppressed, failures);
        }
        collectFailures(throwable.getCause(), failures);
    }

    static final class PartialBarrierDeleteFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final CountDownLatch bothDeletesStarted = new CountDownLatch(2);
        private final CountDownLatch releaseFirstDelete = new CountDownLatch(1);
        private final CountDownLatch releaseSecondDelete = new CountDownLatch(1);
        private final CountDownLatch firstDeleteReturned = new CountDownLatch(1);
        private final AtomicInteger activeDeletes = new AtomicInteger();

        @Override
        public FileStatus[] listStatus(Path path) throws IOException {
            FileStatus[] statuses = super.listStatus(path);
            Arrays.sort(statuses, Comparator.comparing(status -> status.getPath().toString()));
            return statuses;
        }

        @Override
        public boolean delete(Path path, boolean recursive) throws IOException {
            activeDeletes.incrementAndGet();
            bothDeletesStarted.countDown();
            await(bothDeletesStarted, "both barrier delete calls");
            try {
                if ("data-000.csv".equals(path.getName())) {
                    await(releaseFirstDelete, "first barrier delete release");
                    return super.delete(path, recursive);
                }
                await(releaseSecondDelete, "second barrier delete release");
                return super.delete(path, recursive);
            } finally {
                activeDeletes.decrementAndGet();
                if ("data-000.csv".equals(path.getName())) {
                    firstDeleteReturned.countDown();
                }
            }
        }

        boolean awaitBothDeletesStarted() throws InterruptedException {
            return bothDeletesStarted.await(10, TimeUnit.SECONDS);
        }

        void releaseFirstDelete() {
            releaseFirstDelete.countDown();
        }

        void releaseSecondDelete() {
            releaseSecondDelete.countDown();
        }

        boolean awaitFirstDeleteReturned() throws InterruptedException {
            return firstDeleteReturned.await(10, TimeUnit.SECONDS);
        }

        int activeDeletes() {
            return activeDeletes.get();
        }

        private static void await(CountDownLatch latch, String description) throws IOException {
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
