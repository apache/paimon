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

import org.apache.paimon.utils.ThreadPoolUtils.CloseableBatchIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link ThreadPoolUtils}. */
public class ThreadPoolUtilsTest {

    @Test
    public void testCloseableBatchReturnsInOrderAndBoundsSubmission() throws Exception {
        CountingThreadPoolExecutor workers = new CountingThreadPoolExecutor(2);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                        workers,
                        input -> {
                            if (input == 0) {
                                firstStarted.countDown();
                                await(releaseFirst);
                            } else if (input == 1) {
                                secondFinished.countDown();
                            }
                            return Collections.singletonList(input);
                        },
                        Arrays.asList(0, 1, 2, 3),
                        2);

        try {
            Future<Integer> firstResult =
                    consumer.submit(
                            () -> {
                                assertThat(iterator.hasNext()).isTrue();
                                return iterator.next();
                            });

            assertThat(firstStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(secondFinished.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(workers.getSubmittedTaskCount()).isEqualTo(2);
            assertThat(firstResult.isDone()).isFalse();

            releaseFirst.countDown();
            List<Integer> results = new ArrayList<>();
            results.add(firstResult.get(3, TimeUnit.SECONDS));
            assertThat(iterator.hasNext()).isTrue();
            results.add(iterator.next());
            assertThat(workers.getSubmittedTaskCount()).isEqualTo(2);

            assertThat(iterator.hasNext()).isTrue();
            assertThat(workers.getSubmittedTaskCount()).isEqualTo(4);
            results.add(iterator.next());
            assertThat(iterator.hasNext()).isTrue();
            results.add(iterator.next());
            assertThat(iterator.hasNext()).isFalse();
            assertThat(results).containsExactly(0, 1, 2, 3);
        } finally {
            releaseFirst.countDown();
            iterator.close();
            consumer.shutdownNow();
            workers.shutdownNow();
            assertThat(consumer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            assertThat(workers.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testCloseCancelsQueuedTasksAndWaitsUninterruptibly() throws Exception {
        ThreadPoolExecutor workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        CountDownLatch allowSecondToExit = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicInteger executions = new AtomicInteger();
        AtomicBoolean thirdExecuted = new AtomicBoolean();
        AtomicBoolean closeRestoredInterrupt = new AtomicBoolean();
        AtomicReference<Thread> closeThread = new AtomicReference<>();
        CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                        workers,
                        input -> {
                            executions.incrementAndGet();
                            if (input == 1) {
                                secondStarted.countDown();
                                awaitIgnoringInterrupts(allowSecondToExit, workerInterrupted);
                            } else if (input == 2) {
                                thirdExecuted.set(true);
                            }
                            return Collections.singletonList(input);
                        },
                        Arrays.asList(0, 1, 2),
                        3);

        try {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isZero();
            assertThat(secondStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(workers.getTaskCount()).isEqualTo(3);

            Future<?> closeResult =
                    closer.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closeStarted.countDown();
                                iterator.close();
                                closeRestoredInterrupt.set(Thread.currentThread().isInterrupted());
                            });
            assertThat(closeStarted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(workerInterrupted.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(closeResult.isDone()).isFalse();

            closeThread.get().interrupt();
            allowSecondToExit.countDown();
            closeResult.get(3, TimeUnit.SECONDS);

            assertThat(closeRestoredInterrupt).isTrue();
            assertThat(executions).hasValue(2);
            assertThat(thirdExecuted).isFalse();
            assertThat(iterator.hasNext()).isFalse();
            iterator.close();
            iterator.close();
            assertThat(executions).hasValue(2);
        } finally {
            allowSecondToExit.countDown();
            iterator.close();
            closer.shutdownNow();
            workers.shutdownNow();
            assertThat(closer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            assertThat(workers.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testClosePreservesPrimaryErrorAndSuppressesWorkerError() throws Exception {
        ThreadPoolExecutor workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch waitForCancellation = new CountDownLatch(1);
        AssertionError primaryFailure = new AssertionError("primary failure");
        AssertionError workerFailure = new AssertionError("worker failure");
        CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                        workers,
                        input -> {
                            if (input == 0) {
                                await(secondStarted);
                            } else {
                                secondStarted.countDown();
                                try {
                                    waitForCancellation.await();
                                } catch (InterruptedException e) {
                                    throw workerFailure;
                                }
                            }
                            return Collections.singletonList(input);
                        },
                        Arrays.asList(0, 1),
                        2);

        try {
            assertThat(iterator.hasNext()).isTrue();
            assertThat(iterator.next()).isZero();

            Throwable thrown =
                    catchThrowable(
                            () -> {
                                try (CloseableBatchIterator<Integer> ignored = iterator) {
                                    throw primaryFailure;
                                }
                            });

            assertThat(thrown).isSameAs(primaryFailure).hasSuppressedException(workerFailure);
            iterator.close();
        } finally {
            waitForCancellation.countDown();
            iterator.close();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch, CountDownLatch interrupted) {
        while (true) {
            try {
                latch.await();
                return;
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        }
    }

    private static class CountingThreadPoolExecutor extends ThreadPoolExecutor {

        private final AtomicInteger submittedTaskCount = new AtomicInteger();

        private CountingThreadPoolExecutor(int threadCount) {
            super(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable command) {
            submittedTaskCount.incrementAndGet();
            try {
                super.execute(command);
            } catch (RuntimeException | Error failure) {
                submittedTaskCount.decrementAndGet();
                throw failure;
            }
        }

        private int getSubmittedTaskCount() {
            return submittedTaskCount.get();
        }
    }
}
