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

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
        ThreadPoolExecutor workers = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        CountDownLatch thirdStarted = new CountDownLatch(1);
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
                            } else if (input == 2) {
                                thirdStarted.countDown();
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
            workers.submit(() -> {}).get(3, TimeUnit.SECONDS);
            assertThat(thirdStarted.getCount()).isOne();
            assertThat(firstResult.isDone()).isFalse();

            releaseFirst.countDown();
            List<Integer> results = new ArrayList<>();
            results.add(firstResult.get(3, TimeUnit.SECONDS));
            // Consuming input 0 frees one slot. The next lookup refills it before input 1 is
            // consumed, instead of waiting for the whole window to drain.
            assertThat(iterator.hasNext()).isTrue();
            assertThat(thirdStarted.await(3, TimeUnit.SECONDS)).isTrue();
            results.add(iterator.next());
            assertThat(iterator.hasNext()).isTrue();
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
    public void testLazyInputIsConsumedOnlyAsSlotsFree() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger inputsRead = new AtomicInteger();
        Iterator<Integer> input =
                new Iterator<Integer>() {
                    private int next;

                    @Override
                    public boolean hasNext() {
                        return next < 100;
                    }

                    @Override
                    public Integer next() {
                        inputsRead.incrementAndGet();
                        return next++;
                    }
                };
        try (CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialBatchedExecuteAwaitRunningTasksOnClose(
                        workers,
                        value -> {
                            if (value == 0) {
                                await(releaseFirst);
                            }
                            return Collections.singletonList(value);
                        },
                        input,
                        4)) {
            releaseFirst.countDown();
            assertThat(iterator.next()).isEqualTo(0);
            assertThat(inputsRead).hasValue(4);
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    public void testWorkerFailureStopsNewSubmissions() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch secondFailed = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicInteger inputsRead = new AtomicInteger();
        RuntimeException workerFailure = new RuntimeException("worker failure");
        Iterator<Integer> input =
                Iterators.transform(
                        Arrays.asList(0, 1, 2).iterator(),
                        value -> {
                            inputsRead.incrementAndGet();
                            return value;
                        });
        CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialBatchedExecuteAwaitRunningTasksOnClose(
                        workers,
                        value -> {
                            if (value == 0) {
                                await(releaseFirst);
                            } else if (value == 1) {
                                secondFailed.countDown();
                                throw workerFailure;
                            }
                            return Collections.singletonList(value);
                        },
                        input,
                        2);

        try {
            Future<Throwable> result =
                    consumer.submit(
                            () ->
                                    catchThrowable(
                                            () -> {
                                                assertThat(iterator.next()).isZero();
                                                iterator.hasNext();
                                            }));

            assertThat(secondFailed.await(3, TimeUnit.SECONDS)).isTrue();
            // With input 0 still gated, this can only run after the failed task has
            // completely left BatchTask.run and published its failure state.
            workers.submit(() -> {}).get(3, TimeUnit.SECONDS);
            assertThat(inputsRead).hasValue(2);

            releaseFirst.countDown();
            assertThat(result.get(3, TimeUnit.SECONDS)).isSameAs(workerFailure);
            assertThat(inputsRead).hasValue(2);
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
    public void testWorkerUsesCallerClassLoaderAndRestoresPoolClassLoader() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(1);
        ClassLoader callerClassLoader = new ClassLoader(getClass().getClassLoader()) {};
        AtomicReference<ClassLoader> poolClassLoader = new AtomicReference<>();
        workers.submit(() -> poolClassLoader.set(Thread.currentThread().getContextClassLoader()))
                .get(10, TimeUnit.SECONDS);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        List<ClassLoader> seen = new ArrayList<>();
        try {
            Thread.currentThread().setContextClassLoader(callerClassLoader);
            try (CloseableBatchIterator<Integer> iterator =
                    ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                            workers,
                            value -> {
                                seen.add(Thread.currentThread().getContextClassLoader());
                                return Collections.singletonList(value);
                            },
                            Arrays.asList(0, 1),
                            1)) {
                while (iterator.hasNext()) {
                    iterator.next();
                }
            }
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }

        assertThat(seen).containsExactly(callerClassLoader, callerClassLoader);
        // The pool is shared, so a worker that keeps a caller's loader would hand it to whatever
        // runs on that thread next.
        AtomicReference<ClassLoader> restoredPoolClassLoader = new AtomicReference<>();
        workers.submit(
                        () ->
                                restoredPoolClassLoader.set(
                                        Thread.currentThread().getContextClassLoader()))
                .get(10, TimeUnit.SECONDS);
        assertThat(restoredPoolClassLoader.get()).isSameAs(poolClassLoader.get());
        workers.shutdownNow();
    }

    @Test
    public void testDirectExecutorPreservesCallerInterrupt() {
        ExecutorService workers = MoreExecutors.newDirectExecutorService();
        try {
            Thread.currentThread().interrupt();
            try (CloseableBatchIterator<Integer> iterator =
                    ThreadPoolUtils.sequentialBatchedExecuteCloseable(
                            workers, Collections::singletonList, Collections.singletonList(1), 1)) {
                assertThat(iterator.next()).isOne();
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            }
        } finally {
            Thread.interrupted();
            workers.shutdownNow();
        }
    }

    @Test
    public void testWorkerRunsWithTheSubmittingSubject() throws Exception {
        ExecutorService workers =
                ThreadPoolUtils.createCachedThreadPool(1, "subject-propagation-test");
        Subject firstSubject = new Subject();
        Subject secondSubject = new Subject();
        List<Subject> seenSubjects = new ArrayList<>();
        List<Thread> seenWorkers = new ArrayList<>();

        try {
            for (Subject subject : Arrays.asList(firstSubject, secondSubject)) {
                seenSubjects.add(
                        Subject.doAs(
                                subject,
                                (PrivilegedAction<Subject>)
                                        () -> {
                                            try (CloseableBatchIterator<Subject> iterator =
                                                    ThreadPoolUtils
                                                            .sequentialBatchedExecuteCloseable(
                                                                    workers,
                                                                    ignored -> {
                                                                        seenWorkers.add(
                                                                                Thread
                                                                                        .currentThread());
                                                                        return Collections
                                                                                .singletonList(
                                                                                        Subject
                                                                                                .getSubject(
                                                                                                        AccessController
                                                                                                                .getContext()));
                                                                    },
                                                                    Collections.singletonList(0),
                                                                    1)) {
                                                return iterator.next();
                                            }
                                        }));
            }

            assertThat(seenWorkers.get(1)).isSameAs(seenWorkers.get(0));
            assertThat(seenSubjects.get(0)).isSameAs(firstSubject);
            assertThat(seenSubjects.get(1)).isSameAs(secondSubject);
        } finally {
            workers.shutdownNow();
            assertThat(workers.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    public void testCloseCancelsQueuedTasksAndWaitsUninterruptibly() throws Exception {
        LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();
        AtomicBoolean runQueuedTaskOnInterrupt = new AtomicBoolean();
        // If close interrupts the worker before cancelling queued tasks, interrupt() runs the
        // queued task and exposes the ordering bug.
        ThreadPoolExecutor workers =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        taskQueue,
                        runnable ->
                                new Thread(runnable) {
                                    @Override
                                    public void interrupt() {
                                        super.interrupt();
                                        if (runQueuedTaskOnInterrupt.compareAndSet(true, false)) {
                                            Runnable queuedTask = taskQueue.poll();
                                            if (queuedTask != null) {
                                                queuedTask.run();
                                            }
                                        }
                                    }
                                });
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
            assertThat(workers.getQueue()).hasSize(1);
            assertThat(thirdExecuted).isFalse();
            runQueuedTaskOnInterrupt.set(true);

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
            assertThat(runQueuedTaskOnInterrupt).isFalse();
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
}
