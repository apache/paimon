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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SemaphoredDelegatingExecutor}. */
public class SemaphoredDelegatingExecutorTest {

    private static final long TIMEOUT_SECONDS = 10;

    @Test
    public void testInterruptedExecuteRejectsTaskAndKeepsPermitCount() throws Exception {
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        try {
            SemaphoredDelegatingExecutor executor =
                    new SemaphoredDelegatingExecutor(delegate, 0, true);
            AtomicBoolean ran = new AtomicBoolean(false);
            AtomicReference<Throwable> thrown = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean(false);
            CountDownLatch finished = new CountDownLatch(1);

            Thread submitter =
                    new Thread(
                            () -> {
                                try {
                                    executor.execute(() -> ran.set(true));
                                } catch (Throwable t) {
                                    thrown.set(t);
                                    interrupted.set(Thread.currentThread().isInterrupted());
                                } finally {
                                    finished.countDown();
                                }
                            });
            // Daemon: if a regression ever made the permit wait uninterruptible, the await
            // below still fails, and this thread must not keep the surefire fork alive.
            submitter.setDaemon(true);
            submitter.start();
            awaitWaitingOnPermit(executor);

            // Interrupt once, after the submitter is parked on the semaphore: the flag
            // asserted below can then only have been restored by execute() itself.
            submitter.interrupt();
            assertThat(finished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            assertThat(thrown.get())
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(interrupted.get()).isTrue();

            // Drain the delegate before asserting: the original code handed the task to it,
            // and an assertion taken before that worker ran would pass for the wrong reason.
            delegate.shutdown();
            assertThat(delegate.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(ran.get()).isFalse();
            assertThat(executor.getAvailablePermits()).isZero();
        } finally {
            delegate.shutdownNow();
        }
    }

    @Test
    public void testRejectedByDelegateReleasesPermit() {
        ExecutorService delegate = Executors.newSingleThreadExecutor();
        delegate.shutdownNow();
        SemaphoredDelegatingExecutor executor = new SemaphoredDelegatingExecutor(delegate, 1, true);

        assertThat(executor.getAvailablePermits()).isEqualTo(1);

        assertThatThrownBy(() -> executor.execute(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(executor.getAvailablePermits()).isEqualTo(1);

        assertThatThrownBy(() -> executor.submit(() -> null))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(executor.getAvailablePermits()).isEqualTo(1);

        assertThatThrownBy(() -> executor.submit(() -> {}))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(executor.getAvailablePermits()).isEqualTo(1);

        assertThatThrownBy(() -> executor.submit(() -> {}, "result"))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(executor.getAvailablePermits()).isEqualTo(1);
    }

    @Test
    public void testInlineExecutionReleasesPermitOnlyOnce() {
        // corePoolSize 1 with a queue of 1: once the worker is busy and the queue is full,
        // CallerRunsPolicy runs the next task in the calling thread.
        ThreadPoolExecutor delegate =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1),
                        new ThreadPoolExecutor.CallerRunsPolicy());
        CountDownLatch block = new CountDownLatch(1);
        try {
            delegate.execute(
                    () -> {
                        try {
                            block.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
            delegate.execute(() -> {});
            SemaphoredDelegatingExecutor executor =
                    new SemaphoredDelegatingExecutor(delegate, 1, true);

            // The wrapper runs inline and releases the permit in its finally, and the task's
            // own rejection then comes back out of execute(): releasing again would inflate
            // the semaphore past permitCount.
            assertThatThrownBy(
                            () ->
                                    executor.execute(
                                            () -> {
                                                throw new RejectedExecutionException("from task");
                                            }))
                    .isInstanceOf(RejectedExecutionException.class)
                    .hasMessage("from task");
            assertThat(executor.getAvailablePermits()).isEqualTo(1);
        } finally {
            block.countDown();
            delegate.shutdownNow();
        }
    }

    @Test
    public void testNormalExecutionKeepsPermitsBalanced() throws Exception {
        ExecutorService delegate = Executors.newCachedThreadPool();
        try {
            SemaphoredDelegatingExecutor executor =
                    new SemaphoredDelegatingExecutor(delegate, 2, true);
            AtomicInteger completed = new AtomicInteger();

            for (int i = 0; i < 5; i++) {
                executor.execute(completed::incrementAndGet);
            }

            delegate.shutdown();
            assertThat(delegate.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(completed.get()).isEqualTo(5);
            assertThat(executor.getAvailablePermits()).isEqualTo(2);
        } finally {
            delegate.shutdownNow();
        }
    }

    /** Waits until the submitter thread is queued on the semaphore, bounded so it cannot hang. */
    private static void awaitWaitingOnPermit(SemaphoredDelegatingExecutor executor)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (executor.getWaitingCount() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertThat(executor.getWaitingCount())
                .as("submitter should be parked on the semaphore")
                .isEqualTo(1);
    }
}
