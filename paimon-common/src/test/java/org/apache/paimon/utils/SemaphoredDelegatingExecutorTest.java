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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.CommonTestUtils.waitUtil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link SemaphoredDelegatingExecutor}. */
public class SemaphoredDelegatingExecutorTest {

    @Test
    public void testFailingTaskDoesNotDeadlockBlockedSubmission() throws Exception {
        ExecutorService delegated = Executors.newSingleThreadExecutor();
        SemaphoredDelegatingExecutor workers =
                new SemaphoredDelegatingExecutor(delegated, 1, false);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch failFirst = new CountDownLatch(1);
        AtomicInteger inputsRead = new AtomicInteger();
        RuntimeException workerFailure = new RuntimeException("worker failure");
        Iterator<Integer> values = Arrays.asList(0, 1, 2).iterator();
        Iterator<Integer> input =
                new Iterator<Integer>() {
                    @Override
                    public boolean hasNext() {
                        return values.hasNext();
                    }

                    @Override
                    public Integer next() {
                        inputsRead.incrementAndGet();
                        return values.next();
                    }
                };
        CloseableBatchIterator<Integer> iterator =
                ThreadPoolUtils.sequentialSlidingWindowExecuteAwaitRunningTasksOnClose(
                        workers,
                        value -> {
                            if (value == 0) {
                                firstStarted.countDown();
                                await(failFirst);
                                throw workerFailure;
                            }
                            return Collections.singletonList(value);
                        },
                        input,
                        2);
        Future<Throwable> result = consumer.submit(() -> catchThrowable(() -> iterator.hasNext()));

        try (CloseableBatchIterator<Integer> ignored = iterator) {
            try {
                assertThat(firstStarted.await(3, TimeUnit.SECONDS)).isTrue();
                waitUtil(
                        () -> workers.getWaitingCount() == 1,
                        Duration.ofSeconds(3),
                        Duration.ofMillis(10));
                failFirst.countDown();

                assertThat(result.get(3, TimeUnit.SECONDS)).isSameAs(workerFailure);
                assertThat(inputsRead).hasValue(2);
            } finally {
                failFirst.countDown();
                consumer.shutdownNow();
                assertThat(consumer.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            }
        } finally {
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
}
