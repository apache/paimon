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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactFutureManager} cancellation cleanup behaviour. */
public class CompactFutureManagerTest {

    @Test
    public void cancelInvokesDiscardOnRunningTask() throws Exception {
        AtomicBoolean discarded = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);

        CompactTask task =
                new CompactTask(null) {
                    @Override
                    protected CompactResult doCompact() throws Exception {
                        started.countDown();
                        Thread.sleep(Long.MAX_VALUE);
                        return null;
                    }

                    @Override
                    public void discardInflightOutputs() {
                        discarded.set(true);
                    }
                };

        TestCompactFutureManager manager = new TestCompactFutureManager();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            manager.submit(executor, task);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            manager.cancelCompaction();
            assertThat(discarded.get()).isTrue();
            assertThat(manager.getCompactionResult(true)).isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void cancelThenDrainDoesNotDiscardTwice() throws Exception {
        java.util.concurrent.atomic.AtomicInteger discardCalls =
                new java.util.concurrent.atomic.AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);

        CompactTask task =
                new CompactTask(null) {
                    @Override
                    protected CompactResult doCompact() throws Exception {
                        started.countDown();
                        Thread.sleep(Long.MAX_VALUE);
                        return null;
                    }

                    @Override
                    public void discardInflightOutputs() {
                        discardCalls.incrementAndGet();
                    }
                };

        TestCompactFutureManager manager = new TestCompactFutureManager();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            manager.submit(executor, task);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            manager.cancelCompaction();
            assertThat(manager.getCompactionResult(true)).isEmpty();
            assertThat(discardCalls.get()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void cancelAfterTaskCompletesDoesNotDiscard() throws Exception {
        AtomicBoolean discarded = new AtomicBoolean(false);
        CountDownLatch finished = new CountDownLatch(1);

        CompactTask task =
                new CompactTask(null) {
                    @Override
                    protected CompactResult doCompact() throws Exception {
                        finished.countDown();
                        return new CompactResult(Collections.emptyList(), Collections.emptyList());
                    }

                    @Override
                    public void discardInflightOutputs() {
                        discarded.set(true);
                    }
                };

        TestCompactFutureManager manager = new TestCompactFutureManager();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            manager.submit(executor, task);
            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            Optional<CompactResult> result = manager.getCompactionResult(true);
            assertThat(result).isPresent();
            manager.cancelCompaction();
            assertThat(discarded.get()).isFalse();
        } finally {
            executor.shutdownNow();
        }
    }

    private static class TestCompactFutureManager extends CompactFutureManager {

        void submit(ExecutorService executor, CompactTask task) {
            submitTask(executor, task);
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
            return Collections.emptyList();
        }

        @Override
        public void triggerCompaction(boolean fullCompaction) {}

        @Override
        public Optional<CompactResult> getCompactionResult(boolean blocking)
                throws ExecutionException, InterruptedException {
            return innerGetCompactionResult(blocking);
        }

        @Override
        public void close() {}
    }
}
