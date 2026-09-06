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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Coordinates asynchronous compaction, failure propagation, and backpressure. */
class AsyncLsmCompactor extends LsmCompactor {

    private final ExecutorService executor;

    @Nullable private Future<?> compactionFuture;

    AsyncLsmCompactor(
            LsmLevels levels,
            CompactorFactory compactorFactory,
            int levelZeroFileCountTrigger,
            UniversalCompactor.FileSupplier fileSupplier,
            UniversalCompactor.FileDeleter fileDeleter,
            ExecutorService executor) {
        super(levels, compactorFactory, levelZeroFileCountTrigger, fileSupplier, fileDeleter);
        this.executor = executor;
    }

    @Override
    void scheduleIfNeeded() throws IOException {
        checkFailure();
        if (!needsCompaction() || compactionFuture != null) {
            return;
        }

        try {
            compactionFuture =
                    executor.submit(
                            () -> {
                                compactUntilStable();
                                return null;
                            });
        } catch (RuntimeException e) {
            throw new IOException("Failed to schedule background compaction.", e);
        }
    }

    @Override
    void applyBackpressure() throws IOException {
        if (needsBackpressure()) {
            await();
        }
    }

    @Override
    void checkFailure() throws IOException {
        Future<?> future = compactionFuture;
        if (future != null && future.isDone()) {
            awaitFuture(future);
            clearFuture(future);
        }
    }

    @Override
    void await() throws IOException {
        Future<?> future = compactionFuture;
        if (future == null) {
            return;
        }
        awaitFuture(future);
        clearFuture(future);
    }

    private void clearFuture(Future<?> future) {
        if (compactionFuture == future) {
            compactionFuture = null;
        }
    }

    private static void awaitFuture(Future<?> future) throws IOException {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for background compaction.", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Background compaction failed.", cause);
        } catch (CancellationException e) {
            throw new IOException("Background compaction was cancelled.", e);
        }
    }
}
