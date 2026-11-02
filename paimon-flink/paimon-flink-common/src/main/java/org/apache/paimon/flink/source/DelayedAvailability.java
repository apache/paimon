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

package org.apache.paimon.flink.source;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Availability of a source reader that waits between scans. Sleeping in {@code pollNext} would
 * block the task's mailbox thread and make the source report itself 100% busy, so the wait is a
 * timer task that completes the future returned by {@link #future()} instead.
 */
public class DelayedAvailability implements AutoCloseable {

    private final String timerName;

    /** Created on the first wait, so a reader that never waits starts no thread. */
    @Nullable private ScheduledExecutorService timer;

    private final boolean ownsTimer;

    private CompletableFuture<Void> future = CompletableFuture.completedFuture(null);

    @Nullable private ScheduledFuture<?> pendingWakeUp;

    public DelayedAvailability(String timerName) {
        this.timerName = timerName;
        this.ownsTimer = true;
    }

    @VisibleForTesting
    public DelayedAvailability(ScheduledExecutorService timer) {
        this.timerName = null;
        this.timer = timer;
        this.ownsTimer = false;
    }

    public CompletableFuture<Void> future() {
        return future;
    }

    /** Makes the reader unavailable until {@code delayMillis} has elapsed. */
    public void delay(long delayMillis) {
        if (timer == null) {
            timer =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(timerName));
        }
        CompletableFuture<Void> next = new CompletableFuture<>();
        future = next;
        pendingWakeUp =
                timer.schedule(() -> next.complete(null), delayMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (pendingWakeUp != null) {
            pendingWakeUp.cancel(false);
            pendingWakeUp = null;
        }
        // unblock anyone still waiting on availability instead of leaving them hanging
        future.complete(null);
        if (ownsTimer && timer != null) {
            timer.shutdownNow();
        }
    }
}
