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

import org.apache.paimon.annotation.VisibleForTesting;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Base implementation of {@link CompactManager} which runs compaction in a separate thread. */
public abstract class CompactFutureManager implements CompactManager {

    protected Future<CompactResult> taskFuture;

    /**
     * The task behind {@link #taskFuture}, kept so that its files can be deleted if it is
     * cancelled.
     */
    @Nullable private CompactTask task;

    /** Submits a compaction task and remembers it as the current one. */
    protected void submitTask(ExecutorService executor, CompactTask task) {
        this.task = task;
        this.taskFuture = executor.submit(task);
    }

    @Override
    public void cancelCompaction() {
        if (taskFuture != null && !taskFuture.isCancelled()) {
            boolean cancelled = taskFuture.cancel(true);
            if (cancelled && task != null) {
                // A cancelled future throws its result away, so the files the task has already
                // written become unreachable: nothing else knows their names. The task deletes
                // them itself once the interrupt reaches it, but the interrupt can just as well
                // land after the task is done, and then this is the only cleanup left.
                task.abortNewFiles();
            }
        }
    }

    @Override
    public boolean compactNotCompleted() {
        return taskFuture != null;
    }

    protected final Optional<CompactResult> innerGetCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            if (blocking || taskFuture.isDone()) {
                CompactResult result;
                try {
                    result = obtainCompactResult();
                } catch (CancellationException e) {
                    return Optional.empty();
                } finally {
                    taskFuture = null;
                    task = null;
                }
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    protected CompactResult obtainCompactResult() throws InterruptedException, ExecutionException {
        return taskFuture.get();
    }
}
