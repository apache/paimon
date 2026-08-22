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

    @Nullable private CompactTask task;

    protected void submitTask(ExecutorService executor, CompactTask task) {
        this.task = task;
        this.taskFuture = executor.submit(task);
    }

    @Override
    public void cancelCompaction() {
        if (task != null) {
            // Tell the task that its output is not needed anymore before interrupting it, so that
            // it deletes the files it produced no matter whether it observes the interruption.
            // See CompactTask#cancel for the invariant that this must not be followed by
            // prepareCommit on the same writer/maintainer.
            task.cancel();
        }
        if (taskFuture != null && !taskFuture.isCancelled()) {
            taskFuture.cancel(true);
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
                CompactTask currentTask = task;
                try {
                    return Optional.of(obtainCompactResult());
                } catch (CancellationException e) {
                    // Cancellation may have won the race against the completion of the task, in
                    // which case the future has dropped a result whose files are already on disk.
                    // Report it so that the caller can account for them. If the task instead
                    // observed the cancellation, it has deleted its own output and there is
                    // nothing to report here.
                    return Optional.ofNullable(
                            currentTask == null ? null : currentTask.completedResult());
                } finally {
                    taskFuture = null;
                    task = null;
                }
            }
        }
        return Optional.empty();
    }

    @VisibleForTesting
    protected CompactResult obtainCompactResult() throws InterruptedException, ExecutionException {
        return taskFuture.get();
    }
}
