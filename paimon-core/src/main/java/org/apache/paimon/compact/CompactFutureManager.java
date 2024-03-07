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

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Base implementation of {@link CompactManager} which runs compaction in a separate thread. */
public abstract class CompactFutureManager implements CompactManager {

    protected Future<CompactResult> taskFuture;

    @Override
    public void cancelCompaction() {
        // TODO this method may leave behind orphan files if compaction is actually finished
        //  but some CPU work still needs to be done
        if (taskFuture != null && !taskFuture.isCancelled()) {
            taskFuture.cancel(true);
        }
    }

    @Override
    public boolean isCompacting() {
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
