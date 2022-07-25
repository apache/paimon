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

package org.apache.flink.table.store.file.compact;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Manager to submit compaction task. */
public abstract class CompactManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompactManager.class);

    protected final ExecutorService executor;

    protected Future<CompactResult> taskFuture;

    public CompactManager(ExecutorService executor) {
        this.executor = executor;
    }

    /** Submit a new compaction task. */
    public abstract void submitCompaction();

    public boolean isCompactionFinished() {
        return taskFuture == null;
    }

    public Optional<CompactResult> finishCompaction(boolean blocking)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            if (blocking || taskFuture.isDone()) {
                CompactResult result;
                try {
                    result = taskFuture.get();
                } catch (CancellationException e) {
                    LOG.info("Compaction future is cancelled", e);
                    taskFuture = null;
                    return Optional.empty();
                }
                taskFuture = null;
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }

    public void cancelCompaction() {
        // TODO this method may leave behind orphan files if compaction is actually finished
        //  but some CPU work still needs to be done
        if (taskFuture != null && !taskFuture.isCancelled()) {
            taskFuture.cancel(true);
        }
    }
}
