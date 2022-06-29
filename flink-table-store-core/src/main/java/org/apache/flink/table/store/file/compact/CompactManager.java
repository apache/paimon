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

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Manager to submit compaction task.
 *
 * @param <I> The type of subject to be compacted.
 * @param <T> The record type of rewriter to write.
 * @param <O> The type of compact strategy's output.
 */
public abstract class CompactManager<I, T, O> {

    protected final ExecutorService executor;

    protected final CompactStrategy<I, O> strategy;

    protected final CompactRewriter<T> rewriter;

    protected Future<CompactResult> taskFuture;

    public CompactManager(
            ExecutorService executor, CompactStrategy<I, O> strategy, CompactRewriter<T> rewriter) {
        this.executor = executor;
        this.strategy = strategy;
        this.rewriter = rewriter;
    }

    /** Submit a new compaction task. */
    public void submitCompaction(I input) {
        if (taskFuture != null) {
            throw new IllegalStateException(
                    "Please finish the previous compaction before submitting new one.");
        }
        strategy.pick(input)
                .flatMap(output -> createCompactTask(input, output))
                .ifPresent((task) -> taskFuture = executor.submit(task));
    }

    protected abstract Optional<Callable<CompactResult>> createCompactTask(I input, O output);

    public boolean isCompactionFinished() {
        return taskFuture == null;
    }

    public Optional<CompactResult> finishCompaction(boolean blocking)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            if (blocking || taskFuture.isDone()) {
                CompactResult result = taskFuture.get();
                taskFuture = null;
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }
}
