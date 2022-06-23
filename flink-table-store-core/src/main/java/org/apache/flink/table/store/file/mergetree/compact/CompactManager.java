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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.mergetree.Levels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/** Manager to submit compaction task. */
public class CompactManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompactManager.class);

    private final ExecutorService executor;

    private final CompactStrategy strategy;

    private final Comparator<RowData> keyComparator;

    private final long minFileSize;

    private final CompactRewriter rewriter;

    private Future<CompactResult> taskFuture;

    public CompactManager(
            ExecutorService executor,
            CompactStrategy strategy,
            Comparator<RowData> keyComparator,
            long minFileSize,
            CompactRewriter rewriter) {
        this.executor = executor;
        this.minFileSize = minFileSize;
        this.keyComparator = keyComparator;
        this.strategy = strategy;
        this.rewriter = rewriter;
    }

    public boolean isCompactionFinished() {
        return taskFuture == null;
    }

    /** Submit a new compaction task. */
    public void submitCompaction(Levels levels) {
        if (taskFuture != null) {
            throw new IllegalStateException(
                    "Please finish the previous compaction before submitting new one.");
        }
        strategy.pick(levels.numberOfLevels(), levels.levelSortedRuns())
                .ifPresent(
                        unit -> {
                            if (unit.files().size() < 2) {
                                return;
                            }
                            /*
                             * As long as there is no older data, We can drop the deletion.
                             * If the output level is 0, there may be older data not involved in compaction.
                             * If the output level is bigger than 0, as long as there is no older data in
                             * the current levels, the output is the oldest, so we can drop the deletion.
                             * See CompactStrategy.pick.
                             */
                            boolean dropDelete =
                                    unit.outputLevel() != 0
                                            && unit.outputLevel() >= levels.nonEmptyHighestLevel();

                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                        "Submit compaction with files (name, level, size): "
                                                + levels.levelSortedRuns().stream()
                                                        .flatMap(lsr -> lsr.run().files().stream())
                                                        .map(
                                                                file ->
                                                                        String.format(
                                                                                "(%s, %d, %d)",
                                                                                file.fileName(),
                                                                                file.level(),
                                                                                file.fileSize()))
                                                        .collect(Collectors.joining(", ")));
                            }
                            submitCompaction(unit, dropDelete);
                        });
    }

    private void submitCompaction(CompactUnit unit, boolean dropDelete) {
        CompactTask task = new CompactTask(keyComparator, minFileSize, rewriter, unit, dropDelete);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for compaction: {}",
                    unit.files().stream()
                            .map(
                                    file ->
                                            String.format(
                                                    "(%s, %d, %d)",
                                                    file.fileName(), file.level(), file.fileSize()))
                            .collect(Collectors.joining(", ")));
        }
        taskFuture = executor.submit(task);
    }

    /** Finish current task, and update result files to {@link Levels}. */
    public Optional<CompactResult> finishCompaction(Levels levels, boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = finishCompaction(blocking);
        result.ifPresent(r -> levels.update(r.before(), r.after()));
        return result;
    }

    private Optional<CompactResult> finishCompaction(boolean blocking)
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
