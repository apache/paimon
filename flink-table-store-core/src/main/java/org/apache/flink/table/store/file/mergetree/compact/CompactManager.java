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
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/** Manager to submit compaction task. */
public class CompactManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompactManager.class);

    private final ExecutorService executor;

    private final CompactStrategy strategy;

    private final Comparator<RowData> keyComparator;

    private final long minFileSize;

    private final Rewriter rewriter;

    private Future<CompactResult> taskFuture;

    public CompactManager(
            ExecutorService executor,
            CompactStrategy strategy,
            Comparator<RowData> keyComparator,
            long minFileSize,
            Rewriter rewriter) {
        this.executor = executor;
        this.minFileSize = minFileSize;
        this.keyComparator = keyComparator;
        this.strategy = strategy;
        this.rewriter = rewriter;
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

                            LOG.info(
                                    "Submit compaction with files (level, size): "
                                            + levels.levelSortedRuns().stream()
                                                    .flatMap(lsr -> lsr.run().files().stream())
                                                    .map(
                                                            file ->
                                                                    String.format(
                                                                            "(%d, %d)",
                                                                            file.level(),
                                                                            file.fileSize()))
                                                    .collect(Collectors.joining(", ")));
                            LOG.info(
                                    "Pick these files (level, size) for compaction: "
                                            + unit.files().stream()
                                                    .map(
                                                            file ->
                                                                    String.format(
                                                                            "(%d, %d)",
                                                                            file.level(),
                                                                            file.fileSize()))
                                                    .collect(Collectors.joining(", ")));

                            CompactTask task = new CompactTask(unit, dropDelete);
                            taskFuture = executor.submit(task);
                        });
    }

    /** Finish current task, and update result files to {@link Levels}. */
    public Optional<CompactResult> finishCompaction(Levels levels)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            CompactResult result = taskFuture.get();
            levels.update(result.before(), result.after());
            taskFuture = null;
            return Optional.of(result);
        }
        return Optional.empty();
    }

    /** Rewrite sections to the files. */
    @FunctionalInterface
    public interface Rewriter {

        List<SstFileMeta> rewrite(
                int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
                throws Exception;
    }

    /** Result of compaction. */
    public interface CompactResult {

        List<SstFileMeta> before();

        List<SstFileMeta> after();
    }

    // --------------------------------------------------------------------------------------------
    // Internal classes
    // --------------------------------------------------------------------------------------------

    /** Compaction task. */
    private class CompactTask implements Callable<CompactResult> {

        private final int outputLevel;

        private final List<List<SortedRun>> partitioned;

        private final boolean dropDelete;

        // metrics
        private long rewriteInputSize;
        private long rewriteOutputSize;
        private int rewriteFilesNum;
        private int upgradeFilesNum;

        private CompactTask(CompactUnit unit, boolean dropDelete) {
            this.outputLevel = unit.outputLevel();
            this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
            this.dropDelete = dropDelete;

            this.rewriteInputSize = 0;
            this.rewriteOutputSize = 0;
            this.rewriteFilesNum = 0;
            this.upgradeFilesNum = 0;
        }

        @Override
        public CompactResult call() throws Exception {
            return compact();
        }

        private CompactResult compact() throws Exception {
            long startMillis = System.currentTimeMillis();

            List<List<SortedRun>> candidate = new ArrayList<>();
            List<SstFileMeta> before = new ArrayList<>();
            List<SstFileMeta> after = new ArrayList<>();

            // Checking the order and compacting adjacent and contiguous files
            // Note: can't skip an intermediate file to compact, this will destroy the overall
            // orderliness
            for (List<SortedRun> section : partitioned) {
                if (section.size() > 1) {
                    candidate.add(section);
                } else {
                    SortedRun run = section.get(0);
                    // No overlapping:
                    // We can just upgrade the large file and just change the level instead of
                    // rewriting it
                    // But for small files, we will try to compact it
                    for (SstFileMeta file : run.files()) {
                        if (file.fileSize() < minFileSize) {
                            // Smaller files are rewritten along with the previous files
                            candidate.add(singletonList(SortedRun.fromSingle(file)));
                        } else {
                            // Large file appear, rewrite previous and upgrade it
                            rewrite(candidate, before, after);
                            upgrade(file, before, after);
                        }
                    }
                }
            }
            rewrite(candidate, before, after);
            LOG.info(
                    "Done compacting {} files to {} files in {}ms. "
                            + "Rewrite input size = {}, output size = {}, rewrite file num = {}, upgrade file num = {}",
                    before.size(),
                    after.size(),
                    System.currentTimeMillis() - startMillis,
                    rewriteInputSize,
                    rewriteOutputSize,
                    rewriteFilesNum,
                    upgradeFilesNum);
            return result(before, after);
        }

        private void upgrade(SstFileMeta file, List<SstFileMeta> before, List<SstFileMeta> after) {
            if (file.level() != outputLevel) {
                before.add(file);
                after.add(file.upgrade(outputLevel));
                upgradeFilesNum++;
            }
        }

        private void rewrite(
                List<List<SortedRun>> candidate, List<SstFileMeta> before, List<SstFileMeta> after)
                throws Exception {
            if (candidate.isEmpty()) {
                return;
            }
            if (candidate.size() == 1) {
                List<SortedRun> section = candidate.get(0);
                if (section.size() == 0) {
                    return;
                } else if (section.size() == 1) {
                    for (SstFileMeta file : section.get(0).files()) {
                        upgrade(file, before, after);
                    }
                    candidate.clear();
                    return;
                }
            }
            candidate.forEach(
                    runs ->
                            runs.forEach(
                                    run -> {
                                        before.addAll(run.files());
                                        rewriteInputSize +=
                                                run.files().stream()
                                                        .mapToLong(SstFileMeta::fileSize)
                                                        .sum();
                                        rewriteFilesNum += run.files().size();
                                    }));
            List<SstFileMeta> result = rewriter.rewrite(outputLevel, dropDelete, candidate);
            after.addAll(result);
            rewriteOutputSize += result.stream().mapToLong(SstFileMeta::fileSize).sum();
            candidate.clear();
        }

        private CompactResult result(List<SstFileMeta> before, List<SstFileMeta> after) {
            return new CompactResult() {
                @Override
                public List<SstFileMeta> before() {
                    return before;
                }

                @Override
                public List<SstFileMeta> after() {
                    return after;
                }
            };
        }
    }
}
