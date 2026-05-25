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

package org.apache.paimon.append.cluster;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Cluster manager for {@link AppendOnlyFileStore}. */
public class BucketedAppendClusterManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(BucketedAppendClusterManager.class);

    private final ExecutorService executor;
    private final BucketedAppendLevels levels;
    private final IncrementalClusterStrategy strategy;
    private final CompactRewriter rewriter;

    public BucketedAppendClusterManager(
            ExecutorService executor,
            List<DataFileMeta> restored,
            SchemaManager schemaManager,
            List<String> clusterKeys,
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            int numLevels,
            CompactRewriter rewriter) {
        this.executor = executor;
        this.levels = new BucketedAppendLevels(restored, numLevels);
        this.strategy =
                new IncrementalClusterStrategy(
                        schemaManager, clusterKeys, maxSizeAmp, sizeRatio, numRunCompactionTrigger);
        this.rewriter = rewriter;
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
    public void addNewFile(DataFileMeta file) {
        levels.addLevel0File(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;
        List<LevelSortedRun> runs = levels.levelSortedRuns();
        if (fullCompaction) {
            Preconditions.checkState(
                    taskFuture == null,
                    "A compaction task is still running while the user "
                            + "forces a new compaction. This is unexpected.");
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Trigger forced full compaction. Picking from the following runs\n{}",
                        runs);
            }
            optionalUnit = strategy.pick(levels.numberOfLevels(), runs, true);
        } else {
            if (taskFuture != null) {
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Trigger normal compaction. Picking from the following runs\n{}", runs);
            }
            optionalUnit =
                    strategy.pick(levels.numberOfLevels(), runs, false)
                            .filter(unit -> !unit.files().isEmpty());
        }

        optionalUnit.ifPresent(
                unit -> {
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
                    submitCompaction(unit);
                });
    }

    private void submitCompaction(CompactUnit unit) {

        BucketedAppendClusterTask task =
                new BucketedAppendClusterTask(unit.files(), unit.outputLevel(), rewriter);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Pick these files (name, level, size) for {} compaction: {}",
                    task.getClass().getSimpleName(),
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

    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    @Override
    public void close() throws IOException {}

    @VisibleForTesting
    public BucketedAppendLevels levels() {
        return levels;
    }

    /** A {@link CompactTask} impl for clustering of append bucketed table. */
    public static class BucketedAppendClusterTask extends CompactTask {

        private final List<DataFileMeta> toCluster;
        private final int outputLevel;
        private final CompactRewriter rewriter;

        public BucketedAppendClusterTask(
                List<DataFileMeta> toCluster, int outputLevel, CompactRewriter rewriter) {
            super(null);
            this.toCluster = toCluster;
            this.outputLevel = outputLevel;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact() throws Exception {
            List<DataFileMeta> rewrite = rewriter.rewrite(toCluster);
            return new CompactResult(toCluster, upgrade(rewrite));
        }

        protected List<DataFileMeta> upgrade(List<DataFileMeta> files) {
            return files.stream()
                    .map(file -> file.upgrade(outputLevel))
                    .collect(Collectors.toList());
        }
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }
}
