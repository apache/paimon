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
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactRewriter;
import org.apache.flink.table.store.file.compact.CompactUnit;
import org.apache.flink.table.store.file.mergetree.Levels;
import org.apache.flink.table.store.file.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** Compact manager for {@link org.apache.flink.table.store.file.KeyValueFileStore}. */
public class KeyValueCompactManager extends CompactManager<Levels, SortedRun, CompactUnit> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueCompactManager.class);

    private final Comparator<RowData> keyComparator;

    private final long minFileSize;

    public KeyValueCompactManager(
            ExecutorService executor,
            KeyValueCompactStrategy strategy,
            Comparator<RowData> keyComparator,
            long minFileSize,
            CompactRewriter<SortedRun> rewriter) {
        super(executor, strategy, rewriter);
        this.minFileSize = minFileSize;
        this.keyComparator = keyComparator;
    }

    @Override
    protected Optional<Callable<CompactResult>> createCompactTask(Levels levels, CompactUnit unit) {
        if (unit.files().size() < 2) {
            return Optional.empty();
        }
        /*
         * As long as there is no older data, We can drop the deletion.
         * If the output level is 0, there may be older data not involved in compaction.
         * If the output level is bigger than 0, as long as there is no older data in
         * the current levels, the output is the oldest, so we can drop the deletion.
         * See CompactStrategy.pick.
         */
        boolean dropDelete =
                unit.outputLevel() != 0 && unit.outputLevel() >= levels.nonEmptyHighestLevel();

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
        return Optional.of(
                new KeyValueCompactTask(keyComparator, minFileSize, rewriter, unit, dropDelete));
    }

    /** Finish current task, and update result files to {@link Levels}. */
    public Optional<CompactResult> finishCompaction(Levels levels, boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = finishCompaction(blocking);
        result.ifPresent(r -> levels.update(r.before(), r.after()));
        return result;
    }
}
