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
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Collections.singletonList;

/** Compaction task. */
public class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    private final long minFileSize;
    private final CompactRewriter rewriter;
    private final int outputLevel;

    private final List<List<SortedRun>> partitioned;

    private final boolean dropDelete;

    // metrics
    private long rewriteInputSize;
    private long rewriteOutputSize;
    private int rewriteFilesNum;
    private int upgradeFilesNum;

    public CompactTask(
            Comparator<RowData> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete) {
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
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
        List<DataFileMeta> before = new ArrayList<>();
        List<DataFileMeta> after = new ArrayList<>();

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
                for (DataFileMeta file : run.files()) {
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

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Done compacting {} files to {} files in {}ms. "
                            + "Rewrite input size = {}, output size = {}, rewrite file num = {}, upgrade file num = {}",
                    before.size(),
                    after.size(),
                    System.currentTimeMillis() - startMillis,
                    rewriteInputSize,
                    rewriteOutputSize,
                    rewriteFilesNum,
                    upgradeFilesNum);
        }

        return result(before, after);
    }

    private void upgrade(DataFileMeta file, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (file.level() != outputLevel) {
            before.add(file);
            after.add(file.upgrade(outputLevel));
            upgradeFilesNum++;
        }
    }

    private void rewrite(
            List<List<SortedRun>> candidate, List<DataFileMeta> before, List<DataFileMeta> after)
            throws Exception {
        if (candidate.isEmpty()) {
            return;
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {
                for (DataFileMeta file : section.get(0).files()) {
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
                                                    .mapToLong(DataFileMeta::fileSize)
                                                    .sum();
                                    rewriteFilesNum += run.files().size();
                                }));
        List<DataFileMeta> result = rewriter.rewrite(outputLevel, dropDelete, candidate);
        after.addAll(result);
        rewriteOutputSize += result.stream().mapToLong(DataFileMeta::fileSize).sum();
        candidate.clear();
    }

    private CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CompactResult() {
            @Override
            public List<DataFileMeta> before() {
                return before;
            }

            @Override
            public List<DataFileMeta> after() {
                return after;
            }
        };
    }
}
