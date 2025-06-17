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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/** Compact task for merge tree compaction. */
public class MergeTreeCompactTask extends CompactTask {

    private final long minFileSize;
    private final CompactRewriter rewriter;
    private final int outputLevel;
    private final Supplier<CompactDeletionFile> compactDfSupplier;
    private final List<List<SortedRun>> partitioned;
    private final boolean dropDelete;
    private final int maxLevel;
    @Nullable private final RecordLevelExpire recordLevelExpire;
    private final boolean forceRewriteAllFiles;

    // metric
    private int upgradeFilesNum;

    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            int maxLevel,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            Supplier<CompactDeletionFile> compactDfSupplier,
            @Nullable RecordLevelExpire recordLevelExpire,
            boolean forceRewriteAllFiles) {
        super(metricsReporter);
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.compactDfSupplier = compactDfSupplier;
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
        this.dropDelete = dropDelete;
        this.maxLevel = maxLevel;
        this.recordLevelExpire = recordLevelExpire;
        this.forceRewriteAllFiles = forceRewriteAllFiles;

        this.upgradeFilesNum = 0;
    }

    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>();
        CompactResult result = new CompactResult();

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
                        rewrite(candidate, result);
                        upgrade(file, result);
                    }
                }
            }
        }
        rewrite(candidate, result);
        result.setDeletionFile(compactDfSupplier.get());
        return result;
    }

    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), upgradeFilesNum);
    }

    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        if ((outputLevel == maxLevel && containsDeleteRecords(file))
                || forceRewriteAllFiles
                || containsExpiredRecords(file)) {
            List<List<SortedRun>> candidate = new ArrayList<>();
            candidate.add(singletonList(SortedRun.fromSingle(file)));
            rewriteImpl(candidate, toUpdate);
            return;
        }

        if (file.level() != outputLevel) {
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult);
            upgradeFilesNum++;
        }
    }

    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        if (candidate.isEmpty()) {
            return;
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate);
                }
                candidate.clear();
                return;
            }
        }
        rewriteImpl(candidate, toUpdate);
    }

    private void rewriteImpl(List<List<SortedRun>> candidate, CompactResult toUpdate)
            throws Exception {
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        toUpdate.merge(rewriteResult);
        candidate.clear();
    }

    private boolean containsDeleteRecords(DataFileMeta file) {
        return file.deleteRowCount().map(d -> d > 0).orElse(true);
    }

    private boolean containsExpiredRecords(DataFileMeta file) {
        return recordLevelExpire != null && recordLevelExpire.isExpireFile(file);
    }
}
