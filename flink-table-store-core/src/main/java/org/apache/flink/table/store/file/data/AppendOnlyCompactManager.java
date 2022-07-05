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

package org.apache.flink.table.store.file.data;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactTask;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyCompactManager extends CompactManager {

    private final int fileNumCompactionTrigger;
    private final int fileSizeRatioCompactionTrigger;
    private final long targetFileSize;
    private final CompactRewriter rewriter;
    private final LinkedList<DataFileMeta> toCompact;

    public AppendOnlyCompactManager(
            ExecutorService executor,
            LinkedList<DataFileMeta> toCompact,
            int fileNumCompactionTrigger,
            int fileSizeRatioCompactionTrigger,
            long targetFileSize,
            CompactRewriter rewriter) {
        super(executor);
        this.toCompact = toCompact;
        this.fileNumCompactionTrigger = fileNumCompactionTrigger;
        this.fileSizeRatioCompactionTrigger = fileSizeRatioCompactionTrigger;
        this.targetFileSize = targetFileSize;
        this.rewriter = rewriter;
    }

    @Override
    public void submitCompaction() {
        if (taskFuture != null) {
            throw new IllegalStateException(
                    "Please finish the previous compaction before submitting new one.");
        }
        pickCompactBefore()
                .ifPresent(
                        (compactBefore) ->
                                taskFuture =
                                        executor.submit(
                                                new CompactTask() {
                                                    @Override
                                                    protected CompactResult compact()
                                                            throws Exception {
                                                        collectBeforeStats(compactBefore);
                                                        List<DataFileMeta> compactAfter =
                                                                rewriter.rewrite(compactBefore);
                                                        collectAfterStats(compactAfter);
                                                        return result(compactBefore, compactAfter);
                                                    }
                                                }));
    }

    @VisibleForTesting
    Optional<List<DataFileMeta>> pickCompactBefore() {
        List<Tuple2<Integer, Integer>> intervals =
                findSmallFileIntervals(toCompact, targetFileSize);
        for (Tuple2<Integer, Integer> interval : intervals) {
            if (pickInterval(interval)) {
                // [interval.f0, interval.f1] will be compacted
                // [0, interval.f0 - 1] can be immediately released from toCompact
                // Note: [interval.f0, interval.f1] should be released after compact task
                // finished
                List<DataFileMeta> compactBefore =
                        new ArrayList<>(toCompact.subList(interval.f0, interval.f1 + 1));
                for (int i = 0; i <= interval.f0 - 1; i++) {
                    toCompact.pollFirst();
                }
                return Optional.of(compactBefore);
            }
        }
        if (intervals.isEmpty()) {
            // no small file detected, and toCompact can be released
            toCompact.clear();
        } else {
            Tuple2<Integer, Integer> last = intervals.get(intervals.size() - 1);
            boolean lastFile = last.f1 == toCompact.size() - 1;
            boolean noAlmostFullFile =
                    toCompact.subList(last.f0, last.f1 + 1).stream()
                            .noneMatch(
                                    file ->
                                            file.fileSize()
                                                    >= targetFileSize
                                                            / fileSizeRatioCompactionTrigger);
            if (lastFile && noAlmostFullFile) {
                // wait for more small files to match fileNumTrigger
                // keep them and release [0, last.f0 - 1]
                for (int i = 0; i <= last.f0 - 1; i++) {
                    toCompact.pollFirst();
                }
            } else {
                toCompact.clear();
            }
        }
        return Optional.empty();
    }

    private boolean pickInterval(Tuple2<Integer, Integer> interval) {
        int start = interval.f0;
        int end = interval.f1;
        if (start == end) {
            // single small file
            return false;
        } else {
            long totalFileSize =
                    toCompact.subList(start, end + 1).stream()
                            .mapToLong(DataFileMeta::fileSize)
                            .sum();
            int fileNum = end - start + 1;
            // find a balance between file num and compaction ratio
            return fileNum > fileNumCompactionTrigger
                    && totalFileSize < targetFileSize / fileSizeRatioCompactionTrigger;
        }
    }

    @VisibleForTesting
    static List<Tuple2<Integer, Integer>> findSmallFileIntervals(
            List<DataFileMeta> toCompact, long targetFileSize) {
        List<Tuple2<Integer, Integer>> intervals = new ArrayList<>();
        int start = -1;
        int end = -1;
        for (int i = 0; i < toCompact.size(); i++) {
            DataFileMeta file = toCompact.get(i);
            if (file.fileSize() < targetFileSize) {
                start = start == -1 ? i : start;
                end = i;
            } else if (start != -1 && end >= start) {
                intervals.add(Tuple2.of(start, end));
                start = -1;
                end = -1;
            }
        }
        if (start != -1 && end >= start) {
            intervals.add(Tuple2.of(start, end));
        }
        return intervals;
    }

    @VisibleForTesting
    LinkedList<DataFileMeta> getToCompact() {
        return toCompact;
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> toCompact) throws Exception;
    }
}
