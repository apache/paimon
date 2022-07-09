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
import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyCompactManager extends CompactManager {

    private final int minFileNum;
    private final int maxFileNum;
    private final long targetFileSize;
    private final CompactRewriter rewriter;
    private final LinkedList<DataFileMeta> toCompact;

    public AppendOnlyCompactManager(
            ExecutorService executor,
            LinkedList<DataFileMeta> toCompact,
            int minFileNum,
            int maxFileNum,
            long targetFileSize,
            CompactRewriter rewriter) {
        super(executor);
        this.toCompact = toCompact;
        this.maxFileNum = maxFileNum;
        this.minFileNum = minFileNum;
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
                        (inputs) ->
                                taskFuture =
                                        executor.submit(new AutoCompactTask(inputs, rewriter)));
    }

    @VisibleForTesting
    Optional<List<DataFileMeta>> pickCompactBefore() {
        return pick(toCompact, targetFileSize, minFileNum, maxFileNum);
    }

    private static Optional<List<DataFileMeta>> pick(
            LinkedList<DataFileMeta> toCompact,
            long targetFileSize,
            int minFileNum,
            int maxFileNum) {
        long totalFileSize = 0L;
        int fileNum = 0;
        LinkedList<DataFileMeta> candidates = new LinkedList<>();

        while (!toCompact.isEmpty()) {
            DataFileMeta file = toCompact.pollFirst();
            candidates.add(file);
            totalFileSize += file.fileSize();
            fileNum++;
            if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                    || fileNum >= maxFileNum) {
                return Optional.of(candidates);
            } else if (totalFileSize >= targetFileSize) {
                // let pointer shift one pos to right
                DataFileMeta removed = candidates.pollFirst();
                assert removed != null;
                totalFileSize -= removed.fileSize();
                fileNum--;
            }
        }
        toCompact.addAll(candidates);
        return Optional.empty();
    }

    @VisibleForTesting
    LinkedList<DataFileMeta> getToCompact() {
        return toCompact;
    }

    /** A {@link CompactTask} impl for ALTER TABLE COMPACT of append-only table. */
    public static class RollingCompactTask extends CompactTask {

        private final long targetFileSize;
        private final int minFileNum;
        private final int maxFileNum;
        private final CompactRewriter rewriter;

        public RollingCompactTask(
                List<DataFileMeta> inputs,
                long targetFileSize,
                int minFileNum,
                int maxFileNum,
                CompactRewriter rewriter) {
            super(inputs);
            this.targetFileSize = targetFileSize;
            this.minFileNum = minFileNum;
            this.maxFileNum = maxFileNum;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact(List<DataFileMeta> inputs) throws Exception {
            LinkedList<DataFileMeta> toCompact = new LinkedList<>(inputs);
            Set<DataFileMeta> compactBefore = new HashSet<>();
            List<DataFileMeta> compactAfter = new ArrayList<>();
            while (!toCompact.isEmpty()) {
                Optional<List<DataFileMeta>> candidates =
                        AppendOnlyCompactManager.pick(
                                toCompact, targetFileSize, minFileNum, maxFileNum);
                if (candidates.isPresent()) {
                    List<DataFileMeta> before = candidates.get();
                    compactBefore.addAll(before);
                    List<DataFileMeta> after = rewriter.rewrite(before);
                    compactAfter.addAll(after);
                    DataFileMeta lastFile = after.get(after.size() - 1);
                    if (lastFile.fileSize() < targetFileSize) {
                        toCompact.offerFirst(lastFile);
                    }
                } else {
                    break;
                }
            }
            // remove intermediate files
            Iterator<DataFileMeta> iterator = compactAfter.iterator();
            while (iterator.hasNext()) {
                DataFileMeta file = iterator.next();
                if (compactBefore.contains(file)) {
                    compactBefore.remove(file);
                    iterator.remove();
                }
            }
            return result(new ArrayList<>(compactBefore), compactAfter);
        }
    }

    /** A {@link CompactTask} impl for append-only table auto-compaction. */
    public static class AutoCompactTask extends CompactTask {

        private final CompactRewriter rewriter;

        public AutoCompactTask(List<DataFileMeta> toCompact, CompactRewriter rewriter) {
            super(toCompact);
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact(List<DataFileMeta> inputs) throws Exception {
            return result(inputs, rewriter.rewrite(inputs));
        }
    }

    private static CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
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

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }
}
