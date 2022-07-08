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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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
        taskFuture =
                executor.submit(
                        new AppendOnlyCompactTask(
                                toCompact, minFileNum, maxFileNum, targetFileSize, rewriter));
    }

    /** A {@link CompactTask} impl for append-only table. */
    public static class AppendOnlyCompactTask extends CompactTask {

        private final int minFileNum;
        private final int maxFileNum;
        private final long targetFileSize;
        private final CompactRewriter rewriter;

        public AppendOnlyCompactTask(
                LinkedList<DataFileMeta> toCompact,
                int minFileNum,
                int maxFileNum,
                long targetFileSize,
                CompactRewriter rewriter) {
            super(toCompact);
            this.minFileNum = minFileNum;
            this.maxFileNum = maxFileNum;
            this.targetFileSize = targetFileSize;
            this.rewriter = rewriter;
        }

        @Override
        protected CompactResult doCompact(List<DataFileMeta> toCompact) throws Exception {
            List<DataFileMeta> compactBefore = pickCompactBefore().orElse(Collections.emptyList());
            List<DataFileMeta> compactAfter = rewriter.rewrite(compactBefore);
            return new CompactResult() {
                @Override
                public List<DataFileMeta> before() {
                    return compactBefore;
                }

                @Override
                public List<DataFileMeta> after() {
                    return compactAfter;
                }
            };
        }

        @VisibleForTesting
        Optional<List<DataFileMeta>> pickCompactBefore() {
            long totalFileSize = 0L;
            int fileNum = 0;
            LinkedList<DataFileMeta> candidates = new LinkedList<>();

            while (!toCompact.isEmpty()) {
                DataFileMeta file = ((LinkedList<DataFileMeta>) toCompact).pollFirst();
                candidates.add(file);
                totalFileSize += file.fileSize();
                fileNum++;
                if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                        || fileNum >= maxFileNum) {
                    return Optional.of(candidates);
                } else if (totalFileSize >= targetFileSize) {
                    // left pointer shift one pos to right
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
            return (LinkedList<DataFileMeta>) toCompact;
        }
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> compactBefore) throws Exception;
    }
}
