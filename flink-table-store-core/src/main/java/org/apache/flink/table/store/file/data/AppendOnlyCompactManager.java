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
        long totalFileSize = 0L;
        int fileNum = 0;
        int releaseCtr = 0;
        for (int i = 0; i < toCompact.size(); i++) {
            DataFileMeta file = toCompact.get(i);
            totalFileSize += file.fileSize();
            fileNum++;
            int pos = i - fileNum + 1;
            if ((totalFileSize >= targetFileSize && fileNum >= minFileNum)
                    || fileNum >= maxFileNum) {
                // trigger compaction for [pos, i]
                List<DataFileMeta> compactBefore = new ArrayList<>(toCompact.subList(pos, i + 1));
                // files in [0, pos - 1] can be released immediately
                // [pos, i] should be released after compaction finished
                for (int j = 0; j <= pos - 1; j++) {
                    toCompact.pollFirst();
                }
                return Optional.of(compactBefore);
            } else if (totalFileSize >= targetFileSize) {
                // this is equivalent to shift one pos to right
                fileNum--;
                totalFileSize -= toCompact.get(pos).fileSize();
                releaseCtr++;
            }
        }
        while (releaseCtr > 0) {
            toCompact.pollFirst();
            releaseCtr--;
        }
        // optimize when the last file size >= targetFileSize
        DataFileMeta head = toCompact.peekFirst();
        if (head != null && head.fileSize() >= targetFileSize) {
            toCompact.pollFirst();
        }
        return Optional.empty();
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
