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

import org.apache.flink.table.store.file.compact.CompactManager;
import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** Compact manager for {@link org.apache.flink.table.store.file.AppendOnlyFileStore}. */
public class AppendOnlyCompactManager extends CompactManager {

    private final int smallFileNumTrigger;
    private final long targetFileSize;
    private final CompactRewriter rewriter;

    private List<DataFileMeta> compactBefore;

    public AppendOnlyCompactManager(
            ExecutorService executor,
            int smallFileNumTrigger,
            long targetFileSize,
            CompactRewriter rewriter) {
        super(executor);
        this.smallFileNumTrigger = smallFileNumTrigger;
        this.targetFileSize = targetFileSize;
        this.compactBefore = new ArrayList<>();
        this.rewriter = rewriter;
    }

    @Override
    public void submitCompaction() {
        if (taskFuture != null) {
            throw new IllegalStateException(
                    "Please finish the previous compaction before submitting new one.");
        }
        if (compactBefore == null) {
            throw new IllegalStateException(
                    "Please update compactBefore before submitting a compaction");
        }
        if (triggerCompaction()) {
            taskFuture =
                    executor.submit(
                            new CompactTask() {
                                @Override
                                protected CompactResult compact() throws Exception {
                                    collectBeforeStats(compactBefore);
                                    List<DataFileMeta> compactAfter =
                                            rewriter.rewrite(compactBefore);
                                    collectAfterStats(compactAfter);
                                    return result(compactBefore, compactAfter);
                                }
                            });
        }
    }

    public void updateCompactBefore(List<DataFileMeta> files) {
        this.compactBefore = files;
    }

    private boolean triggerCompaction() {
        if (compactBefore.size() < smallFileNumTrigger) {
            return false;
        }
        int adjacentSmallFileCtr = 0;
        for (DataFileMeta file : compactBefore) {
            if (file.fileSize() < targetFileSize) {
                adjacentSmallFileCtr += 1;
            } else if (adjacentSmallFileCtr >= smallFileNumTrigger) {
                return true;
            } else {
                adjacentSmallFileCtr = 0;
            }
        }
        return adjacentSmallFileCtr > smallFileNumTrigger;
    }

    /** Compact rewriter for append-only table. */
    public interface CompactRewriter {
        List<DataFileMeta> rewrite(List<DataFileMeta> toCompact) throws Exception;
    }
}
