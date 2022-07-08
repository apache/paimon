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

package org.apache.flink.table.store.file.compact;

import org.apache.flink.table.store.file.data.DataFileMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/** Compact task. */
public abstract class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    protected final List<DataFileMeta> compactBefore;

    protected final List<DataFileMeta> compactAfter;

    // metrics
    private long rewriteInputSize;
    private long rewriteOutputSize;

    public CompactTask() {
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.rewriteInputSize = 0;
        this.rewriteOutputSize = 0;
    }

    @Override
    public CompactResult call() throws Exception {
        long startMillis = System.currentTimeMillis();
        doCompact();
        if (LOG.isDebugEnabled()) {
            collectBeforeStats();
            collectAfterStats();
            LOG.debug(logMetric(startMillis));
        }
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

    protected String logMetric(long startMillis) {
        return String.format(
                "Done compacting %d files to %d files in %dms. "
                        + "Rewrite input file size = %d, output file size = %d",
                compactBefore.size(),
                compactAfter.size(),
                System.currentTimeMillis() - startMillis,
                rewriteInputSize,
                rewriteOutputSize);
    }

    protected abstract void doCompact() throws Exception;

    private void collectBeforeStats() {
        compactBefore.forEach(file -> rewriteInputSize += file.fileSize());
    }

    private void collectAfterStats() {
        rewriteOutputSize += compactAfter.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
