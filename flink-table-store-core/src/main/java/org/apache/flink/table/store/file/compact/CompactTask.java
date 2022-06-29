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

import java.util.List;
import java.util.concurrent.Callable;

/** Compact task. */
public abstract class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);

    // metrics
    protected long rewriteInputSize;
    protected long rewriteOutputSize;
    protected int rewriteFilesNum;

    public CompactTask() {
        this.rewriteInputSize = 0;
        this.rewriteOutputSize = 0;
        this.rewriteFilesNum = 0;
    }

    @Override
    public CompactResult call() throws Exception {
        long startMillis = System.currentTimeMillis();
        CompactResult result = compact();
        if (LOG.isDebugEnabled()) {
            LOG.debug(logMetric(startMillis, result));
        }
        return result;
    }

    protected String logMetric(long startMillis, CompactResult result) {
        return String.format(
                "Done compacting %d files to %d files in %dms. "
                        + "Rewrite input size = %d, output size = %d, rewrite file num = %d",
                result.before().size(),
                result.after().size(),
                System.currentTimeMillis() - startMillis,
                rewriteInputSize,
                rewriteOutputSize,
                rewriteFilesNum);
    }

    protected abstract CompactResult compact() throws Exception;

    protected CompactResult result(List<DataFileMeta> before, List<DataFileMeta> after) {
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

    protected void collectBeforeStats(List<DataFileMeta> before) {
        before.forEach(file -> rewriteInputSize += file.fileSize());
        rewriteFilesNum += before.size();
    }

    protected void collectAfterStats(List<DataFileMeta> after) {
        rewriteOutputSize += after.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
