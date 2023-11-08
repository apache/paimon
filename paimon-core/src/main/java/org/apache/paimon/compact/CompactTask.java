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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.CompactionStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/** Compact task. */
public abstract class CompactTask implements Callable<CompactResult> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactTask.class);
    @Nullable private final CompactionMetrics metrics;

    public CompactTask(@Nullable CompactionMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public CompactResult call() throws Exception {
        long startMillis = System.currentTimeMillis();
        CompactResult result = null;
        try {
            result = doCompact();

            if (LOG.isDebugEnabled()) {
                logMetric(startMillis, result.before(), result.after());
            }
            return result;
        } finally {
            if (metrics != null) {
                long duration = System.currentTimeMillis() - startMillis;
                CompactionStats compactionStats =
                        result == null
                                ? new CompactionStats(
                                        duration,
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        Collections.emptyList())
                                : new CompactionStats(
                                        duration,
                                        result.before(),
                                        result.after(),
                                        result.changelog());
                metrics.reportCompaction(compactionStats);
            }
        }
    }

    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "Done compacting %d files to %d files in %dms. "
                        + "Rewrite input file size = %d, output file size = %d",
                compactBefore.size(),
                compactAfter.size(),
                System.currentTimeMillis() - startMillis,
                collectRewriteSize(compactBefore),
                collectRewriteSize(compactAfter));
    }

    /**
     * Perform compaction.
     *
     * @return {@link CompactResult} of compact before and compact after files.
     */
    protected abstract CompactResult doCompact() throws Exception;

    private long collectRewriteSize(List<DataFileMeta> files) {
        return files.stream().mapToLong(DataFileMeta::fileSize).sum();
    }
}
