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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Metrics to measure a compaction. */
public class CompactionMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionMetrics.class);
    private static final int HISTOGRAM_WINDOW_SIZE = 10_000;
    private static final String GROUP_NAME = "compaction";

    private final MetricGroup metricGroup;

    public CompactionMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName);
        registerGenericCompactionMetrics();
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    private Histogram durationHistogram;
    private CompactionStats latestCompaction;

    @VisibleForTesting static final String LAST_COMPACTION_DURATION = "lastCompactionDuration";
    @VisibleForTesting static final String COMPACTION_DURATION = "compactionDuration";

    @VisibleForTesting
    static final String LAST_TABLE_FILES_COMPACTED_BEFORE = "lastTableFilesCompactedBefore";

    @VisibleForTesting
    static final String LAST_TABLE_FILES_COMPACTED_AFTER = "lastTableFilesCompactedAfter";

    @VisibleForTesting
    static final String LAST_CHANGELOG_FILES_COMPACTED = "lastChangelogFilesCompacted";

    @VisibleForTesting
    static final String LAST_REWRITE_INPUT_FILE_SIZE = "lastRewriteInputFileSize";

    @VisibleForTesting
    static final String LAST_REWRITE_OUTPUT_FILE_SIZE = "lastRewriteOutputFileSize";

    @VisibleForTesting
    static final String LAST_REWRITE_CHANGELOG_FILE_SIZE = "lastRewriteChangelogFileSize";

    private void registerGenericCompactionMetrics() {
        metricGroup.gauge(
                LAST_COMPACTION_DURATION,
                () -> latestCompaction == null ? 0L : latestCompaction.getDuration());
        durationHistogram = metricGroup.histogram(COMPACTION_DURATION, HISTOGRAM_WINDOW_SIZE);
        metricGroup.gauge(
                LAST_TABLE_FILES_COMPACTED_BEFORE,
                () ->
                        latestCompaction == null
                                ? 0L
                                : latestCompaction.getCompactedDataFilesBefore());
        metricGroup.gauge(
                LAST_TABLE_FILES_COMPACTED_AFTER,
                () ->
                        latestCompaction == null
                                ? 0L
                                : latestCompaction.getCompactedDataFilesAfter());
        metricGroup.gauge(
                LAST_CHANGELOG_FILES_COMPACTED,
                () -> latestCompaction == null ? 0L : latestCompaction.getCompactedChangelogs());
        metricGroup.gauge(
                LAST_REWRITE_INPUT_FILE_SIZE,
                () -> latestCompaction == null ? 0L : latestCompaction.getRewriteInputFileSize());
        metricGroup.gauge(
                LAST_REWRITE_OUTPUT_FILE_SIZE,
                () -> latestCompaction == null ? 0L : latestCompaction.getRewriteOutputFileSize());
        metricGroup.gauge(
                LAST_REWRITE_CHANGELOG_FILE_SIZE,
                () ->
                        latestCompaction == null
                                ? 0L
                                : latestCompaction.getRewriteChangelogFileSize());
    }

    public void reportCompaction(CompactionStats compactionStats) {
        latestCompaction = compactionStats;
        durationHistogram.update(compactionStats.getDuration());
    }
}
