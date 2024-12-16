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

/** Metrics to measure a commit. */
public class CommitMetrics {

    private static final int HISTOGRAM_WINDOW_SIZE = 100;
    private static final String GROUP_NAME = "commit";

    private final MetricGroup metricGroup;

    public CommitMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName);
        registerGenericCommitMetrics();
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    private Histogram durationHistogram;
    private CommitStats latestCommit;

    @VisibleForTesting static final String LAST_COMMIT_DURATION = "lastCommitDuration";
    @VisibleForTesting static final String COMMIT_DURATION = "commitDuration";
    @VisibleForTesting static final String LAST_COMMIT_ATTEMPTS = "lastCommitAttempts";
    @VisibleForTesting static final String LAST_TABLE_FILES_ADDED = "lastTableFilesAdded";
    @VisibleForTesting static final String LAST_TABLE_FILES_DELETED = "lastTableFilesDeleted";
    @VisibleForTesting static final String LAST_TABLE_FILES_APPENDED = "lastTableFilesAppended";

    @VisibleForTesting
    static final String LAST_TABLE_FILES_COMMIT_COMPACTED = "lastTableFilesCommitCompacted";

    @VisibleForTesting
    static final String LAST_CHANGELOG_FILES_APPENDED = "lastChangelogFilesAppended";

    @VisibleForTesting
    static final String LAST_CHANGELOG_FILES_COMMIT_COMPACTED = "lastChangelogFileCommitCompacted";

    @VisibleForTesting static final String LAST_GENERATED_SNAPSHOTS = "lastGeneratedSnapshots";
    @VisibleForTesting static final String LAST_DELTA_RECORDS_APPENDED = "lastDeltaRecordsAppended";

    @VisibleForTesting
    static final String LAST_CHANGELOG_RECORDS_APPENDED = "lastChangelogRecordsAppended";

    @VisibleForTesting
    static final String LAST_DELTA_RECORDS_COMMIT_COMPACTED = "lastDeltaRecordsCommitCompacted";

    @VisibleForTesting
    static final String LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED =
            "lastChangelogRecordsCommitCompacted";

    @VisibleForTesting static final String LAST_PARTITIONS_WRITTEN = "lastPartitionsWritten";
    @VisibleForTesting static final String LAST_BUCKETS_WRITTEN = "lastBucketsWritten";

    static final String LAST_COMPACTION_INPUT_FILE_SIZE = "lastCompactionInputFileSize";
    static final String LAST_COMPACTION_OUTPUT_FILE_SIZE = "lastCompactionOutputFileSize";

    private void registerGenericCommitMetrics() {
        metricGroup.gauge(
                LAST_COMMIT_DURATION, () -> latestCommit == null ? 0L : latestCommit.getDuration());
        metricGroup.gauge(
                LAST_COMMIT_ATTEMPTS, () -> latestCommit == null ? 0L : latestCommit.getAttempts());
        metricGroup.gauge(
                LAST_GENERATED_SNAPSHOTS,
                () -> latestCommit == null ? 0L : latestCommit.getGeneratedSnapshots());
        metricGroup.gauge(
                LAST_PARTITIONS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumPartitionsWritten());
        metricGroup.gauge(
                LAST_BUCKETS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumBucketsWritten());
        durationHistogram = metricGroup.histogram(COMMIT_DURATION, HISTOGRAM_WINDOW_SIZE);
        metricGroup.gauge(
                LAST_TABLE_FILES_ADDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAdded());
        metricGroup.gauge(
                LAST_TABLE_FILES_DELETED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesDeleted());
        metricGroup.gauge(
                LAST_TABLE_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAppended());
        metricGroup.gauge(
                LAST_TABLE_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesCompacted());
        metricGroup.gauge(
                LAST_CHANGELOG_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesAppended());
        metricGroup.gauge(
                LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesCompacted());
        metricGroup.gauge(
                LAST_DELTA_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsAppended());
        metricGroup.gauge(
                LAST_CHANGELOG_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsAppended());
        metricGroup.gauge(
                LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsCompacted());
        metricGroup.gauge(
                LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsCompacted());
        metricGroup.gauge(
                LAST_COMPACTION_INPUT_FILE_SIZE,
                () -> latestCommit == null ? 0L : latestCommit.getCompactionInputFileSize());
        metricGroup.gauge(
                LAST_COMPACTION_OUTPUT_FILE_SIZE,
                () -> latestCommit == null ? 0L : latestCommit.getCompactionOutputFileSize());
    }

    public void reportCommit(CommitStats commitStats) {
        latestCommit = commitStats;
        durationHistogram.update(commitStats.getDuration());
    }
}
