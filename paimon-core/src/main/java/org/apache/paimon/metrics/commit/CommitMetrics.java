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

package org.apache.paimon.metrics.commit;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.metrics.AbstractMetricGroup;
import org.apache.paimon.metrics.DescriptiveStatisticsHistogram;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.groups.GenericMetricGroup;

/** Metrics to measure a commit. */
public class CommitMetrics {
    private static final int HISTOGRAM_WINDOW_SIZE = 10_000;
    protected static final String GROUP_NAME = "commit";

    private final AbstractMetricGroup genericMetricGroup;

    public CommitMetrics(String tableName) {
        this.genericMetricGroup =
                GenericMetricGroup.createGenericMetricGroup(tableName, GROUP_NAME);
        registerGenericCommitMetrics();
    }

    @VisibleForTesting
    public AbstractMetricGroup getMetricGroup() {
        return genericMetricGroup;
    }

    private final Histogram durationHistogram =
            new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE);

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

    private void registerGenericCommitMetrics() {
        genericMetricGroup.gauge(
                LAST_COMMIT_DURATION, () -> latestCommit == null ? 0L : latestCommit.getDuration());
        genericMetricGroup.gauge(
                LAST_COMMIT_ATTEMPTS, () -> latestCommit == null ? 0L : latestCommit.getAttempts());
        genericMetricGroup.gauge(
                LAST_GENERATED_SNAPSHOTS,
                () -> latestCommit == null ? 0L : latestCommit.getGeneratedSnapshots());
        genericMetricGroup.gauge(
                LAST_PARTITIONS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumPartitionsWritten());
        genericMetricGroup.gauge(
                LAST_BUCKETS_WRITTEN,
                () -> latestCommit == null ? 0L : latestCommit.getNumBucketsWritten());
        genericMetricGroup.histogram(COMMIT_DURATION, durationHistogram);
        genericMetricGroup.gauge(
                LAST_TABLE_FILES_ADDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAdded());
        genericMetricGroup.gauge(
                LAST_TABLE_FILES_DELETED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesDeleted());
        genericMetricGroup.gauge(
                LAST_TABLE_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesAppended());
        genericMetricGroup.gauge(
                LAST_TABLE_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getTableFilesCompacted());
        genericMetricGroup.gauge(
                LAST_CHANGELOG_FILES_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesAppended());
        genericMetricGroup.gauge(
                LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogFilesCompacted());
        genericMetricGroup.gauge(
                LAST_DELTA_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsAppended());
        genericMetricGroup.gauge(
                LAST_CHANGELOG_RECORDS_APPENDED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsAppended());
        genericMetricGroup.gauge(
                LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getDeltaRecordsCompacted());
        genericMetricGroup.gauge(
                LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED,
                () -> latestCommit == null ? 0L : latestCommit.getChangelogRecordsCompacted());
    }

    public void reportCommit(CommitStats commitStats) {
        latestCommit = commitStats;
        durationHistogram.update(commitStats.getDuration());
    }

    public void close() {
        this.genericMetricGroup.close();
    }
}
