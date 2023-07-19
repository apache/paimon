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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.DescriptiveStatisticsHistogram;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.groups.BucketMetricGroup;
import org.apache.paimon.metrics.groups.GenericMetricGroup;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Metrics to measure a commit. */
public class CommitMetrics {
    private static final int HISTOGRAM_WINDOW_SIZE = 10_000;
    private final Map<String, BucketMetricGroup> bucketMetricGroups = new HashMap<>();

    private final String groupKeyFormat = "%s-%s";
    private final MetricGroup genericMetricGroup;
    private final FileStorePathFactory pathFactory;

    public CommitMetrics(FileStorePathFactory pathFactory) {
        this.pathFactory = pathFactory;
        this.genericMetricGroup = new GenericMetricGroup(pathFactory.root().getName());
        registerGenericCommitMetrics();
    }

    public Map<String, BucketMetricGroup> getBucketMetricGroups() {
        return bucketMetricGroups;
    }

    public MetricGroup getGenericMetricGroup() {
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

    @VisibleForTesting static final String TOTAL_TABLE_FILES = "totalTableFiles";
    @VisibleForTesting static final String TOTAL_CHANGELOG_FILES = "totalChangelogFiles";
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

    transient Counter totalTableFilesCounter;
    transient Counter totalChangelogFilesCounter;

    private void registerGenericCommitMetrics() {
        genericMetricGroup.gauge(LAST_COMMIT_DURATION, new LatestDurationGauge());
        genericMetricGroup.gauge(LAST_COMMIT_ATTEMPTS, new LatestAttemptsGauge());
        genericMetricGroup.gauge(LAST_GENERATED_SNAPSHOTS, new LatestGeneratedSnapshotGauge());
        genericMetricGroup.gauge(LAST_PARTITIONS_WRITTEN, new LatestPartitionsWrittenGauge());
        genericMetricGroup.gauge(LAST_BUCKETS_WRITTEN, new LatestBucketsWrittenGauge());
        genericMetricGroup.histogram(COMMIT_DURATION, durationHistogram);
        totalTableFilesCounter = genericMetricGroup.counter(TOTAL_TABLE_FILES);
        totalChangelogFilesCounter = genericMetricGroup.counter(TOTAL_CHANGELOG_FILES);
    }

    private void registerBucketCommitMetrics() {
        if (latestCommit != null) {
            Map<BinaryRow, Set<Integer>> partBuckets = latestCommit.getPartBucketsWritten();
            for (Map.Entry<BinaryRow, Set<Integer>> kv : partBuckets.entrySet()) {
                BinaryRow partition = kv.getKey();
                String partitionStr = getPartitionString(partition);
                for (Integer bucket : kv.getValue()) {
                    String groupKey = String.format(groupKeyFormat, partitionStr, bucket);
                    if (!bucketMetricGroups.containsKey(groupKey)) {
                        BucketMetricGroup group =
                                bucketMetricGroups.compute(
                                        groupKey,
                                        (k, v) ->
                                                BucketMetricGroup.createBucketMetricGroup(
                                                        pathFactory.root().getName(),
                                                        bucket,
                                                        partitionStr));
                        group.gauge(
                                LAST_TABLE_FILES_ADDED,
                                () -> latestCommit.getBucketedTableFilesAdded(partition, bucket));
                        group.gauge(
                                LAST_TABLE_FILES_DELETED,
                                () -> latestCommit.getBucketedTableFilesDeleted(partition, bucket));
                        group.gauge(
                                LAST_TABLE_FILES_APPENDED,
                                () ->
                                        latestCommit.getBucketedTableFilesAppended(
                                                partition, bucket));
                        group.gauge(
                                LAST_TABLE_FILES_COMMIT_COMPACTED,
                                () ->
                                        latestCommit.getBucketedTableFilesCompacted(
                                                partition, bucket));
                        group.gauge(
                                LAST_CHANGELOG_FILES_APPENDED,
                                () ->
                                        latestCommit.getBucketedChangelogFilesAppended(
                                                partition, bucket));
                        group.gauge(
                                LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                                () ->
                                        latestCommit.getBucketedChangelogFilesCompacted(
                                                partition, bucket));
                        group.gauge(
                                LAST_DELTA_RECORDS_APPENDED,
                                () ->
                                        latestCommit.getBucketedDeltaRecordsAppended(
                                                partition, bucket));
                        group.gauge(
                                LAST_CHANGELOG_RECORDS_APPENDED,
                                () ->
                                        latestCommit.getBucketedChangelogRecordsAppended(
                                                partition, bucket));
                        group.gauge(
                                LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                                () ->
                                        latestCommit.getBucketedDeltaRecordsCompacted(
                                                partition, bucket));
                        group.gauge(
                                LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED,
                                () ->
                                        latestCommit.getBucketedChangelogRecordsCompacted(
                                                partition, bucket));
                    }
                }
            }
        }
    }

    private String getPartitionString(BinaryRow partition) {
        String partitionStr = pathFactory.getPartitionString(partition);
        return partitionStr.replace(Path.SEPARATOR, "-").substring(0, partitionStr.length() - 1);
    }

    private class LatestDurationGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CommitStats commit = latestCommit;
            if (commit != null) {
                return commit.getDuration();
            } else {
                return 0L;
            }
        }
    }

    private class LatestAttemptsGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CommitStats commit = latestCommit;
            if (commit != null) {
                return commit.getAttempts();
            } else {
                return 0L;
            }
        }
    }

    private class LatestGeneratedSnapshotGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CommitStats commit = latestCommit;
            if (commit != null) {
                return commit.getGeneratedSnapshots();
            } else {
                return 0L;
            }
        }
    }

    private class LatestPartitionsWrittenGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CommitStats commit = latestCommit;
            if (commit != null) {
                return commit.getNumPartitionsWritten();
            } else {
                return 0L;
            }
        }
    }

    private class LatestBucketsWrittenGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CommitStats commit = latestCommit;
            if (commit != null) {
                return commit.getNumBucketsWritten();
            } else {
                return 0L;
            }
        }
    }

    public void reportCommit(CommitStats commitStats) {
        latestCommit = commitStats;
        totalTableFilesCounter.inc(
                commitStats.getTableFilesAdded() - commitStats.getTableFilesDeleted());
        totalChangelogFilesCounter.inc(
                commitStats.getChangelogFilesCommitAppended()
                        + commitStats.getChangelogFilesCompacted());
        durationHistogram.update(commitStats.getDuration());
        registerBucketCommitMetrics();
    }
}
