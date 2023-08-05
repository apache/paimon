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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.DescriptiveStatisticsHistogram;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.groups.GenericMetricGroup;
import org.apache.paimon.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Metrics to measure a commit. */
public class CommitMetrics {
    private static Logger log = LoggerFactory.getLogger(CommitMetrics.class);
    private static final int HISTOGRAM_WINDOW_SIZE = 10_000;
    protected static final String GROUP_NAME = "commit";

    private final MetricGroup genericMetricGroup;

    private final FileStorePathFactory pathFactory;
    private final FileIO fileIO;

    private long initTableFilesCount = 0;
    private long initChangelogFilesCount = 0;

    public CommitMetrics(FileStorePathFactory pathFactory, FileIO fileIO) {
        this.genericMetricGroup =
                GenericMetricGroup.createGenericMetricGroup(
                        pathFactory.root().getName(), GROUP_NAME);
        this.pathFactory = pathFactory;
        this.fileIO = fileIO;
        initDataFilesCount();
        registerGenericCommitMetrics();
    }

    @VisibleForTesting
    void initDataFilesCount() {
        try {
            List<Path> dirs =
                    Arrays.stream(fileIO.listStatus(pathFactory.root()))
                            .map(f -> f.getPath())
                            .collect(Collectors.toList());
            boolean hasPartition = true;
            for (Path dir : dirs) {
                if (dir.getName().startsWith("bucket-")) {
                    hasPartition = false;
                    break;
                }
            }
            if (hasPartition) {
                List<Path> buckets = new ArrayList<>();
                for (Path dir : dirs) {
                    FileStatus[] fileStatuses = fileIO.listStatus(dir);
                    buckets.addAll(
                            Arrays.stream(fileStatuses)
                                    .filter(
                                            f ->
                                                    f.isDir()
                                                            && f.getPath()
                                                                    .getName()
                                                                    .startsWith("bucket-"))
                                    .map(f -> f.getPath())
                                    .collect(Collectors.toList()));
                }
                for (Path bucket : buckets) {
                    FileStatus[] fileStatuses = fileIO.listStatus(bucket);
                    accFilesCount(fileStatuses);
                }
            } else {
                for (Path dir : dirs) {
                    FileStatus[] fileStatuses = fileIO.listStatus(dir);
                    accFilesCount(fileStatuses);
                }
            }
        } catch (IOException ie) {
            log.warn(
                    "List table files failed, the 'total' prefixed commit metrics will calculate the number of total files since the job started. ");
        }
    }

    private void accFilesCount(FileStatus[] fileStatuses) {
        initTableFilesCount +=
                Arrays.stream(fileStatuses)
                        .filter(
                                f ->
                                        f.getPath()
                                                .getName()
                                                .startsWith(DataFilePathFactory.DATA_FILE_PREFIX))
                        .count();
        initChangelogFilesCount +=
                Arrays.stream(fileStatuses)
                        .filter(
                                f ->
                                        f.getPath()
                                                .getName()
                                                .startsWith(
                                                        DataFilePathFactory.CHANGELOG_FILE_PREFIX))
                        .count();
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
        totalTableFilesCounter = genericMetricGroup.counter(TOTAL_TABLE_FILES);
        totalTableFilesCounter.inc(initTableFilesCount);
        totalChangelogFilesCounter = genericMetricGroup.counter(TOTAL_CHANGELOG_FILES);
        totalChangelogFilesCounter.inc(initChangelogFilesCount);
    }

    public void reportCommit(CommitStats commitStats) {
        latestCommit = commitStats;
        totalTableFilesCounter.inc(
                commitStats.getTableFilesAdded() - commitStats.getTableFilesDeleted());
        totalChangelogFilesCounter.inc(
                commitStats.getChangelogFilesAppended() + commitStats.getChangelogFilesCompacted());
        durationHistogram.update(commitStats.getDuration());
    }

    @VisibleForTesting
    long getInitTableFilesCount() {
        return initTableFilesCount;
    }

    @VisibleForTesting
    long getInitChangelogFilesCount() {
        return initChangelogFilesCount;
    }

    @VisibleForTesting
    FileStorePathFactory getPathFactory() {
        return pathFactory;
    }

    @VisibleForTesting
    FileIO getFileIO() {
        return fileIO;
    }
}
