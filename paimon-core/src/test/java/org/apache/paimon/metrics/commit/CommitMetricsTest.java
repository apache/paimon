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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.Metrics;
import org.apache.paimon.metrics.groups.BucketMetricGroup;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.manifest.ManifestFileMetaTestBase.makeEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CommitMetrics}. */
public class CommitMetricsTest {

    @TempDir static java.nio.file.Path tempDir;
    private static final String TABLE_NAME = "myTable";

    private CommitMetrics commitMetrics;

    @BeforeEach
    public void beforeEach() {
        Metrics.getInstance().getMetricGroups().clear();
        commitMetrics = getCommitMetrics();
    }

    /** Tests the registration of the commit metrics. */
    @Test
    public void testGenericMetricsRegistration() {
        MetricGroup group = commitMetrics.getGenericMetricGroup();
        Map<String, BucketMetricGroup> taggedMetricGroups = commitMetrics.getBucketMetricGroups();

        assertEquals(1, Metrics.getInstance().getMetricGroups().size());
        assertEquals(group, Metrics.getInstance().getMetricGroups().get(0));
        assertEquals("table", group.getGroupName());

        Map<String, Metric> registeredMetrics = group.getMetrics();
        assertTrue(
                registeredMetrics
                        .keySet()
                        .containsAll(
                                Arrays.asList(
                                        CommitMetrics.LAST_COMMIT_DURATION,
                                        CommitMetrics.LAST_COMMIT_ATTEMPTS,
                                        CommitMetrics.LAST_GENERATED_SNAPSHOTS,
                                        CommitMetrics.LAST_PARTITIONS_WRITTEN,
                                        CommitMetrics.LAST_BUCKETS_WRITTEN,
                                        CommitMetrics.COMMIT_DURATION,
                                        CommitMetrics.TOTAL_TABLE_FILES,
                                        CommitMetrics.TOTAL_CHANGELOG_FILES)));
        assertEquals(8, registeredMetrics.size());

        reportOnce(commitMetrics);

        assertEquals(3, taggedMetricGroups.size());

        List<MetricGroup> registeredGroups = Metrics.getInstance().getMetricGroups();
        assertEquals(4, registeredGroups.size());
        BucketMetricGroup taggedGroup = taggedMetricGroups.get("f0=1-1");
        assertEquals(taggedGroup, registeredGroups.get(1));
        assertEquals(BucketMetricGroup.GROUP_NAME, taggedGroup.getGroupName());

        Map<String, Metric> registeredMetricsByBucket1 = taggedGroup.getMetrics();
        assertTrue(
                registeredMetricsByBucket1
                        .keySet()
                        .containsAll(
                                Arrays.asList(
                                        CommitMetrics.LAST_TABLE_FILES_ADDED,
                                        CommitMetrics.LAST_TABLE_FILES_DELETED,
                                        CommitMetrics.LAST_TABLE_FILES_APPENDED,
                                        CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED,
                                        CommitMetrics.LAST_CHANGELOG_FILES_APPENDED,
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED,
                                        CommitMetrics.LAST_DELTA_RECORDS_APPENDED,
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED,
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED,
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED)));
        assertEquals(10, registeredMetricsByBucket1.size());

        reportAgain(commitMetrics);

        assertEquals(4, taggedMetricGroups.size());
        assertEquals(5, Metrics.getInstance().getMetricGroups().size());
    }

    /** Tests that the metrics are updated properly. */
    @Test
    @SuppressWarnings("unchecked")
    public void testMetricsAreUpdated() {
        Map<String, Metric> registeredGenericMetrics =
                commitMetrics.getGenericMetricGroup().getMetrics();
        Map<String, BucketMetricGroup> taggedMetricGroups = commitMetrics.getBucketMetricGroups();

        // Check initial values
        Gauge<Long> lastCommitDuration =
                (Gauge<Long>) registeredGenericMetrics.get(CommitMetrics.LAST_COMMIT_DURATION);
        Histogram commitDuration =
                (Histogram) registeredGenericMetrics.get(CommitMetrics.COMMIT_DURATION);
        Gauge<Long> lastCommitAttempts =
                (Gauge<Long>) registeredGenericMetrics.get(CommitMetrics.LAST_COMMIT_ATTEMPTS);
        Gauge<Long> lastGeneratedSnapshots =
                (Gauge<Long>) registeredGenericMetrics.get(CommitMetrics.LAST_GENERATED_SNAPSHOTS);
        Gauge<Long> lastPartitionsWritten =
                (Gauge<Long>) registeredGenericMetrics.get(CommitMetrics.LAST_PARTITIONS_WRITTEN);
        Gauge<Long> lastBucketsWritten =
                (Gauge<Long>) registeredGenericMetrics.get(CommitMetrics.LAST_BUCKETS_WRITTEN);
        Counter totalTableFiles =
                (Counter) registeredGenericMetrics.get(CommitMetrics.TOTAL_TABLE_FILES);
        Counter totalChangelogFiles =
                (Counter) registeredGenericMetrics.get(CommitMetrics.TOTAL_CHANGELOG_FILES);

        assertEquals(Long.valueOf(0), lastCommitDuration.getValue());
        assertEquals(0, commitDuration.getCount());
        assertEquals(0, commitDuration.getStatistics().size());
        assertEquals(Long.valueOf(0), lastCommitAttempts.getValue());
        assertEquals(Long.valueOf(0), lastGeneratedSnapshots.getValue());
        assertEquals(Long.valueOf(0), lastPartitionsWritten.getValue());
        assertEquals(Long.valueOf(0), lastBucketsWritten.getValue());
        assertEquals(0, totalTableFiles.getCount());
        assertEquals(0, totalChangelogFiles.getCount());
        assertEquals(0, taggedMetricGroups.size());

        // report once
        reportOnce(commitMetrics);

        // generic metrics value updated
        assertEquals(Long.valueOf(200), lastCommitDuration.getValue());
        assertEquals(1, commitDuration.getCount());
        assertEquals(1, commitDuration.getStatistics().size());
        assertEquals(200L, commitDuration.getStatistics().getValues()[0]);
        assertEquals(200, commitDuration.getStatistics().getMin());
        assertEquals(200.0, commitDuration.getStatistics().getQuantile(0.5));
        assertEquals(200, commitDuration.getStatistics().getMean());
        assertEquals(200, commitDuration.getStatistics().getMax());
        assertEquals(0, commitDuration.getStatistics().getStdDev());
        assertEquals(Long.valueOf(1), lastCommitAttempts.getValue());
        assertEquals(Long.valueOf(2), lastGeneratedSnapshots.getValue());
        assertEquals(Long.valueOf(3), lastPartitionsWritten.getValue());
        assertEquals(Long.valueOf(3), lastBucketsWritten.getValue());
        assertEquals(3, totalTableFiles.getCount());
        assertEquals(4, totalChangelogFiles.getCount());
        assertTrue(
                taggedMetricGroups
                        .keySet()
                        .containsAll(Arrays.asList("f0=1-1", "f0=2-3", "f0=3-5")));

        Map<String, Metric> metricsTagged1 = taggedMetricGroups.get("f0=1-1").getMetrics();
        assertEquals(
                Long.valueOf(2),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(201),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(202),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(203),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(205),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        Map<String, Metric> metricsTagged3 = taggedMetricGroups.get("f0=2-3").getMetrics();
        assertEquals(
                Long.valueOf(2),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(302),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(301),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(304),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(307),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        Map<String, Metric> metricsTagged5 = taggedMetricGroups.get("f0=3-5").getMetrics();
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(106),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        // report again
        reportAgain(commitMetrics);

        // generic metrics value updated
        assertEquals(Long.valueOf(500), lastCommitDuration.getValue());
        assertEquals(2, commitDuration.getCount());
        assertEquals(2, commitDuration.getStatistics().size());
        assertEquals(500L, commitDuration.getStatistics().getValues()[1]);
        assertEquals(200, commitDuration.getStatistics().getMin());
        assertEquals(350.0, commitDuration.getStatistics().getQuantile(0.5));
        assertEquals(350, commitDuration.getStatistics().getMean());
        assertEquals(500, commitDuration.getStatistics().getMax());
        assertEquals(212, Math.round(commitDuration.getStatistics().getStdDev()));
        assertEquals(Long.valueOf(2), lastCommitAttempts.getValue());
        assertEquals(Long.valueOf(1), lastGeneratedSnapshots.getValue());
        assertEquals(Long.valueOf(2), lastPartitionsWritten.getValue());
        assertEquals(Long.valueOf(3), lastBucketsWritten.getValue());
        assertEquals(6, totalTableFiles.getCount());
        assertEquals(8, totalChangelogFiles.getCount());
        assertEquals(4, taggedMetricGroups.size());

        assertTrue(
                taggedMetricGroups
                        .keySet()
                        .containsAll(Arrays.asList("f0=1-1", "f0=2-3", "f0=3-5", "f0=3-4")));

        assertEquals(
                Long.valueOf(2),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(400),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(102),
                ((Gauge<Long>) metricsTagged1.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(200),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(300),
                ((Gauge<Long>)
                                metricsTagged1.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged3.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged3.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        Map<String, Metric> metricsBucket4 = taggedMetricGroups.get("f0=3-4").getMetrics();
        assertEquals(
                Long.valueOf(2),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>)
                                metricsBucket4.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(405),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(111),
                ((Gauge<Long>) metricsBucket4.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(201),
                ((Gauge<Long>)
                                metricsBucket4.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(301),
                ((Gauge<Long>)
                                metricsBucket4.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());

        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_ADDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_DELETED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(1),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_TABLE_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_CHANGELOG_FILES_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_CHANGELOG_FILES_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_DELTA_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>) metricsTagged5.get(CommitMetrics.LAST_CHANGELOG_RECORDS_APPENDED))
                        .getValue());
        assertEquals(
                Long.valueOf(105),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_DELTA_RECORDS_COMMIT_COMPACTED))
                        .getValue());
        assertEquals(
                Long.valueOf(0),
                ((Gauge<Long>)
                                metricsTagged5.get(
                                        CommitMetrics.LAST_CHANGELOG_RECORDS_COMMIT_COMPACTED))
                        .getValue());
    }

    private void reportOnce(CommitMetrics commitMetrics) {
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelogFiles = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelogFiles = new ArrayList<>();

        appendTableFiles.add(makeEntry(FileKind.ADD, 1, 1, 201));
        appendTableFiles.add(makeEntry(FileKind.ADD, 2, 3, 302));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 202));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 2, 3, 301));
        compactTableFiles.add(makeEntry(FileKind.ADD, 1, 1, 203));
        compactTableFiles.add(makeEntry(FileKind.ADD, 2, 3, 304));
        compactTableFiles.add(makeEntry(FileKind.DELETE, 3, 5, 106));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 205));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 2, 3, 307));

        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        200,
                        2,
                        1);

        commitMetrics.reportCommit(commitStats);
    }

    private void reportAgain(CommitMetrics commitMetrics) {
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelogFiles = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelogFiles = new ArrayList<>();

        appendTableFiles.add(makeEntry(FileKind.ADD, 1, 1, 400));
        appendTableFiles.add(makeEntry(FileKind.ADD, 3, 4, 405));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 102));
        appendChangelogFiles.add(makeEntry(FileKind.ADD, 3, 4, 111));
        compactTableFiles.add(makeEntry(FileKind.ADD, 1, 1, 200));
        compactTableFiles.add(makeEntry(FileKind.ADD, 3, 4, 201));
        compactTableFiles.add(makeEntry(FileKind.DELETE, 3, 5, 105));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 1, 1, 300));
        compactChangelogFiles.add(makeEntry(FileKind.ADD, 3, 4, 301));

        CommitStats commitStats =
                new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        500,
                        1,
                        2);

        commitMetrics.reportCommit(commitStats);
    }

    private CommitMetrics getCommitMetrics() {
        Path path = new Path(tempDir.toString(), TABLE_NAME);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        path,
                        RowType.of(new IntType()),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString());
        return new CommitMetrics(pathFactory);
    }
}
