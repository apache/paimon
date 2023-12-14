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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricRegistryImpl;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/** Tests for {@link CompactionMetrics}. */
public class CompactionMetricsTest {

    private static final String TABLE_NAME = "myTable";
    private static final String PARTITION = "date=20230623";
    private static final int BUCKET = 5;

    /** Tests that the metrics are updated properly. */
    @SuppressWarnings("unchecked")
    @Test
    public void testMetricsAreUpdated() {
        CompactionMetrics compactionMetrics = getCompactionMetrics();
        Map<String, Metric> registeredGenericMetrics =
                compactionMetrics.getMetricGroup().getMetrics();

        // Check initial values
        Gauge<Long> lastCompactionDuration =
                (Gauge<Long>)
                        registeredGenericMetrics.get(CompactionMetrics.LAST_COMPACTION_DURATION);
        Histogram compactionDuration =
                (Histogram) registeredGenericMetrics.get(CompactionMetrics.COMPACTION_DURATION);
        Gauge<Long> lastTableFilesCompactedBefore =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_TABLE_FILES_COMPACTED_BEFORE);
        Gauge<Long> lastTableFilesCompactedAfter =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_TABLE_FILES_COMPACTED_AFTER);
        Gauge<Long> lastChangelogFilesCompacted =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_CHANGELOG_FILES_COMPACTED);
        Gauge<Long> lastRewriteInputFileSize =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_REWRITE_INPUT_FILE_SIZE);
        Gauge<Long> lastRewriteOutputFileSize =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_REWRITE_OUTPUT_FILE_SIZE);
        Gauge<Long> lastRewriteChangelogFileSize =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                CompactionMetrics.LAST_REWRITE_CHANGELOG_FILE_SIZE);
        Gauge<Integer> runningCompaction = (Gauge<Integer>) registeredGenericMetrics.get(
                CompactionMetrics.RUNNING_COMPACTION);

        assertThat(lastCompactionDuration.getValue()).isEqualTo(0);
        assertThat(compactionDuration.getCount()).isEqualTo(0);
        assertThat(compactionDuration.getStatistics().size()).isEqualTo(0);
        assertThat(lastTableFilesCompactedBefore.getValue()).isEqualTo(0);
        assertThat(lastTableFilesCompactedAfter.getValue()).isEqualTo(0);
        assertThat(lastChangelogFilesCompacted.getValue()).isEqualTo(0);
        assertThat(lastRewriteInputFileSize.getValue()).isEqualTo(0);
        assertThat(lastRewriteOutputFileSize.getValue()).isEqualTo(0);
        assertThat(lastRewriteChangelogFileSize.getValue()).isEqualTo(0);

        // report once
        reportRunning(compactionMetrics);
        assertThat(runningCompaction.getValue()).isEqualTo(1);

        reportOnce(compactionMetrics);

        // generic metrics value updated
        assertThat(lastCompactionDuration.getValue()).isEqualTo(3000);
        assertThat(compactionDuration.getCount()).isEqualTo(1);
        assertThat(compactionDuration.getStatistics().size()).isEqualTo(1);
        assertThat(compactionDuration.getStatistics().getValues()[0]).isEqualTo(3000L);
        assertThat(compactionDuration.getStatistics().getMin()).isEqualTo(3000);
        assertThat(compactionDuration.getStatistics().getQuantile(0.5))
                .isCloseTo(3000.0, offset(0.001));
        assertThat(compactionDuration.getStatistics().getMean()).isEqualTo(3000);
        assertThat(compactionDuration.getStatistics().getMax()).isEqualTo(3000);
        assertThat(compactionDuration.getStatistics().getStdDev()).isEqualTo(0);
        assertThat(lastTableFilesCompactedBefore.getValue()).isEqualTo(2);
        assertThat(lastTableFilesCompactedAfter.getValue()).isEqualTo(2);
        assertThat(lastChangelogFilesCompacted.getValue()).isEqualTo(2);
        assertThat(lastRewriteInputFileSize.getValue()).isEqualTo(2001);
        assertThat(lastRewriteOutputFileSize.getValue()).isEqualTo(1101);
        assertThat(lastRewriteChangelogFileSize.getValue()).isEqualTo(3001);
        assertThat(runningCompaction.getValue()).isEqualTo(0);

        // report again
        reportRunning(compactionMetrics);
        assertThat(runningCompaction.getValue()).isEqualTo(1);

        reportAgain(compactionMetrics);

        // generic metrics value updated
        assertThat(lastCompactionDuration.getValue()).isEqualTo(6000);
        assertThat(compactionDuration.getCount()).isEqualTo(2);
        assertThat(compactionDuration.getStatistics().size()).isEqualTo(2);
        assertThat(compactionDuration.getStatistics().getValues()[1]).isEqualTo(6000L);
        assertThat(compactionDuration.getStatistics().getMin()).isEqualTo(3000);
        assertThat(compactionDuration.getStatistics().getQuantile(0.5))
                .isCloseTo(4500, offset(0.001));
        assertThat(compactionDuration.getStatistics().getMean()).isEqualTo(4500);
        assertThat(compactionDuration.getStatistics().getMax()).isEqualTo(6000);
        assertThat(compactionDuration.getStatistics().getStdDev())
                .isCloseTo(2121.320, offset(0.001));
        assertThat(lastTableFilesCompactedBefore.getValue()).isEqualTo(2);
        assertThat(lastTableFilesCompactedAfter.getValue()).isEqualTo(2);
        assertThat(lastChangelogFilesCompacted.getValue()).isEqualTo(2);
        assertThat(lastRewriteInputFileSize.getValue()).isEqualTo(2001);
        assertThat(lastRewriteOutputFileSize.getValue()).isEqualTo(1201);
        assertThat(lastRewriteChangelogFileSize.getValue()).isEqualTo(2501);
        assertThat(runningCompaction.getValue()).isEqualTo(0);
    }

    private void reportRunning(CompactionMetrics compactionMetrics) {
        compactionMetrics.reportRunningCompaction();
    }

    private void reportOnce(CompactionMetrics compactionMetrics) {
        List<DataFileMeta> compactBefore = new ArrayList<>();
        List<DataFileMeta> compactAfter = new ArrayList<>();
        List<DataFileMeta> compactChangelog = new ArrayList<>();
        compactBefore.add(DataFileTestUtils.newFile(0, 1000));
        compactBefore.add(DataFileTestUtils.newFile(1001, 2000));
        compactAfter.add(DataFileTestUtils.newFile(400, 1000));
        compactAfter.add(DataFileTestUtils.newFile(1001, 1500));
        compactChangelog.add(DataFileTestUtils.newFile(0, 2000));
        compactChangelog.add(DataFileTestUtils.newFile(2001, 3000));

        CompactionStats compactionStats =
                new CompactionStats(3000, compactBefore, compactAfter, compactChangelog);

        compactionMetrics.reportCompleteCompaction(compactionStats);
    }

    private void reportAgain(CompactionMetrics compactionMetrics) {
        List<DataFileMeta> compactBefore = new ArrayList<>();
        List<DataFileMeta> compactAfter = new ArrayList<>();
        List<DataFileMeta> compactChangelog = new ArrayList<>();
        compactBefore.add(DataFileTestUtils.newFile(2000, 3000));
        compactBefore.add(DataFileTestUtils.newFile(3001, 4000));
        compactAfter.add(DataFileTestUtils.newFile(600, 1200));
        compactAfter.add(DataFileTestUtils.newFile(1201, 1800));
        compactChangelog.add(DataFileTestUtils.newFile(0, 1500));
        compactChangelog.add(DataFileTestUtils.newFile(1501, 2500));

        CompactionStats compactionStats =
                new CompactionStats(6000, compactBefore, compactAfter, compactChangelog);

        compactionMetrics.reportCompleteCompaction(compactionStats);
    }

    private CompactionMetrics getCompactionMetrics() {
        return new CompactionMetrics(new MetricRegistryImpl(), TABLE_NAME, PARTITION, BUCKET);
    }
}
