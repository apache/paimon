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

import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistryImpl;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/** Tests for {@link ScanMetrics}. */
public class ScanMetricsTest {
    private static final String TABLE_NAME = "myTable";

    /** Tests the registration of the commit metrics. */
    @Test
    public void testGenericMetricsRegistration() {
        ScanMetrics scanMetrics = getScanMetrics();
        MetricGroup metricGroup = scanMetrics.getMetricGroup();
        assertThat(metricGroup.getGroupName()).isEqualTo(ScanMetrics.GROUP_NAME);
        Map<String, Metric> registeredMetrics = metricGroup.getMetrics();
        assertThat(registeredMetrics.keySet())
                .containsExactlyInAnyOrder(
                        ScanMetrics.LAST_SCAN_DURATION,
                        ScanMetrics.SCAN_DURATION,
                        ScanMetrics.LAST_SCANNED_MANIFESTS,
                        ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES,
                        ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES,
                        ScanMetrics.LAST_SKIPPED_BY_PARTITION_AND_STATS,
                        ScanMetrics.LAST_SKIPPED_BY_WHOLE_BUCKET_FILES_FILTER);
    }

    /** Tests that the metrics are updated properly. */
    @Test
    public void testMetricsAreUpdated() {
        ScanMetrics scanMetrics = getScanMetrics();
        Map<String, Metric> registeredGenericMetrics = scanMetrics.getMetricGroup().getMetrics();

        // Check initial values
        Gauge<Long> lastScanDuration =
                (Gauge<Long>) registeredGenericMetrics.get(ScanMetrics.LAST_SCAN_DURATION);
        Histogram scanDuration =
                (Histogram) registeredGenericMetrics.get(ScanMetrics.SCAN_DURATION);
        Gauge<Long> lastScannedManifests =
                (Gauge<Long>) registeredGenericMetrics.get(ScanMetrics.LAST_SCANNED_MANIFESTS);
        Gauge<Long> lastSkippedByPartitionAndStats =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                ScanMetrics.LAST_SKIPPED_BY_PARTITION_AND_STATS);
        Gauge<Long> lastSkippedByWholeBucketFilesFilter =
                (Gauge<Long>)
                        registeredGenericMetrics.get(
                                ScanMetrics.LAST_SKIPPED_BY_WHOLE_BUCKET_FILES_FILTER);
        Gauge<Long> lastScanSkippedTableFiles =
                (Gauge<Long>)
                        registeredGenericMetrics.get(ScanMetrics.LAST_SCAN_SKIPPED_TABLE_FILES);
        Gauge<Long> lastScanResultedTableFiles =
                (Gauge<Long>)
                        registeredGenericMetrics.get(ScanMetrics.LAST_SCAN_RESULTED_TABLE_FILES);

        assertThat(lastScanDuration.getValue()).isEqualTo(0);
        assertThat(scanDuration.getCount()).isEqualTo(0);
        assertThat(scanDuration.getStatistics().size()).isEqualTo(0);
        assertThat(lastScannedManifests.getValue()).isEqualTo(0);
        assertThat(lastSkippedByPartitionAndStats.getValue()).isEqualTo(0);
        assertThat(lastSkippedByWholeBucketFilesFilter.getValue()).isEqualTo(0);
        assertThat(lastScanSkippedTableFiles.getValue()).isEqualTo(0);
        assertThat(lastScanResultedTableFiles.getValue()).isEqualTo(0);

        // report once
        reportOnce(scanMetrics);

        // generic metrics value updated
        assertThat(lastScanDuration.getValue()).isEqualTo(200);
        assertThat(scanDuration.getCount()).isEqualTo(1);
        assertThat(scanDuration.getStatistics().size()).isEqualTo(1);
        assertThat(scanDuration.getStatistics().getValues()[0]).isEqualTo(200L);
        assertThat(scanDuration.getStatistics().getMin()).isEqualTo(200);
        assertThat(scanDuration.getStatistics().getQuantile(0.5)).isCloseTo(200.0, offset(0.001));
        assertThat(scanDuration.getStatistics().getMean()).isEqualTo(200);
        assertThat(scanDuration.getStatistics().getMax()).isEqualTo(200);
        assertThat(scanDuration.getStatistics().getStdDev()).isEqualTo(0);
        assertThat(lastScannedManifests.getValue()).isEqualTo(20);
        assertThat(lastSkippedByPartitionAndStats.getValue()).isEqualTo(25);
        assertThat(lastSkippedByWholeBucketFilesFilter.getValue()).isEqualTo(32);
        assertThat(lastScanSkippedTableFiles.getValue()).isEqualTo(57);
        assertThat(lastScanResultedTableFiles.getValue()).isEqualTo(10);

        // report again
        reportAgain(scanMetrics);

        // generic metrics value updated
        assertThat(lastScanDuration.getValue()).isEqualTo(500);
        assertThat(scanDuration.getCount()).isEqualTo(2);
        assertThat(scanDuration.getStatistics().size()).isEqualTo(2);
        assertThat(scanDuration.getStatistics().getValues()[1]).isEqualTo(500L);
        assertThat(scanDuration.getStatistics().getMin()).isEqualTo(200);
        assertThat(scanDuration.getStatistics().getQuantile(0.5)).isCloseTo(350.0, offset(0.001));
        assertThat(scanDuration.getStatistics().getMean()).isEqualTo(350);
        assertThat(scanDuration.getStatistics().getMax()).isEqualTo(500);
        assertThat(scanDuration.getStatistics().getStdDev()).isCloseTo(212.132, offset(0.001));
        assertThat(lastScannedManifests.getValue()).isEqualTo(22);
        assertThat(lastSkippedByPartitionAndStats.getValue()).isEqualTo(30);
        assertThat(lastSkippedByWholeBucketFilesFilter.getValue()).isEqualTo(33);
        assertThat(lastScanSkippedTableFiles.getValue()).isEqualTo(63);
        assertThat(lastScanResultedTableFiles.getValue()).isEqualTo(8);
    }

    private void reportOnce(ScanMetrics scanMetrics) {
        ScanStats scanStats = new ScanStats(200, 20, 25, 32, 10);
        scanMetrics.reportScan(scanStats);
    }

    private void reportAgain(ScanMetrics scanMetrics) {
        ScanStats scanStats = new ScanStats(500, 22, 30, 33, 8);
        scanMetrics.reportScan(scanStats);
    }

    private ScanMetrics getScanMetrics() {
        return new ScanMetrics(new MetricRegistryImpl(), TABLE_NAME);
    }
}
