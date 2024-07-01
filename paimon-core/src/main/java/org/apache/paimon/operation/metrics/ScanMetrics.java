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

/** Metrics to measure scan operation. */
public class ScanMetrics {

    private static final int HISTOGRAM_WINDOW_SIZE = 100;
    public static final String GROUP_NAME = "scan";

    private final MetricGroup metricGroup;

    public ScanMetrics(MetricRegistry registry, String tableName) {
        this.metricGroup = registry.tableMetricGroup(GROUP_NAME, tableName);
        registerGenericScanMetrics();
    }

    @VisibleForTesting
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    private Histogram durationHistogram;

    private ScanStats latestScan;

    public static final String LAST_SCAN_DURATION = "lastScanDuration";
    public static final String SCAN_DURATION = "scanDuration";
    public static final String LAST_SCANNED_MANIFESTS = "lastScannedManifests";

    public static final String LAST_SKIPPED_BY_PARTITION_AND_STATS =
            "lastSkippedByPartitionAndStats";

    public static final String LAST_SKIPPED_BY_BUCKET_AND_LEVEL_FILTER =
            "lastSkippedByBucketAndLevelFilter";

    public static final String LAST_SKIPPED_BY_WHOLE_BUCKET_FILES_FILTER =
            "lastSkippedByWholeBucketFilesFilter";

    public static final String LAST_SCAN_SKIPPED_TABLE_FILES = "lastScanSkippedTableFiles";

    public static final String LAST_SCAN_RESULTED_TABLE_FILES = "lastScanResultedTableFiles";

    private void registerGenericScanMetrics() {
        metricGroup.gauge(
                LAST_SCAN_DURATION, () -> latestScan == null ? 0L : latestScan.getDuration());
        durationHistogram = metricGroup.histogram(SCAN_DURATION, HISTOGRAM_WINDOW_SIZE);
        metricGroup.gauge(
                LAST_SCANNED_MANIFESTS,
                () -> latestScan == null ? 0L : latestScan.getScannedManifests());
        metricGroup.gauge(
                LAST_SKIPPED_BY_PARTITION_AND_STATS,
                () -> latestScan == null ? 0L : latestScan.getSkippedByPartitionAndStats());
        metricGroup.gauge(
                LAST_SKIPPED_BY_BUCKET_AND_LEVEL_FILTER,
                () -> latestScan == null ? 0L : latestScan.getSkippedByBucketAndLevelFilter());
        metricGroup.gauge(
                LAST_SKIPPED_BY_WHOLE_BUCKET_FILES_FILTER,
                () -> latestScan == null ? 0L : latestScan.getSkippedByWholeBucketFiles());
        metricGroup.gauge(
                LAST_SCAN_SKIPPED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getSkippedTableFiles());
        metricGroup.gauge(
                LAST_SCAN_RESULTED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getResultedTableFiles());
    }

    public void reportScan(ScanStats scanStats) {
        latestScan = scanStats;
        durationHistogram.update(scanStats.getDuration());
    }
}
