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
    public static final String LAST_SCAN_DURATION = "lastScanDuration";
    public static final String SCAN_DURATION = "scanDuration";
    public static final String LAST_SCANNED_SNAPSHOT_ID = "lastScannedSnapshotId";
    public static final String LAST_SCANNED_MANIFESTS = "lastScannedManifests";
    public static final String LAST_SCAN_SKIPPED_TABLE_FILES = "lastScanSkippedTableFiles";
    public static final String LAST_SCAN_RESULTED_TABLE_FILES = "lastScanResultedTableFiles";
    public static final String MANIFEST_HIT_CACHE = "manifestHitCache";
    public static final String MANIFEST_MISSED_CACHE = "manifestMissedCache";
    public static final String DVMETA_HIT_CACHE = "dvMetaHitCache";
    public static final String DVMETA_MISSED_CACHE = "dvMetaMissedCache";

    private final MetricGroup metricGroup;
    private final Histogram durationHistogram;
    private final CacheMetrics cacheMetrics;
    private final CacheMetrics dvMetaCacheMetrics;

    private ScanStats latestScan;

    public ScanMetrics(MetricRegistry registry, String tableName) {
        metricGroup = registry.createTableMetricGroup(GROUP_NAME, tableName);
        metricGroup.gauge(
                LAST_SCAN_DURATION, () -> latestScan == null ? 0L : latestScan.getDuration());
        durationHistogram = metricGroup.histogram(SCAN_DURATION, HISTOGRAM_WINDOW_SIZE);
        cacheMetrics = new CacheMetrics();
        dvMetaCacheMetrics = new CacheMetrics();
        metricGroup.gauge(
                LAST_SCANNED_SNAPSHOT_ID,
                () -> latestScan == null ? 0L : latestScan.getScannedSnapshotId());
        metricGroup.gauge(
                LAST_SCANNED_MANIFESTS,
                () -> latestScan == null ? 0L : latestScan.getScannedManifests());
        metricGroup.gauge(
                LAST_SCAN_SKIPPED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getSkippedTableFiles());
        metricGroup.gauge(
                LAST_SCAN_RESULTED_TABLE_FILES,
                () -> latestScan == null ? 0L : latestScan.getResultedTableFiles());
        metricGroup.gauge(MANIFEST_HIT_CACHE, () -> cacheMetrics.getHitObject().get());
        metricGroup.gauge(MANIFEST_MISSED_CACHE, () -> cacheMetrics.getMissedObject().get());
        metricGroup.gauge(DVMETA_HIT_CACHE, () -> dvMetaCacheMetrics.getHitObject().get());
        metricGroup.gauge(DVMETA_MISSED_CACHE, () -> dvMetaCacheMetrics.getMissedObject().get());
    }

    @VisibleForTesting
    MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public void reportScan(ScanStats scanStats) {
        latestScan = scanStats;
        durationHistogram.update(scanStats.getDuration());
    }

    public CacheMetrics getCacheMetrics() {
        return cacheMetrics;
    }

    public CacheMetrics getDvMetaCacheMetrics() {
        return dvMetaCacheMetrics;
    }
}
