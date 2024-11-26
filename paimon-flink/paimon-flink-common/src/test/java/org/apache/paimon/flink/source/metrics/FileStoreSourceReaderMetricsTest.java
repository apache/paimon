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

package org.apache.paimon.flink.source.metrics;

import org.apache.flink.metrics.testutils.MetricListener;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FileStoreSourceReaderMetricsTest {
    @Test
    public void testRecordSnapshotUpdate() {
        MetricListener metricListener = new MetricListener();

        final FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(metricListener.getMetricGroup());
        assertThat(sourceReaderMetrics.getLatestFileCreationTime())
                .isEqualTo(FileStoreSourceReaderMetrics.UNDEFINED);
        assertThat(sourceReaderMetrics.getLastSplitUpdateTime())
                .isEqualTo(FileStoreSourceReaderMetrics.UNDEFINED);
        sourceReaderMetrics.recordSnapshotUpdate(123);
        assertThat(sourceReaderMetrics.getLatestFileCreationTime()).isEqualTo(123);
        assertThat(sourceReaderMetrics.getLastSplitUpdateTime())
                .isGreaterThan(FileStoreSourceReaderMetrics.UNDEFINED);
    }

    @Test
    public void testCurrentFetchLagUpdated() {
        MetricListener metricListener = new MetricListener();

        final FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(metricListener.getMetricGroup());
        assertThat(sourceReaderMetrics.getFetchTimeLag())
                .isEqualTo(FileStoreSourceReaderMetrics.UNDEFINED);
        sourceReaderMetrics.recordSnapshotUpdate(123);
        assertThat(sourceReaderMetrics.getFetchTimeLag())
                .isNotEqualTo(FileStoreSourceReaderMetrics.UNDEFINED);
    }

    @Test
    public void testSourceIdleTimeUpdated() throws InterruptedException {
        MetricListener metricListener = new MetricListener();
        final FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(metricListener.getMetricGroup());

        assertThat(sourceReaderMetrics.getIdleTime()).isEqualTo(0L);

        // idle start
        sourceReaderMetrics.idlingStarted();
        Thread.sleep(10L);
        assertThat(sourceReaderMetrics.getIdleTime()).isGreaterThan(9L);

        //non-idle
        sourceReaderMetrics.recordSnapshotUpdate(123);
        Thread.sleep(10L);
        assertThat(sourceReaderMetrics.getIdleTime()).isEqualTo(0L);

        // idle start
        sourceReaderMetrics.idlingStarted();
        Thread.sleep(10L);
        assertThat(sourceReaderMetrics.getIdleTime()).isGreaterThan(9L);
    }
}
