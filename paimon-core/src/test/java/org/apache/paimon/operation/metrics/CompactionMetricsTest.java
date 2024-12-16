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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricRegistryImpl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionMetrics}. */
public class CompactionMetricsTest {

    @Test
    public void testReportMetrics() {
        CompactionMetrics metrics = new CompactionMetrics(new MetricRegistryImpl(), "myTable");
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(-1L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(-1.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_THREAD_BUSY)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_COMPACTION_TIME)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(0L);
        CompactionMetrics.Reporter[] reporters = new CompactionMetrics.Reporter[3];
        for (int i = 0; i < reporters.length; i++) {
            reporters[i] = metrics.createReporter(BinaryRow.EMPTY_ROW, i);
        }

        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_THREAD_BUSY)).isEqualTo(0.0);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(0L);

        reporters[0].reportLevel0FileCount(5);
        reporters[1].reportLevel0FileCount(3);
        reporters[2].reportLevel0FileCount(4);
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(5L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(4.0);

        reporters[0].reportLevel0FileCount(8);
        assertThat(getMetric(metrics, CompactionMetrics.MAX_LEVEL0_FILE_COUNT)).isEqualTo(8L);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_LEVEL0_FILE_COUNT)).isEqualTo(5.0);

        reporters[0].reportCompactionTime(300000);
        reporters[0].reportCompactionTime(250000);
        reporters[0].reportCompactionTime(270000);
        assertThat(getMetric(metrics, CompactionMetrics.AVG_COMPACTION_TIME))
                .isEqualTo(273333.3333333333);

        // enqueue compaction request
        reporters[0].increaseCompactionsQueuedCount();
        reporters[1].increaseCompactionsQueuedCount();
        reporters[2].increaseCompactionsQueuedCount();
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(0L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(3L);

        // completed compactions and remove them from queue
        reporters[0].increaseCompactionsCompletedCount();
        reporters[0].decreaseCompactionsQueuedCount();
        reporters[1].decreaseCompactionsQueuedCount();
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_COMPLETED_COUNT)).isEqualTo(1L);
        assertThat(getMetric(metrics, CompactionMetrics.COMPACTION_QUEUED_COUNT)).isEqualTo(1L);
    }

    private Object getMetric(CompactionMetrics metrics, String metricName) {
        Metric metric = metrics.getMetricGroup().getMetrics().get(metricName);
        if (metric instanceof Gauge) {
            return ((Gauge<?>) metric).getValue();
        } else {
            return ((Counter) metric).getCount();
        }
    }
}
