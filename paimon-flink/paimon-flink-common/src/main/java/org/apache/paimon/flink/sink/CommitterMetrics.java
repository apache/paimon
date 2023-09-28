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

package org.apache.paimon.flink.sink;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.commit.CommitMetrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import javax.annotation.Nullable;

import java.util.Map;

/** Flink metrics for {@link Committer}. */
public class CommitterMetrics {

    private static final String SINK_METRIC_GROUP = "sink";

    private final Counter numBytesOutCounter;
    private final Counter numRecordsOutCounter;
    private final OperatorMetricGroup metricGroup;

    public CommitterMetrics(OperatorMetricGroup metricGroup) {
        this(metricGroup, null);
    }

    public CommitterMetrics(OperatorMetricGroup metricGroup, @Nullable CommitMetrics commitMetrics) {
        this.metricGroup = metricGroup;
        OperatorIOMetricGroup operatorIOMetricGroup = metricGroup.getIOMetricGroup();
        MetricGroup sinkMetricGroup = operatorIOMetricGroup.addGroup(SINK_METRIC_GROUP);

        numBytesOutCounter = operatorIOMetricGroup.getNumBytesOutCounter();
        sinkMetricGroup.counter(MetricNames.IO_NUM_BYTES_OUT, numBytesOutCounter);
        sinkMetricGroup.meter(MetricNames.IO_NUM_BYTES_OUT_RATE, new MeterView(numBytesOutCounter));

        numRecordsOutCounter = operatorIOMetricGroup.getNumRecordsOutCounter();
        sinkMetricGroup.counter(MetricNames.IO_NUM_RECORDS_OUT, numRecordsOutCounter);
        sinkMetricGroup.meter(
                MetricNames.IO_NUM_RECORDS_OUT_RATE, new MeterView(numRecordsOutCounter));

        // Paimon Commit Metrics
        registerCommitMetrics(commitMetrics);
    }

    public void registerCommitMetrics(CommitMetrics commitMetrics) {
        if (commitMetrics != null) {
            MetricGroup commitMetricGroup = metricGroup.addGroup("commit");
            for (Map.Entry<String, Metric> metric : commitMetrics.getMetricGroup().getMetrics().entrySet()) {
                switch (metric.getValue().getMetricType()) {
                    case COUNTER:
                        commitMetricGroup.counter(metric.getKey(), (Counter) metric.getValue());
                        break;
                    case GAUGE:
                        commitMetricGroup.gauge(metric.getKey(), (Gauge<?>) metric.getValue());
                        break;
                    case HISTOGRAM:
                        commitMetricGroup.histogram(metric.getKey(), (Histogram) metric.getValue());
                        break;
                    default:
                        throw new UnsupportedOperationException("Custom metric types are not supported.");
                }
            }
        }
    }

    public void increaseNumBytesOut(long numBytesOut) {
        numBytesOutCounter.inc(numBytesOut);
    }

    public void increaseNumRecordsOut(long numRecordsOut) {
        numRecordsOutCounter.inc(numRecordsOut);
    }

    @VisibleForTesting
    public Counter getNumBytesOutCounter() {
        return numBytesOutCounter;
    }

    @VisibleForTesting
    public Counter getNumRecordsOutCounter() {
        return numRecordsOutCounter;
    }
}
