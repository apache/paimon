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

package org.apache.paimon.flink.metrics;

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.Metric;
import org.apache.paimon.metrics.MetricGroup;

import java.util.Map;

/**
 * {@link MetricGroup} which wraps a Flink's {@link org.apache.flink.metrics.MetricGroup} and
 * register all metrics into Flink's metric system.
 */
public class FlinkMetricGroup implements MetricGroup {

    private static final String PAIMON_GROUP_NAME = "paimon";

    private final org.apache.flink.metrics.MetricGroup wrapped;
    private final String groupName;
    private final Map<String, String> variables;

    public FlinkMetricGroup(
            org.apache.flink.metrics.MetricGroup wrapped,
            String groupName,
            Map<String, String> variables) {
        wrapped = wrapped.addGroup(PAIMON_GROUP_NAME);
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            wrapped = wrapped.addGroup(entry.getKey(), entry.getValue());
        }
        wrapped = wrapped.addGroup(groupName);

        this.wrapped = wrapped;
        this.groupName = groupName;
        this.variables = variables;
    }

    @Override
    public Counter counter(String name) {
        return new FlinkCounter(wrapped.counter(name));
    }

    @Override
    public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
        return new FlinkGauge<>(wrapped.gauge(name, gauge::getValue));
    }

    @Override
    public Histogram histogram(String name, int windowSize) {
        return new FlinkHistogram(
                wrapped.histogram(
                        name,
                        new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(
                                windowSize)));
    }

    @Override
    public Map<String, String> getAllVariables() {
        return variables;
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public Map<String, Metric> getMetrics() {
        throw new UnsupportedOperationException(
                "FlinkMetricGroup does not support fetching all metrics. "
                        + "Please read the metrics through Flink's metric system.");
    }

    @Override
    public void close() {
        if (wrapped instanceof org.apache.flink.runtime.metrics.groups.AbstractMetricGroup) {
            ((org.apache.flink.runtime.metrics.groups.AbstractMetricGroup<?>) wrapped).close();
        }
    }
}
