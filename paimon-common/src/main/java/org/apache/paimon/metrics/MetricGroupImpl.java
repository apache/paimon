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

package org.apache.paimon.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/** Default implementation of {@link MetricGroup}. */
public class MetricGroupImpl implements MetricGroup {

    private static final Logger LOG = LoggerFactory.getLogger(MetricGroupImpl.class);

    private final String groupName;
    private final Map<String, String> variables;
    private final Map<String, Metric> metrics;

    public MetricGroupImpl(String groupName) {
        this(groupName, new HashMap<>());
    }

    public MetricGroupImpl(String groupName, Map<String, String> variables) {
        this.groupName = groupName;
        this.variables = variables;
        this.metrics = new HashMap<>();
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public Map<String, String> getAllVariables() {
        return variables;
    }

    @Override
    public Counter counter(String name) {
        return (Counter) addMetric(name, new SimpleCounter());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
        return (Gauge<T>) addMetric(name, gauge);
    }

    @Override
    public Histogram histogram(String name, int windowSize) {
        return (Histogram) addMetric(name, new DescriptiveStatisticsHistogram(windowSize));
    }

    /**
     * Adds the given metric to the group and registers it at the registry, if the group is not yet
     * closed, and if no metric with the same name has been registered before.
     *
     * @param metricName the name to register the metric under
     * @param metric the metric to register
     */
    private Metric addMetric(String metricName, Metric metric) {
        if (metric == null) {
            LOG.warn(
                    "Ignoring attempted registration of a metric due to being null for name {}.",
                    metricName);
            return null;
        }

        switch (metric.getMetricType()) {
            case COUNTER:
            case GAUGE:
            case HISTOGRAM:
                // immediately put without a 'contains' check to optimize the common case
                // (no collision), collisions are resolved later
                Metric prior = metrics.put(metricName, metric);

                // check for collisions with other metric names
                if (prior != null) {
                    // we had a collision. put back the original value
                    metrics.put(metricName, prior);

                    // we warn here, rather than failing, because metrics are tools that
                    // should not fail the program when used incorrectly
                    LOG.warn(
                            "Name collision: Group already contains a Metric with the name '"
                                    + metricName
                                    + "'. The new added Metric will not be reported.");
                }
                break;
            default:
                LOG.warn(
                        "Cannot add unknown metric type {}. This indicates that the paimon "
                                + "does not support this metric type.",
                        metric.getClass().getName());
        }
        return metrics.get(metricName);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metrics;
    }

    @Override
    public void close() {}
}
