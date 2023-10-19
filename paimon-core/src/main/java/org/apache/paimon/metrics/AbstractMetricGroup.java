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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains key functionality for adding metrics and carries metrics.
 *
 * <p>A MetricGroup can be {@link #close() closed}. Upon closing, the group de-register all metrics.
 *
 * <p>The {@link #close()} method and {@link #addMetric(String, Metric)} method should never be
 * invoked by multiple threads at the same time, {@link #addMetric(String, Metric)} and {@link
 * #getMetrics()} have multi-threads problems, like the reporter is reading the metrics map and the
 * group is adding metrics to the map at the same time.
 */
public abstract class AbstractMetricGroup implements MetricGroup {
    protected static final Logger LOG = LoggerFactory.getLogger(MetricGroup.class);

    // ------------------------------------------------------------------------

    /** The map containing all tags and their associated values. */
    protected Map<String, String> tags;

    /** Flag indicating whether this group has been closed. */
    private boolean closed = false;

    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------------

    public AbstractMetricGroup(Map<String, String> tags) {
        this.tags = tags;
        Metrics.getInstance().addGroup(this);
    }

    @Override
    public Map<String, String> getAllTags() {
        return tags;
    }

    /**
     * Returns the fully qualified metric name using the configured delimiter for the reporter with
     * the given index, for example {@code "commit.metricName"}.
     *
     * @param metricName metric name
     * @param delimiter delimiter to use
     * @return fully qualified metric name
     */
    public String getMetricIdentifier(String metricName, String delimiter) {
        return String.join(delimiter, getGroupName(), metricName);
    }

    /**
     * Creates and registers a new {@link org.apache.paimon.metrics.Counter} or return the existing
     * {@link org.apache.paimon.metrics.Counter}.
     *
     * @param name name of the counter
     * @return the created or existing counter
     */
    public Counter counter(String name) {
        return counter(name, new SimpleCounter());
    }

    /**
     * Registers a {@link org.apache.paimon.metrics.Counter}.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    public <C extends Counter> C counter(String name, C counter) {
        return (C) addMetric(name, counter);
    }

    /**
     * Registers a new {@link org.apache.paimon.metrics.Gauge}.
     *
     * @param name name of the gauge
     * @param gauge gauge to register
     * @param <T> return type of the gauge
     * @return the given gauge
     */
    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
        return (G) addMetric(name, gauge);
    }

    /**
     * Registers a new {@link Histogram} with Paimon.
     *
     * @param name name of the histogram
     * @param histogram histogram to register
     * @param <H> histogram type
     * @return the registered histogram
     */
    public <H extends Histogram> H histogram(String name, H histogram) {
        return (H) addMetric(name, histogram);
    }

    /**
     * Adds the given metric to the group and registers it at the registry, if the group is not yet
     * closed, and if no metric with the same name has been registered before.
     *
     * @param metricName the name to register the metric under
     * @param metric the metric to register
     */
    protected Metric addMetric(String metricName, Metric metric) {
        if (metric == null) {
            LOG.warn(
                    "Ignoring attempted registration of a metric due to being null for name {}.",
                    metricName);
            return null;
        }
        // add the metric only if the group is still open
        if (!isClosed()) {
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
        }
        return metrics.get(metricName);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return Collections.unmodifiableMap(metrics);
    }

    /**
     * Returns the name for this group, meaning what kind of entity it represents, for example
     * "commit".
     *
     * @return logical name for this group
     */
    public abstract String getGroupName();

    public void close() {
        if (!closed) {
            closed = true;
            metrics.clear();
            Metrics.getInstance().removeGroup(this);
        }
    }

    public final boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "MetricGroup{"
                + "groupName="
                + getGroupName()
                + ", metrics="
                + String.join(",", metrics.keySet())
                + '}';
    }
}
