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

import org.apache.paimon.annotation.Public;

import java.util.Map;

/**
 * A MetricGroup is a named container for {@link Metric Metrics} and further metric subgroups.
 *
 * <p>Instances of this class can be used to register new metrics with Paimon.
 */
@Public
public interface MetricGroup {
    /**
     * Creates and registers a new {@link org.apache.paimon.metrics.Counter} with Paimon.
     *
     * @param name name of the counter
     * @return the created counter
     */
    default Counter counter(int name) {
        return counter(String.valueOf(name));
    }

    /**
     * Creates and registers a new {@link org.apache.paimon.metrics.Counter} with Paimon.
     *
     * @param name name of the counter
     * @return the created counter
     */
    Counter counter(String name);

    /**
     * Registers a {@link org.apache.paimon.metrics.Counter} with Paimon.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    default <C extends Counter> C counter(int name, C counter) {
        return counter(String.valueOf(name), counter);
    }

    /**
     * Registers a {@link org.apache.paimon.metrics.Counter} with Paimon.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    <C extends Counter> C counter(String name, C counter);

    /**
     * Registers a new {@link org.apache.paimon.metrics.Gauge} with Paimon.
     *
     * @param name name of the gauge
     * @param gauge gauge to register
     * @param <T> return type of the gauge
     * @return the given gauge
     */
    default <T, G extends Gauge<T>> G gauge(int name, G gauge) {
        return gauge(String.valueOf(name), gauge);
    }

    /**
     * Registers a new {@link org.apache.paimon.metrics.Gauge} with Paimon.
     *
     * @param name name of the gauge
     * @param gauge gauge to register
     * @param <T> return type of the gauge
     * @return the given gauge
     */
    <T, G extends Gauge<T>> G gauge(String name, G gauge);

    /**
     * Registers a new {@link Histogram} with Paimon.
     *
     * @param name name of the histogram
     * @param histogram histogram to register
     * @param <H> histogram type
     * @return the registered histogram
     */
    <H extends Histogram> H histogram(String name, H histogram);

    /**
     * Registers a new {@link Histogram} with Paimon.
     *
     * @param name name of the histogram
     * @param histogram histogram to register
     * @param <H> histogram type
     * @return the registered histogram
     */
    default <H extends Histogram> H histogram(int name, H histogram) {
        return histogram(String.valueOf(name), histogram);
    }

    /**
     * Gets the scope as an array of the scope components, for example {@code ["myTable",
     * "bucket-1"]}.
     */
    String[] getScopeComponents();

    /**
     * Returns the fully qualified metric name, for example {@code "myTable.bucket-1.metricName"}.
     *
     * @param metricName metric name
     * @return fully qualified metric name
     */
    String getMetricIdentifier(String metricName, char delimiter);

    /** Returns a map of all variables and their associated value. */
    Map<String, String> getAllVariables();

    /**
     * Returns the name for this group, meaning what kind of entity it represents, for example
     * "bucket".
     */
    String getGroupName();

    /** Returns all the metrics the group carries. */
    Map<String, Metric> getMetrics();
}
