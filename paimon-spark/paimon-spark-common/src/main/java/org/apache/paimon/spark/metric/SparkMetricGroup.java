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

package org.apache.paimon.spark.metric;

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.MetricGroupImpl;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link org.apache.paimon.metrics.MetricGroup} that extends {@link MetricGroupImpl} to
 * additionally register metrics with a Codahale {@link MetricRegistry} for exposure via Spark's
 * MetricsSystem (JMX, Prometheus, etc.).
 *
 * <p>This preserves full backward compatibility with the existing Spark UI integration (which reads
 * from {@link MetricGroupImpl#getMetrics()}) while also making metrics available to external
 * monitoring systems.
 */
public class SparkMetricGroup extends MetricGroupImpl {

    private static final Logger LOG = LoggerFactory.getLogger(SparkMetricGroup.class);

    private final MetricRegistry codahaleRegistry;
    private final String metricPrefix;
    private final Set<String> registeredNames;

    public SparkMetricGroup(
            String groupName, Map<String, String> variables, MetricRegistry codahaleRegistry) {
        super(groupName, variables);
        this.codahaleRegistry = codahaleRegistry;
        this.metricPrefix = buildPrefix(groupName, variables);
        this.registeredNames = new HashSet<>();
    }

    @Override
    public Counter counter(String name) {
        Counter paimonCounter = super.counter(name);
        if (paimonCounter != null) {
            registerCodahaleGauge(
                    codahaleName(name), (com.codahale.metrics.Gauge<Long>) paimonCounter::getCount);
        }
        return paimonCounter;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
        Gauge<T> paimonGauge = super.gauge(name, gauge);
        if (paimonGauge != null) {
            registerCodahaleGauge(
                    codahaleName(name), (com.codahale.metrics.Gauge<Object>) paimonGauge::getValue);
        }
        return paimonGauge;
    }

    @Override
    public Histogram histogram(String name, int windowSize) {
        Histogram paimonHistogram = super.histogram(name, windowSize);
        if (paimonHistogram != null) {
            String base = codahaleName(name);
            registerCodahaleGauge(
                    base + ".count", (com.codahale.metrics.Gauge<Long>) paimonHistogram::getCount);
            registerCodahaleGauge(
                    base + ".mean",
                    (com.codahale.metrics.Gauge<Double>)
                            () -> paimonHistogram.getStatistics().getMean());
            registerCodahaleGauge(
                    base + ".min",
                    (com.codahale.metrics.Gauge<Long>)
                            () -> paimonHistogram.getStatistics().getMin());
            registerCodahaleGauge(
                    base + ".max",
                    (com.codahale.metrics.Gauge<Long>)
                            () -> paimonHistogram.getStatistics().getMax());
            registerCodahaleGauge(
                    base + ".p50",
                    (com.codahale.metrics.Gauge<Double>)
                            () -> paimonHistogram.getStatistics().getQuantile(0.5));
            registerCodahaleGauge(
                    base + ".p95",
                    (com.codahale.metrics.Gauge<Double>)
                            () -> paimonHistogram.getStatistics().getQuantile(0.95));
            registerCodahaleGauge(
                    base + ".p99",
                    (com.codahale.metrics.Gauge<Double>)
                            () -> paimonHistogram.getStatistics().getQuantile(0.99));
        }
        return paimonHistogram;
    }

    @Override
    public void close() {
        for (String name : registeredNames) {
            codahaleRegistry.remove(name);
        }
        registeredNames.clear();
        super.close();
    }

    private String codahaleName(String metricName) {
        return metricPrefix + "." + metricName;
    }

    private void registerCodahaleGauge(String name, com.codahale.metrics.Gauge<?> gauge) {
        // Always remove first to ensure we replace any stale gauge from a previous commit.
        // This avoids a race window in the try-register/catch-remove/re-register pattern.
        codahaleRegistry.remove(name);
        try {
            codahaleRegistry.register(name, gauge);
            registeredNames.add(name);
        } catch (IllegalArgumentException e) {
            LOG.warn("Failed to register Codahale metric '{}': {}", name, e.getMessage());
        }
    }

    private static String buildPrefix(String groupName, Map<String, String> variables) {
        StringBuilder sb = new StringBuilder("paimon");
        String table = variables.get("table");
        if (table != null && !table.isEmpty()) {
            sb.append('.').append(sanitize(table));
        }
        sb.append('.').append(sanitize(groupName));
        return sb.toString();
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
