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
import org.apache.paimon.metrics.Metric;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SparkMetricGroup}. */
public class SparkMetricGroupTest {

    @Test
    public void testGaugeRegisteredInCodahaleRegistry() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        group.gauge("lastCommitDuration", () -> 42L);

        // Verify the Paimon metric is registered
        Map<String, Metric> paimonMetrics = group.getMetrics();
        assertThat(paimonMetrics).containsKey("lastCommitDuration");
        assertThat(((Gauge<Long>) paimonMetrics.get("lastCommitDuration")).getValue())
                .isEqualTo(42L);

        // Verify the Codahale metric is registered with the correct name
        String expectedName = "paimon.myTable.commit.lastCommitDuration";
        assertThat(codahaleRegistry.getGauges()).containsKey(expectedName);
        assertThat(codahaleRegistry.getGauges().get(expectedName).getValue()).isEqualTo(42L);
    }

    @Test
    public void testCounterRegisteredInCodahaleRegistry() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        Counter counter = group.counter("numCommits");
        counter.inc();
        counter.inc();
        counter.inc();

        // Verify the Codahale gauge wraps the counter's count
        String expectedName = "paimon.myTable.commit.numCommits";
        assertThat(codahaleRegistry.getGauges()).containsKey(expectedName);
        assertThat(codahaleRegistry.getGauges().get(expectedName).getValue()).isEqualTo(3L);
    }

    @Test
    public void testHistogramRegisteredInCodahaleRegistry() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        Histogram histogram = group.histogram("commitDuration", 100);
        histogram.update(100);
        histogram.update(200);
        histogram.update(300);

        String prefix = "paimon.myTable.commit.commitDuration";

        // Verify all histogram sub-metrics are registered
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".count");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".mean");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".min");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".max");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".p50");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".p95");
        assertThat(codahaleRegistry.getGauges()).containsKey(prefix + ".p99");

        // Verify values
        assertThat(codahaleRegistry.getGauges().get(prefix + ".count").getValue()).isEqualTo(3L);
        assertThat(codahaleRegistry.getGauges().get(prefix + ".min").getValue()).isEqualTo(100L);
        assertThat(codahaleRegistry.getGauges().get(prefix + ".max").getValue()).isEqualTo(300L);
    }

    @Test
    public void testGaugeValueUpdatesLazily() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("scan", variables, codahaleRegistry);

        long[] holder = {0L};
        group.gauge("lastScanDuration", () -> holder[0]);

        String codahaleName = "paimon.myTable.scan.lastScanDuration";
        assertThat(codahaleRegistry.getGauges().get(codahaleName).getValue()).isEqualTo(0L);

        // Mutate the backing value and verify the Codahale gauge reflects it
        holder[0] = 999L;
        assertThat(codahaleRegistry.getGauges().get(codahaleName).getValue()).isEqualTo(999L);
    }

    @Test
    public void testCloseRemovesCodahaleMetrics() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        group.gauge("g1", () -> 1L);
        group.counter("c1");
        group.histogram("h1", 100);

        // Verify metrics exist
        assertThat(codahaleRegistry.getGauges()).isNotEmpty();

        group.close();

        // Verify all Codahale metrics are removed
        assertThat(codahaleRegistry.getMetrics()).isEmpty();
    }

    @Test
    public void testMetricNameSanitization() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "my-db.my-table");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        group.gauge("someMetric", () -> 1L);

        // Special characters in table name should be replaced with underscores
        String expectedName = "paimon.my_db_my_table.commit.someMetric";
        assertThat(codahaleRegistry.getGauges()).containsKey(expectedName);
    }

    @Test
    public void testMetricNameWithoutTableVariable() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();

        SparkMetricGroup group = new SparkMetricGroup("scan", variables, codahaleRegistry);
        group.gauge("duration", () -> 100L);

        // Without a table variable, the prefix should be paimon.<group>
        String expectedName = "paimon.scan.duration";
        assertThat(codahaleRegistry.getGauges()).containsKey(expectedName);
    }

    @Test
    public void testMetricReplaceOnCollision() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        // Pre-register a Codahale gauge with the same name
        String metricName = "paimon.myTable.commit.existingMetric";
        codahaleRegistry.register(metricName, (com.codahale.metrics.Gauge<Long>) () -> -1L);
        assertThat(codahaleRegistry.getGauges().get(metricName).getValue()).isEqualTo(-1L);

        // Now register via SparkMetricGroup - should replace the existing one
        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        group.gauge("existingMetric", () -> 42L);

        assertThat(codahaleRegistry.getGauges().get(metricName).getValue()).isEqualTo(42L);
    }

    @Test
    public void testPaimonMetricsPreservedForSparkUI() {
        MetricRegistry codahaleRegistry = new MetricRegistry();
        Map<String, String> variables = new HashMap<>();
        variables.put("table", "myTable");

        SparkMetricGroup group = new SparkMetricGroup("commit", variables, codahaleRegistry);
        group.gauge("lastCommitDuration", () -> 100L);
        group.counter("numOps");
        group.histogram("commitDuration", 100);

        // Verify getMetrics() still works for Spark UI integration (backward compat)
        Map<String, Metric> paimonMetrics = group.getMetrics();
        assertThat(paimonMetrics).containsKey("lastCommitDuration");
        assertThat(paimonMetrics).containsKey("numOps");
        assertThat(paimonMetrics).containsKey("commitDuration");
        assertThat(paimonMetrics).hasSize(3);
    }
}
