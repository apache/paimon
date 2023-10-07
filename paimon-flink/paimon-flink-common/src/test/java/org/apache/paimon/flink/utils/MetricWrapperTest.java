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

package org.apache.paimon.flink.utils;

import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.DescriptiveStatisticsHistogram;
import org.apache.paimon.metrics.Gauge;
import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.SimpleCounter;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for wrapper of paimon metrics to flink metrics. */
public class MetricWrapperTest {

    private static OperatorMetricGroup metricGroup;

    @BeforeAll
    public static void before() {
        metricGroup = UnregisteredMetricsGroup.createOperatorMetricGroup();
    }

    @Test
    public void testGaugeWrap() {
        LongGaugeTest longGauge = new LongGaugeTest();
        org.apache.flink.metrics.Gauge<Long> flinkGauge =
                metricGroup.gauge("testGauge", new GaugeMetricMutableWrapper(longGauge));
        assertThat(flinkGauge.getValue()).isEqualTo(5L);

        // Lambda gauge
        Gauge<Long> lambdaGauge = () -> 6L;
        flinkGauge =
                metricGroup.gauge("testLambdaGauge", new GaugeMetricMutableWrapper(lambdaGauge));
        assertThat(flinkGauge.getValue()).isEqualTo(6L);
    }

    @Test
    public void testCounterWrap() {
        Counter myCounter = new SimpleCounter();
        org.apache.flink.metrics.Counter flinkCounter =
                metricGroup.counter("testCounter", new CounterMetricMutableWrapper(myCounter));
        myCounter.inc();
        assertThat(flinkCounter.getCount()).isEqualTo(1L);
        myCounter.inc(5);
        assertThat(flinkCounter.getCount()).isEqualTo(6L);
        myCounter.dec(2);
        assertThat(flinkCounter.getCount()).isEqualTo(4L);
        assertThrows(UnsupportedOperationException.class, () -> flinkCounter.inc());
    }

    @Test
    public void testHistogramWrap() {
        Histogram myHistogram = new DescriptiveStatisticsHistogram(10000);
        org.apache.flink.metrics.Histogram flinkHistogram =
                metricGroup.histogram(
                        "testHistogram", new HistogramMetricMutableWrapper(myHistogram));
        assertThat(flinkHistogram.getCount()).isEqualTo(0L);
        myHistogram.update(1L);
        assertThat(flinkHistogram.getCount()).isEqualTo(1L);
        assertThat(flinkHistogram.getStatistics().getMax()).isEqualTo(1L);
        assertThrows(UnsupportedOperationException.class, () -> flinkHistogram.update(2L));
    }

    private class LongGaugeTest implements Gauge<Long> {
        @Override
        public Long getValue() {
            return 5L;
        }
    }
}
