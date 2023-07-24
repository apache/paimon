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

import org.apache.paimon.utils.InstantiationUtil;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.ThrowingFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/**
 * Tests for {@link DescriptiveStatisticsHistogram} and {@link
 * DescriptiveStatisticsHistogramStatistics}.
 */
class DescriptiveStatisticsHistogramTest {

    private static final double[] DATA = {1, 2, 3, 4, 5, 6, 7, 8, 9};

    @Test
    void testDescriptiveHistogram() {
        int size = 10;
        testHistogram(size, new DescriptiveStatisticsHistogram(size));
    }

    @Test
    void testSerialization() throws Exception {
        testDuplication(
                original -> {
                    final byte[] bytes = InstantiationUtil.serializeObject(original);
                    return InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
                });
    }

    @Test
    void testCopy() throws Exception {
        testDuplication(DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot::copy);
    }

    private void testHistogram(int size, Histogram histogram) {
        HistogramStatistics statistics;

        for (int i = 0; i < size; i++) {
            histogram.update(i);

            statistics = histogram.getStatistics();
            assertThat(histogram.getCount()).isEqualTo(i + 1);
            assertThat(statistics.size()).isEqualTo(histogram.getCount());
            assertThat(statistics.getMax()).isEqualTo(i);
            assertThat(statistics.getMin()).isEqualTo(0);
        }

        statistics = histogram.getStatistics();
        assertThat(statistics.size()).isEqualTo(size);
        assertThat(statistics.getQuantile(0.5)).isCloseTo((size - 1) / 2.0, offset(0.001));

        for (int i = size; i < 2 * size; i++) {
            histogram.update(i);

            statistics = histogram.getStatistics();
            assertThat(histogram.getCount()).isEqualTo(i + 1);
            assertThat(statistics.size()).isEqualTo(size);
            assertThat(statistics.getMax()).isEqualTo(i);
            assertThat(statistics.getMin()).isEqualTo(i + 1 - size);
        }

        statistics = histogram.getStatistics();
        assertThat(statistics.size()).isEqualTo(size);
        assertThat(statistics.getQuantile(0.5)).isCloseTo(size + (size - 1) / 2.0, offset(0.001));
    }

    private static void testDuplication(
            ThrowingFunction<
                            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot,
                            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot>
                    duplicator)
            throws Exception {

        DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot original =
                new DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot();
        original.evaluate(DATA);

        assertOperations(original);

        final DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot copy =
                duplicator.apply(original);

        assertOperations(copy);
    }

    private static void assertOperations(
            DescriptiveStatisticsHistogramStatistics.CommonMetricsSnapshot statistics) {
        assertThat(statistics.getPercentile(0.5)).isEqualTo(1);
        assertThat(statistics.getCount()).isEqualTo(9);
        assertThat(statistics.getMin()).isEqualTo(1);
        assertThat(statistics.getMax()).isEqualTo(9);
        assertThat(statistics.getMean()).isEqualTo(5);
        assertThat(statistics.getStandardDeviation()).isCloseTo(2.7, Offset.offset(0.5));
        assertThat(statistics.getValues()).containsExactly(DATA);
    }
}
