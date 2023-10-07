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

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * {@link Histogram} for getting the current value of a Paimon {@link
 * org.apache.paimon.metrics.Histogram} metric.
 */
public class HistogramMetricMutableWrapper implements Histogram {
    private org.apache.paimon.metrics.Histogram histogram;

    public HistogramMetricMutableWrapper(org.apache.paimon.metrics.Histogram histogram) {
        this.histogram = histogram;
    }

    @Override
    public void update(long l) {
        throw new UnsupportedOperationException(
                "update operation is not supported in the mutable flink metrics wrapper.");
    }

    @Override
    public long getCount() {
        return histogram.getCount();
    }

    @Override
    public HistogramStatistics getStatistics() {
        return new HistogramStatistics() {
            @Override
            public double getQuantile(double v) {
                return histogram.getStatistics().getQuantile(v);
            }

            @Override
            public long[] getValues() {
                return histogram.getStatistics().getValues();
            }

            @Override
            public int size() {
                return histogram.getStatistics().size();
            }

            @Override
            public double getMean() {
                return histogram.getStatistics().getMean();
            }

            @Override
            public double getStdDev() {
                return histogram.getStatistics().getStdDev();
            }

            @Override
            public long getMax() {
                return histogram.getStatistics().getMax();
            }

            @Override
            public long getMin() {
                return histogram.getStatistics().getMin();
            }
        };
    }
}
