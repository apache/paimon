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

import org.apache.paimon.metrics.Histogram;
import org.apache.paimon.metrics.HistogramStatistics;

/** {@link Histogram} which wraps a Flink's {@link org.apache.flink.metrics.Histogram}. */
public class FlinkHistogram implements Histogram {

    private final org.apache.flink.metrics.Histogram wrapped;

    public FlinkHistogram(org.apache.flink.metrics.Histogram wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void update(long value) {
        wrapped.update(value);
    }

    @Override
    public long getCount() {
        return wrapped.getCount();
    }

    @Override
    public HistogramStatistics getStatistics() {
        org.apache.flink.metrics.HistogramStatistics stats = wrapped.getStatistics();

        return new HistogramStatistics() {

            @Override
            public double getQuantile(double quantile) {
                return stats.getQuantile(quantile);
            }

            @Override
            public long[] getValues() {
                return stats.getValues();
            }

            @Override
            public int size() {
                return stats.size();
            }

            @Override
            public double getMean() {
                return stats.getMean();
            }

            @Override
            public double getStdDev() {
                return stats.getStdDev();
            }

            @Override
            public long getMax() {
                return stats.getMax();
            }

            @Override
            public long getMin() {
                return stats.getMin();
            }
        };
    }
}
