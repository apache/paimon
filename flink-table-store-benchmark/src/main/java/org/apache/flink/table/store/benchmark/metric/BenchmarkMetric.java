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

package org.apache.flink.table.store.benchmark.metric;

import org.apache.flink.table.store.benchmark.utils.BenchmarkUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Metric collected per {@link
 * org.apache.flink.table.store.benchmark.BenchmarkOptions#METRIC_MONITOR_DURATION} for a single
 * query.
 */
public class BenchmarkMetric {
    private final double bps;
    private final long totalBytes;
    private final double cpu;
    @Nullable private final Long dataFreshness;

    public BenchmarkMetric(double bps, long totalBytes, double cpu, @Nullable Long dataFreshness) {
        this.bps = bps;
        this.totalBytes = totalBytes;
        this.cpu = cpu;
        this.dataFreshness = dataFreshness;
    }

    public double getBps() {
        return bps;
    }

    public String getPrettyBps() {
        return BenchmarkUtils.formatLongValue((long) bps);
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public String getPrettyTotalBytes() {
        return BenchmarkUtils.formatLongValue(totalBytes);
    }

    public double getCpu() {
        return cpu;
    }

    public String getPrettyCpu() {
        return BenchmarkUtils.NUMBER_FORMAT.format(cpu);
    }

    @Nullable
    public Long getDataFreshness() {
        return dataFreshness;
    }

    public String getDataFreshnessString() {
        return BenchmarkUtils.formatDataFreshness(dataFreshness);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BenchmarkMetric that = (BenchmarkMetric) o;
        return Double.compare(that.bps, bps) == 0 && Double.compare(that.cpu, cpu) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bps, cpu);
    }

    @Override
    public String toString() {
        return "BenchmarkMetric{" + "bps=" + bps + ", cpu=" + cpu + '}';
    }
}
