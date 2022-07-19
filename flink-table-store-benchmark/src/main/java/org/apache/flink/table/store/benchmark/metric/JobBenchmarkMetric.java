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

import java.util.Objects;

/** The aggregated result of a single benchmark query. */
public class JobBenchmarkMetric {
    private final double bps;
    private final long totalBytes;
    private final double cpu;
    private final Long avgDataFreshness;
    private final Long maxDataFreshness;
    private double queryRps;

    public JobBenchmarkMetric(
            double bps, long totalBytes, double cpu, Long avgDataFreshness, Long maxDataFreshness) {
        this.bps = bps;
        this.totalBytes = totalBytes;
        this.cpu = cpu;
        this.avgDataFreshness = avgDataFreshness;
        this.maxDataFreshness = maxDataFreshness;
    }

    public String getPrettyBps() {
        return BenchmarkUtils.formatLongValue((long) bps);
    }

    public String getPrettyTotalBytes() {
        return BenchmarkUtils.formatLongValue(totalBytes);
    }

    public String getPrettyCpu() {
        return BenchmarkUtils.NUMBER_FORMAT.format(cpu);
    }

    public double getCpu() {
        return cpu;
    }

    public String getPrettyBpsPerCore() {
        return BenchmarkUtils.formatLongValue(getBpsPerCore());
    }

    public long getBpsPerCore() {
        return (long) (bps / cpu);
    }

    public String getAvgDataFreshnessString() {
        return BenchmarkUtils.formatDataFreshness(avgDataFreshness);
    }

    public String getMaxDataFreshnessString() {
        return BenchmarkUtils.formatDataFreshness(maxDataFreshness);
    }

    public void setQueryRps(double queryRps) {
        this.queryRps = queryRps;
    }

    public String getPrettyQueryRps() {
        return BenchmarkUtils.formatLongValue((long) queryRps);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobBenchmarkMetric that = (JobBenchmarkMetric) o;
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
