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

package org.apache.paimon.benchmark.metric;

import org.apache.paimon.benchmark.utils.BenchmarkUtils;

/** The aggregated result of a single benchmark query. */
public class JobBenchmarkMetric {

    private final String name;
    private final double rps;
    private final long totalRows;
    private final double cpu;
    private final Long avgDataFreshness;
    private final Long maxDataFreshness;

    public JobBenchmarkMetric(
            String name,
            double rps,
            long totalRows,
            double cpu,
            Long avgDataFreshness,
            Long maxDataFreshness) {
        this.name = name;
        this.rps = rps;
        this.totalRows = totalRows;
        this.cpu = cpu;
        this.avgDataFreshness = avgDataFreshness;
        this.maxDataFreshness = maxDataFreshness;
    }

    public String getName() {
        return name;
    }

    public String getPrettyRps() {
        return BenchmarkUtils.formatLongValue((long) rps);
    }

    public String getPrettyTotalRows() {
        return BenchmarkUtils.formatLongValue(totalRows);
    }

    public String getPrettyCpu() {
        return BenchmarkUtils.NUMBER_FORMAT.format(cpu);
    }

    public double getCpu() {
        return cpu;
    }

    public String getPrettyRpsPerCore() {
        return BenchmarkUtils.formatLongValue(getRpsPerCore());
    }

    public long getRpsPerCore() {
        return (long) (rps / cpu);
    }

    public String getAvgDataFreshnessString() {
        return BenchmarkUtils.formatDataFreshness(avgDataFreshness);
    }

    public String getMaxDataFreshnessString() {
        return BenchmarkUtils.formatDataFreshness(maxDataFreshness);
    }
}
