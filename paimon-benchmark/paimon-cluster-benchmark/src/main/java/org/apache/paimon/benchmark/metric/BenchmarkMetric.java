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

import javax.annotation.Nullable;

/**
 * Metric collected per {@link org.apache.paimon.benchmark.BenchmarkOptions#METRIC_MONITOR_INTERVAL}
 * for a single query.
 */
public class BenchmarkMetric {
    private final double rps;
    private final long totalRows;
    private final double cpu;
    @Nullable private final Long dataFreshness;

    public BenchmarkMetric(double rps, long totalRows, double cpu, @Nullable Long dataFreshness) {
        this.rps = rps;
        this.totalRows = totalRows;
        this.cpu = cpu;
        this.dataFreshness = dataFreshness;
    }

    public double getRps() {
        return rps;
    }

    public String getPrettyRps() {
        return BenchmarkUtils.formatLongValue((long) rps);
    }

    public long getTotalRows() {
        return totalRows;
    }

    public String getPrettyTotalRows() {
        return BenchmarkUtils.formatLongValue(totalRows);
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
}
