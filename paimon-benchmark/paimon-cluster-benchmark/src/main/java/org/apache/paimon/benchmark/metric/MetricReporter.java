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

import org.apache.paimon.benchmark.metric.cpu.CpuMetricReceiver;

import org.apache.flink.api.common.time.Deadline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A reporter to aggregate metrics and report summary results. */
public class MetricReporter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricReporter.class);

    private final Duration monitorDelay;
    private final Duration monitorInterval;
    private final Long monitorRowNumber;

    private final FlinkRestClient flinkRestClient;
    private final CpuMetricReceiver cpuMetricReceiver;
    private final List<BenchmarkMetric> metrics;
    private final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    private volatile Throwable error;

    public MetricReporter(
            FlinkRestClient flinkRestClient,
            CpuMetricReceiver cpuMetricReceiver,
            Duration monitorDelay,
            Duration monitorInterval,
            long monitorRowNumber) {
        this.monitorDelay = monitorDelay;
        this.monitorInterval = monitorInterval;
        this.monitorRowNumber = monitorRowNumber;

        this.flinkRestClient = flinkRestClient;
        this.cpuMetricReceiver = cpuMetricReceiver;
        this.metrics = new ArrayList<>();
    }

    private void submitMonitorThread(String jobId) {
        String vertexId = flinkRestClient.getSourceVertexId(jobId);
        this.service.scheduleWithFixedDelay(
                new MetricCollector(jobId, vertexId),
                0L,
                monitorInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    private void waitFor(Duration duration) {
        Deadline deadline = Deadline.fromNow(duration);
        while (deadline.hasTimeLeft()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (error != null) {
                throw new RuntimeException(error);
            }
        }
    }

    public JobBenchmarkMetric reportMetric(String name, String jobId) throws InterruptedException {
        System.out.printf("Monitor metrics after %s seconds.%n", monitorDelay.getSeconds());
        waitFor(monitorDelay);

        submitMonitorThread(jobId);
        System.out.printf("Start to monitor metrics for %d rows.%n", monitorRowNumber);
        flinkRestClient.waitUntilNumberOfRows(jobId, monitorRowNumber);

        // cleanup the resource
        this.close();

        if (metrics.isEmpty()) {
            throw new RuntimeException("The metric reporter doesn't collect any metrics.");
        }
        double sumRps = 0.0;
        long totalRows = 0;
        double sumCpu = 0.0;

        Long avgDataFreshness = 0L;
        Long maxDataFreshness = 0L;
        int validDataFreshnessCount = 0;

        for (BenchmarkMetric metric : metrics) {
            sumRps += metric.getRps();
            totalRows = metric.getTotalRows();
            sumCpu += metric.getCpu();
            if (metric.getDataFreshness() != null) {
                avgDataFreshness += metric.getDataFreshness();
                maxDataFreshness = Math.max(maxDataFreshness, metric.getDataFreshness());
                validDataFreshnessCount += 1;
            }
        }

        double avgRps = sumRps / metrics.size();
        double avgCpu = sumCpu / metrics.size();
        if (validDataFreshnessCount == 0) {
            avgDataFreshness = null;
            maxDataFreshness = null;
        } else {
            avgDataFreshness /= validDataFreshnessCount;
        }
        JobBenchmarkMetric metric =
                new JobBenchmarkMetric(
                        name, avgRps, totalRows, avgCpu, avgDataFreshness, maxDataFreshness);

        String message =
                String.format(
                        "Summary: Name = %s, "
                                + "Average Throughput = %s, "
                                + "Total Rows = %s, "
                                + "Cores = %s, "
                                + "Avg Data Freshness = %s, "
                                + "Max Data Freshness = %s",
                        name,
                        metric.getPrettyRps(),
                        metric.getPrettyTotalRows(),
                        metric.getPrettyCpu(),
                        metric.getAvgDataFreshnessString(),
                        metric.getMaxDataFreshnessString());

        System.out.println(message);
        LOG.info(message);
        return metric;
    }

    public void close() {
        service.shutdownNow();
    }

    private class MetricCollector implements Runnable {
        private final String jobId;
        private final String vertexId;

        private MetricCollector(String jobId, String vertexId) {
            this.jobId = jobId;
            this.vertexId = vertexId;
        }

        @Override
        public void run() {
            try {
                String sourceVertexId = flinkRestClient.getSourceVertexId(jobId);
                double tps = flinkRestClient.getNumRecordsPerSecond(jobId, sourceVertexId);
                double totalRows = flinkRestClient.getTotalNumRecords(jobId, sourceVertexId);
                Long dataFreshness = flinkRestClient.getDataFreshness(jobId);
                double cpu = cpuMetricReceiver.getTotalCpu();
                int tms = cpuMetricReceiver.getNumberOfTM();
                BenchmarkMetric metric =
                        new BenchmarkMetric(tps, (long) totalRows, cpu, dataFreshness);

                if (tps >= 0 && totalRows >= 0) {
                    // it's thread-safe to update metrics
                    metrics.add(metric);
                }

                // logging
                String message =
                        String.format(
                                "Current Throughput = %s, Total Rows = %s, Cores = %s (%s TMs), Data Freshness = %s",
                                metric.getPrettyRps(),
                                metric.getPrettyTotalRows(),
                                metric.getPrettyCpu(),
                                tms,
                                metric.getDataFreshnessString());

                System.out.println(message);
                LOG.info(message);
            } catch (Exception e) {
                error = e;
            }
        }
    }
}
