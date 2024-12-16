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

package org.apache.paimon.benchmark;

import org.apache.paimon.benchmark.metric.FlinkRestClient;
import org.apache.paimon.benchmark.metric.JobBenchmarkMetric;
import org.apache.paimon.benchmark.metric.MetricReporter;
import org.apache.paimon.benchmark.metric.cpu.CpuMetricReceiver;
import org.apache.paimon.benchmark.utils.AutoClosableProcess;
import org.apache.paimon.benchmark.utils.BenchmarkGlobalConfiguration;
import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Runner of a single benchmark query. */
public class QueryRunner {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

    private final Query query;
    private final Sink sink;
    private final Path flinkDist;
    private final FlinkRestClient flinkRestClient;
    private final CpuMetricReceiver cpuMetricReceiver;
    private final Configuration benchmarkConf;

    public QueryRunner(
            Query query,
            Sink sink,
            Path flinkDist,
            FlinkRestClient flinkRestClient,
            CpuMetricReceiver cpuMetricReceiver,
            Configuration benchmarkConf) {
        this.query = query;
        this.sink = sink;
        this.flinkDist = flinkDist;
        this.flinkRestClient = flinkRestClient;
        this.cpuMetricReceiver = cpuMetricReceiver;
        this.benchmarkConf = benchmarkConf;
    }

    public Result run() {
        try {
            BenchmarkUtils.printAndLog(
                    LOG,
                    "==================================================================\n"
                            + "Start to run query "
                            + query.name()
                            + " with sink "
                            + sink.name());

            String sinkPathConfig =
                    BenchmarkGlobalConfiguration.loadConfiguration()
                            .get(BenchmarkOptions.SINK_PATH);
            if (sinkPathConfig == null) {
                throw new IllegalArgumentException(
                        BenchmarkOptions.SINK_PATH.key() + " must be set");
            }

            // before submitting SQL, clean sink path
            FileSystem fs = new org.apache.flink.core.fs.Path(sinkPathConfig).getFileSystem();
            fs.delete(new org.apache.flink.core.fs.Path(sinkPathConfig), true);

            String sinkPath = sinkPathConfig + "/data";
            String savepointPath = sinkPathConfig + "/savepoint";

            Duration monitorDelay = benchmarkConf.get(BenchmarkOptions.METRIC_MONITOR_DELAY);
            Duration monitorInterval = benchmarkConf.get(BenchmarkOptions.METRIC_MONITOR_INTERVAL);

            List<JobBenchmarkMetric> writeMetric = new ArrayList<>();
            for (Query.WriteSql sql : query.getWriteBenchmarkSql(sink, sinkPath)) {
                BenchmarkUtils.printAndLog(LOG, "Running SQL " + sql.name);
                String jobId = submitSQLJob(sql.text);
                MetricReporter metricReporter =
                        new MetricReporter(
                                flinkRestClient,
                                cpuMetricReceiver,
                                monitorDelay,
                                monitorInterval,
                                sql.rowNum);
                JobBenchmarkMetric metric = metricReporter.reportMetric(sql.name, jobId);
                writeMetric.add(metric);
                flinkRestClient.stopJobWithSavepoint(jobId, savepointPath);
                flinkRestClient.waitUntilJobFinished(jobId);
            }

            String queryJobId = submitSQLJob(query.getReadBenchmarkSql(sink, sinkPath));
            long queryJobRunMillis = flinkRestClient.waitUntilJobFinished(queryJobId);
            double numRecordsRead =
                    flinkRestClient.getTotalNumRecords(
                            queryJobId, flinkRestClient.getSourceVertexId(queryJobId));
            double scanRps = numRecordsRead / (queryJobRunMillis / 1000.0);
            System.out.println(
                    "Scan RPS for query "
                            + query.name()
                            + " is "
                            + BenchmarkUtils.formatLongValue((long) scanRps));

            return new Result(writeMetric, scanRps);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public String submitSQLJob(String sql) throws IOException {
        String[] sqlLines = sql.split("\n");
        long startMillis = System.currentTimeMillis();

        Path flinkBin = flinkDist.resolve("bin");
        final List<String> commands = new ArrayList<>();
        commands.add(flinkBin.resolve("sql-client.sh").toAbsolutePath().toString());
        commands.add("embedded");

        LOG.info(
                "\n================================================================================"
                        + "\nQuery {} with sink {} is running."
                        + "\n--------------------------------------------------------------------------------"
                        + "\n",
                query.name(),
                sink.name());

        SqlClientStdoutProcessor stdoutProcessor = new SqlClientStdoutProcessor();
        AutoClosableProcess.create(commands.toArray(new String[0]))
                .setStdInputs(sqlLines)
                .setStdoutProcessor(stdoutProcessor) // logging the SQL statements and error message
                .runBlocking();

        if (stdoutProcessor.jobId == null) {
            throw new RuntimeException("Cannot determine job ID");
        }

        String jobId = stdoutProcessor.jobId;
        long endMillis = System.currentTimeMillis();
        BenchmarkUtils.printAndLog(
                LOG,
                "SQL client submit millis = " + (endMillis - startMillis) + ", jobId = " + jobId);
        return jobId;
    }

    /** Result metric of the current query. */
    public static class Result {
        public final List<JobBenchmarkMetric> writeMetric;
        public final double scanRps;

        private Result(List<JobBenchmarkMetric> writeMetric, double scanRps) {
            this.writeMetric = writeMetric;
            this.scanRps = scanRps;
        }
    }

    private static class SqlClientStdoutProcessor implements Consumer<String> {

        private String jobId = null;

        @Override
        public void accept(String line) {
            if (line.contains("Job ID:")) {
                jobId = line.split(":")[1].trim();
            }
            LOG.info(line);
        }
    }
}
