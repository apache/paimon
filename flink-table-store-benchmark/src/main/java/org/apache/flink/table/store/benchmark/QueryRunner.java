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

package org.apache.flink.table.store.benchmark;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.store.benchmark.metric.FlinkRestClient;
import org.apache.flink.table.store.benchmark.metric.JobBenchmarkMetric;
import org.apache.flink.table.store.benchmark.metric.MetricReporter;
import org.apache.flink.table.store.benchmark.utils.AutoClosableProcess;
import org.apache.flink.table.store.benchmark.utils.BenchmarkGlobalConfiguration;
import org.apache.flink.table.store.benchmark.utils.BenchmarkUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/** Runner of a single benchmark query. */
public class QueryRunner {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

    private final Query query;
    private final Sink sink;
    private final Path flinkDist;
    private final MetricReporter metricReporter;
    private final FlinkRestClient flinkRestClient;

    public QueryRunner(
            Query query,
            Sink sink,
            Path flinkDist,
            MetricReporter metricReporter,
            FlinkRestClient flinkRestClient) {
        this.query = query;
        this.sink = sink;
        this.flinkDist = flinkDist;
        this.metricReporter = metricReporter;
        this.flinkRestClient = flinkRestClient;
    }

    public JobBenchmarkMetric run() {
        try {
            BenchmarkUtils.printAndLog(
                    LOG,
                    "==================================================================\n"
                            + "Start to run query "
                            + query.name()
                            + " with sink "
                            + sink.name());

            String sinkPath =
                    BenchmarkGlobalConfiguration.loadConfiguration()
                            .getString(BenchmarkOptions.SINK_PATH);
            if (sinkPath == null) {
                throw new IllegalArgumentException(
                        BenchmarkOptions.SINK_PATH.key() + " must be set");
            }

            // before submitting SQL, clean sink path
            FileSystem fs = new org.apache.flink.core.fs.Path(sinkPath).getFileSystem();
            fs.delete(new org.apache.flink.core.fs.Path(sinkPath), true);

            // run initialization SQL if needed
            Optional<String> beforeSql = query.getBeforeSql(sink, sinkPath);
            if (beforeSql.isPresent()) {
                BenchmarkUtils.printAndLog(LOG, "Initializing query " + query.name());
                String beforeJobId = submitSQLJob(beforeSql.get());
                flinkRestClient.waitUntilJobFinished(beforeJobId);
            }

            String insertJobId = submitSQLJob(query.getWriteBenchmarkSql(sink, sinkPath));
            // blocking until collect enough metrics
            JobBenchmarkMetric metrics = metricReporter.reportMetric(insertJobId);
            // cancel job
            BenchmarkUtils.printAndLog(
                    LOG, "Stop job query " + query.name() + " with sink " + sink.name());
            if (flinkRestClient.isJobRunning(insertJobId)) {
                flinkRestClient.cancelJob(insertJobId);
            }

            String queryJobId = submitSQLJob(query.getReadBenchmarkSql(sink, sinkPath));
            long queryJobRunMillis = flinkRestClient.waitUntilJobFinished(queryJobId);
            double numRecordsRead =
                    flinkRestClient.getTotalNumRecords(
                            queryJobId, flinkRestClient.getSourceVertexId(queryJobId));
            metrics.setQueryRps(numRecordsRead / (queryJobRunMillis / 1000.0));
            System.out.println(
                    "Scan RPS for query " + query.name() + " is " + metrics.getPrettyQueryRps());

            return metrics;
        } catch (IOException e) {
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
