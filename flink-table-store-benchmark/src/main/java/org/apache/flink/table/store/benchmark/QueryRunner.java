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
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Runner of a single benchmark query. */
public class QueryRunner {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRunner.class);

    private static final Pattern SINK_DDL_TEMPLATE_PATTERN =
            Pattern.compile("-- __SINK_DDL_BEGIN__([\\s\\S]*?)-- __SINK_DDL_END__");

    private final Path queryPath;
    private final Path sinkTemplatePath;
    private final Path flinkDist;
    private final MetricReporter metricReporter;
    private final FlinkRestClient flinkRestClient;

    public QueryRunner(
            Path queryPath,
            Path sinkTemplatePath,
            Path flinkDist,
            MetricReporter metricReporter,
            FlinkRestClient flinkRestClient) {
        this.queryPath = queryPath;
        this.sinkTemplatePath = sinkTemplatePath;
        this.flinkDist = flinkDist;
        this.metricReporter = metricReporter;
        this.flinkRestClient = flinkRestClient;
    }

    public JobBenchmarkMetric run() {
        try {
            System.out.println(
                    "==================================================================");
            System.out.println(
                    "Start to run query "
                            + queryPath.getFileName()
                            + " with sink "
                            + sinkTemplatePath.getFileName());
            LOG.info("==================================================================");
            LOG.info(
                    "Start to run query "
                            + queryPath.getFileName()
                            + " with sink "
                            + sinkTemplatePath.getFileName());

            String sinkPath =
                    BenchmarkGlobalConfiguration.loadConfiguration()
                            .getString(BenchmarkOptions.SINK_PATH);
            if (sinkPath == null) {
                throw new IllegalArgumentException(
                        BenchmarkOptions.SINK_PATH.key() + " must be set");
            }
            String sinkProperties =
                    FileUtils.readFileUtf8(sinkTemplatePath.toFile())
                            .replace("${SINK_PATH}", sinkPath);
            String sqlTemplate = FileUtils.readFileUtf8(queryPath.toFile());
            Matcher m = SINK_DDL_TEMPLATE_PATTERN.matcher(sqlTemplate);
            if (!m.find()) {
                throw new IllegalArgumentException(
                        "Cannot find __SINK_DDL_BEGIN__ and __SINK_DDL_END__ in query "
                                + queryPath.getFileName()
                                + ". This query is not valid.");
            }
            String sinkDdlTemplate = m.group(1);

            String insertSql =
                    sqlTemplate
                            .replace("${SINK_NAME}", "S")
                            .replace("${DDL_TEMPLATE}", sinkProperties);
            String querySql =
                    "SET 'execution.runtime-mode' = 'batch';\n"
                            + sinkDdlTemplate
                                    .replace("${SINK_NAME}", "S")
                                    .replace("${DDL_TEMPLATE}", sinkProperties)
                            + sinkDdlTemplate
                                    .replace("${SINK_NAME}", "B")
                                    .replace("${DDL_TEMPLATE}", "'connector' = 'blackhole'")
                            + "INSERT INTO B SELECT * FROM S;";

            // before submitting SQL, clean sink path
            FileSystem fs = new org.apache.flink.core.fs.Path(sinkPath).getFileSystem();
            fs.delete(new org.apache.flink.core.fs.Path(sinkPath), true);

            String insertJobId = submitSQLJob(Arrays.asList(insertSql.split("\n")));
            // blocking until collect enough metrics
            JobBenchmarkMetric metrics = metricReporter.reportMetric(insertJobId);
            // cancel job
            System.out.println(
                    "Stop job query "
                            + queryPath.getFileName()
                            + " with sink "
                            + sinkTemplatePath.getFileName());
            LOG.info(
                    "Stop job query "
                            + queryPath.getFileName()
                            + " with sink "
                            + sinkTemplatePath.getFileName());
            if (flinkRestClient.isJobRunning(insertJobId)) {
                flinkRestClient.cancelJob(insertJobId);
            }

            String queryJobId = submitSQLJob(Arrays.asList(querySql.split("\n")));
            long queryJobRunMillis = flinkRestClient.waitUntilJobFinished(queryJobId);
            double numRecordsRead =
                    flinkRestClient.getTotalNumRecords(
                            queryJobId, flinkRestClient.getSourceVertexId(queryJobId));
            metrics.setQueryRps(numRecordsRead / (queryJobRunMillis / 1000.0));
            System.out.println(
                    "Scan RPS for query "
                            + queryPath.getFileName()
                            + " is "
                            + metrics.getPrettyQueryRps());

            return metrics;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String submitSQLJob(List<String> sqlLines) throws IOException {
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
                queryPath.getFileName(),
                sinkTemplatePath.getFileName());

        SqlClientStdoutProcessor stdoutProcessor = new SqlClientStdoutProcessor();
        AutoClosableProcess.create(commands.toArray(new String[0]))
                .setStdInputs(sqlLines.toArray(new String[0]))
                .setStdoutProcessor(stdoutProcessor) // logging the SQL statements and error message
                .runBlocking();

        if (stdoutProcessor.jobId == null) {
            throw new RuntimeException("Cannot determine job ID");
        }

        String jobId = stdoutProcessor.jobId;
        long endMillis = System.currentTimeMillis();
        LOG.info("SQL client submit millis = " + (endMillis - startMillis) + ", jobId = " + jobId);
        System.out.println(
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
