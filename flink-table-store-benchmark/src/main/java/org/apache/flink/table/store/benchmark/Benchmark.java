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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.store.benchmark.metric.FlinkRestClient;
import org.apache.flink.table.store.benchmark.metric.JobBenchmarkMetric;
import org.apache.flink.table.store.benchmark.metric.MetricReporter;
import org.apache.flink.table.store.benchmark.metric.cpu.CpuMetricReceiver;
import org.apache.flink.table.store.benchmark.utils.BenchmarkGlobalConfiguration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The entry point to run benchmark. */
public class Benchmark {

    private static final Option LOCATION =
            new Option("l", "location", true, "Benchmark directory.");
    private static final Option QUERIES =
            new Option(
                    "q",
                    "queries",
                    true,
                    "Queries to run. If the value is 'all', all queries will be run.");
    private static final Option SINKS =
            new Option(
                    "s",
                    "sinks",
                    true,
                    "Sinks to run. If the value is 'all', all sinks will be run.");

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new RuntimeException(
                    "Usage: --location /path/to/benchmark --queries q1,q3 --sinks table_store,hudi_merge_on_read");
        }

        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);

        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        Path queriesLocation = location.resolve("queries");
        Path sinksLocation = location.resolve("sinks");

        String queriesValue = line.getOptionValue(QUERIES.getOpt());
        List<String> queries;
        if ("all".equals(queriesValue.toLowerCase())) {
            queries = getAllQueries(queriesLocation);
        } else {
            queries = Arrays.asList(queriesValue.split(","));
        }
        System.out.println("Benchmark Queries: " + queries);

        String sinksValue = line.getOptionValue(SINKS.getOpt());
        List<String> sinks;
        if ("all".equals(sinksValue.toLowerCase())) {
            sinks = getAllSinks(sinksLocation);
        } else {
            sinks = Arrays.asList(sinksValue.split(","));
        }
        System.out.println("Benchmark Sinks: " + queries);

        runQueries(queries, sinks, queriesLocation, sinksLocation);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(LOCATION);
        options.addOption(QUERIES);
        options.addOption(SINKS);
        return options;
    }

    private static List<String> getAllQueries(Path queriesLocation) throws IOException {
        return Files.list(queriesLocation)
                .map(p -> FilenameUtils.removeExtension(p.getFileName().toString()))
                .sorted()
                .collect(Collectors.toList());
    }

    private static List<String> getAllSinks(Path sinksLocation) throws IOException {
        return Files.list(sinksLocation)
                .map(p -> FilenameUtils.removeExtension(p.getFileName().toString()))
                .collect(Collectors.toList());
    }

    private static void runQueries(
            List<String> queries, List<String> sinks, Path queriesLocation, Path sinksLocation) {
        String flinkHome = System.getenv("FLINK_HOME");
        if (flinkHome == null) {
            throw new IllegalArgumentException("FLINK_HOME environment variable is not set.");
        }
        Path flinkDist = new File(flinkHome).toPath();

        // start metric servers
        Configuration benchmarkConf = BenchmarkGlobalConfiguration.loadConfiguration();
        String jmAddress = benchmarkConf.get(BenchmarkOptions.FLINK_REST_ADDRESS);
        int jmPort = benchmarkConf.get(BenchmarkOptions.FLINK_REST_PORT);
        String reporterAddress = benchmarkConf.get(BenchmarkOptions.METRIC_REPORTER_HOST);
        int reporterPort = benchmarkConf.get(BenchmarkOptions.METRIC_REPORTER_PORT);
        FlinkRestClient flinkRestClient = new FlinkRestClient(jmAddress, jmPort);
        CpuMetricReceiver cpuMetricReceiver = new CpuMetricReceiver(reporterAddress, reporterPort);
        cpuMetricReceiver.runServer();

        Duration monitorDelay = benchmarkConf.get(BenchmarkOptions.METRIC_MONITOR_DELAY);
        Duration monitorInterval = benchmarkConf.get(BenchmarkOptions.METRIC_MONITOR_INTERVAL);
        Duration monitorDuration = benchmarkConf.get(BenchmarkOptions.METRIC_MONITOR_DURATION);

        // start to run queries
        LinkedHashMap<String, JobBenchmarkMetric> totalMetrics = new LinkedHashMap<>();
        for (String queryName : queries) {
            for (String sinkName : sinks) {
                MetricReporter reporter =
                        new MetricReporter(
                                flinkRestClient,
                                cpuMetricReceiver,
                                monitorDelay,
                                monitorInterval,
                                monitorDuration);
                QueryRunner runner =
                        new QueryRunner(
                                queriesLocation.resolve(queryName + ".sql"),
                                sinksLocation.resolve(sinkName + ".sql"),
                                flinkDist,
                                reporter,
                                flinkRestClient);
                JobBenchmarkMetric metric = runner.run();
                totalMetrics.put(queryName + " - " + sinkName, metric);
            }
        }

        // print benchmark summary
        printSummary(totalMetrics);

        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    public static void printSummary(LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        if (totalMetrics.isEmpty()) {
            return;
        }
        System.err.println(
                "-------------------------------- Benchmark Results --------------------------------");
        int itemMaxLength = 27;
        System.err.println();
        printBPSSummary(itemMaxLength, totalMetrics);
        System.err.println();
    }

    private static void printBPSSummary(
            int itemMaxLength, LinkedHashMap<String, JobBenchmarkMetric> totalMetrics) {
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "", "");
        printLine(
                ' ',
                "|",
                itemMaxLength,
                " Benchmark Query",
                " Throughput (byte/s)",
                " Total Bytes",
                " Cores",
                " Throughput/Cores",
                " Avg Data Freshness",
                " Max Data Freshness",
                " Query Throughput (row/s)");
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "", "");

        for (Map.Entry<String, JobBenchmarkMetric> entry : totalMetrics.entrySet()) {
            JobBenchmarkMetric metric = entry.getValue();
            printLine(
                    ' ',
                    "|",
                    itemMaxLength,
                    entry.getKey(),
                    metric.getPrettyBps(),
                    metric.getPrettyTotalBytes(),
                    metric.getPrettyCpu(),
                    metric.getPrettyBpsPerCore(),
                    metric.getAvgDataFreshnessString(),
                    metric.getMaxDataFreshnessString(),
                    metric.getPrettyQueryRps());
        }
        printLine('-', "+", itemMaxLength, "", "", "", "", "", "", "", "");
    }

    private static void printLine(
            char charToFill, String separator, int itemMaxLength, String... items) {
        StringBuilder builder = new StringBuilder();
        for (String item : items) {
            builder.append(separator);
            builder.append(item);
            int left = itemMaxLength - item.length() - separator.length();
            for (int i = 0; i < left; i++) {
                builder.append(charToFill);
            }
        }
        builder.append(separator);
        System.err.println(builder.toString());
    }
}
