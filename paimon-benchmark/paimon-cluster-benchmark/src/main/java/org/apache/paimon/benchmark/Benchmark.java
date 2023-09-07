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
import org.apache.paimon.benchmark.metric.cpu.CpuMetricReceiver;
import org.apache.paimon.benchmark.utils.BenchmarkGlobalConfiguration;
import org.apache.paimon.benchmark.utils.BenchmarkUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                    "Usage: --location /path/to/benchmark --queries q1 --sinks paimon");
        }

        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);

        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();

        String queriesValue = line.getOptionValue(QUERIES.getOpt());
        List<Query> queries = Query.load(location);
        if (!"all".equalsIgnoreCase(queriesValue)) {
            Set<String> wantedQueries =
                    Arrays.stream(queriesValue.split(","))
                            .map(String::trim)
                            .collect(Collectors.toSet());
            Set<String> supportedQueries =
                    queries.stream().map(Query::name).collect(Collectors.toSet());
            Set<String> unSupportedQueries = new HashSet<>(wantedQueries);
            unSupportedQueries.removeAll(supportedQueries);
            if (!unSupportedQueries.isEmpty()) {
                throw new RuntimeException(
                        "Unsupported queries: "
                                + unSupportedQueries
                                + ", current supported queries are "
                                + supportedQueries);
            }
            queries.removeIf(q -> !wantedQueries.contains(q.name()));
        }

        String sinksValue = line.getOptionValue(SINKS.getOpt());
        List<Sink> sinks = Sink.load(location);
        if (!"all".equalsIgnoreCase(sinksValue)) {
            Set<String> wantedSinks =
                    Arrays.stream(sinksValue.split(","))
                            .map(String::trim)
                            .collect(Collectors.toSet());
            Set<String> supportedSinks = sinks.stream().map(Sink::name).collect(Collectors.toSet());
            Set<String> unSupportedSinks = new HashSet<>(wantedSinks);
            unSupportedSinks.removeAll(supportedSinks);
            if (!unSupportedSinks.isEmpty()) {
                throw new RuntimeException(
                        "Unsupported sinks: "
                                + unSupportedSinks
                                + ", current supported sinks are "
                                + supportedSinks);
            }
            sinks.removeIf(s -> !wantedSinks.contains(s.name()));
        }

        runQueries(queries, sinks);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(LOCATION);
        options.addOption(QUERIES);
        options.addOption(SINKS);
        return options;
    }

    private static void runQueries(List<Query> queries, List<Sink> sinks) {
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

        // start to run queries
        LinkedHashMap<String, QueryRunner.Result> totalResults = new LinkedHashMap<>();
        for (Query query : queries) {
            for (Sink sink : sinks) {
                QueryRunner runner =
                        new QueryRunner(
                                query,
                                sink,
                                flinkDist,
                                flinkRestClient,
                                cpuMetricReceiver,
                                benchmarkConf);
                QueryRunner.Result result = runner.run();
                totalResults.put(query.name() + " - " + sink.name(), result);
            }
        }

        // print benchmark summary
        printSummary(totalResults);

        flinkRestClient.close();
        cpuMetricReceiver.close();
    }

    public static void printSummary(LinkedHashMap<String, QueryRunner.Result> totalResults) {
        if (totalResults.isEmpty()) {
            return;
        }
        System.err.println(
                "-------------------------------- Benchmark Results --------------------------------");
        int itemMaxLength = 27;
        System.err.println();
        printBPSSummary(itemMaxLength, totalResults);
        System.err.println();
    }

    private static void printBPSSummary(
            int itemMaxLength, LinkedHashMap<String, QueryRunner.Result> totalResults) {
        String[] emptyItems = new String[7];
        Arrays.fill(emptyItems, "");
        printLine('-', "+", itemMaxLength, emptyItems);
        printLine(
                ' ',
                "|",
                itemMaxLength,
                " Benchmark Query",
                " Throughput (rows/s)",
                " Total Rows",
                " Cores",
                " Throughput/Core",
                " Avg Data Freshness",
                " Max Data Freshness");
        printLine('-', "+", itemMaxLength, emptyItems);

        for (Map.Entry<String, QueryRunner.Result> entry : totalResults.entrySet()) {
            QueryRunner.Result result = entry.getValue();
            for (JobBenchmarkMetric metric : result.writeMetric) {
                printLine(
                        ' ',
                        "|",
                        itemMaxLength,
                        entry.getKey() + " - " + metric.getName(),
                        metric.getPrettyRps(),
                        metric.getPrettyTotalRows(),
                        metric.getPrettyCpu(),
                        metric.getPrettyRpsPerCore(),
                        metric.getAvgDataFreshnessString(),
                        metric.getMaxDataFreshnessString());
            }
            printLine(
                    ' ',
                    "|",
                    itemMaxLength,
                    entry.getKey() + " - Scan",
                    BenchmarkUtils.formatLongValue((long) result.scanRps),
                    "",
                    "",
                    "",
                    "",
                    "");
        }
        printLine('-', "+", itemMaxLength, emptyItems);
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
        System.err.println(builder);
    }
}
