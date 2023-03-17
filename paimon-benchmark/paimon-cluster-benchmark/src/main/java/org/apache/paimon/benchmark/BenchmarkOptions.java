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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Benchmark execution options. */
public class BenchmarkOptions {

    public static final ConfigOption<Duration> METRIC_MONITOR_DELAY =
            ConfigOptions.key("benchmark.metric.monitor.delay")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription(
                            "When to monitor the metrics, default 1 minute after job is started.");

    public static final ConfigOption<Duration> METRIC_MONITOR_INTERVAL =
            ConfigOptions.key("benchmark.metric.monitor.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The interval to request the metrics.");

    public static final ConfigOption<String> METRIC_REPORTER_HOST =
            ConfigOptions.key("benchmark.metric.reporter.host")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("The metric reporter server host.");

    public static final ConfigOption<Integer> METRIC_REPORTER_PORT =
            ConfigOptions.key("benchmark.metric.reporter.port")
                    .intType()
                    .defaultValue(9098)
                    .withDescription("The metric reporter server port.");

    public static final ConfigOption<String> SINK_PATH =
            ConfigOptions.key("benchmark.sink.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Sinks will write test data to this path.");

    public static final ConfigOption<String> FLINK_REST_ADDRESS =
            ConfigOptions.key("flink.rest.address")
                    .stringType()
                    .defaultValue("localhost")
                    .withDescription("Flink REST address.");

    public static final ConfigOption<Integer> FLINK_REST_PORT =
            ConfigOptions.key("flink.rest.port")
                    .intType()
                    .defaultValue(8081)
                    .withDescription("Flink REST port.");
}
