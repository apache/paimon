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

package org.apache.flink.table.store.connector;

import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.options.ConfigOption;
import org.apache.flink.table.store.options.ConfigOptions;
import org.apache.flink.table.store.options.description.Description;
import org.apache.flink.table.store.options.description.TextElement;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.store.options.ConfigOptions.key;

/** Options for flink connector. */
public class FlinkConnectorOptions {

    public static final String NONE = "none";

    public static final ConfigOption<String> LOG_SYSTEM =
            ConfigOptions.key("log.system")
                    .stringType()
                    .defaultValue(NONE)
                    .withDescription(
                            Description.builder()
                                    .text("The log system used to keep changes of the table.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Possible values:")
                                    .linebreak()
                                    .list(
                                            TextElement.text(
                                                    "\"none\": No log system, the data is written only to file store,"
                                                            + " and the streaming read will be directly read from the file store."))
                                    .list(
                                            TextElement.text(
                                                    "\"kafka\": Kafka log system, the data is double written to file"
                                                            + " store and kafka, and the streaming read will be read from kafka."))
                                    .build());

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<Boolean> SINK_SHUFFLE_BY_PARTITION =
            ConfigOptions.key("sink.partition-shuffle")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "The option to enable shuffle data by dynamic partition fields in sink phase for table store.");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Define a custom parallelism for the scan source. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<Boolean> STREAMING_READ_ATOMIC =
            ConfigOptions.key("streaming-read-atomic")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The option to enable return per iterator instead of per record in streaming read.")
                                    .text(
                                            "This can ensure that there will be no checkpoint segmentation in iterator consumption.")
                                    .linebreak()
                                    .text(
                                            "By default, streaming source checkpoint will be performed in any time,"
                                                    + " this means 'UPDATE_BEFORE' and 'UPDATE_AFTER' can be split into two checkpoint."
                                                    + " Downstream can see intermediate state.")
                                    .build());

    public static final ConfigOption<Duration> CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL =
            key("changelog-producer.compaction-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "When "
                                    + CoreOptions.CHANGELOG_PRODUCER.key()
                                    + " is set to "
                                    + CoreOptions.ChangelogProducer.FULL_COMPACTION.name()
                                    + ", full compaction will be constantly triggered after this interval.");

    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = FlinkConnectorOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(FlinkConnectorOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }
}
