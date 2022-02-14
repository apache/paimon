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

package org.apache.flink.table.store.log;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;

import java.time.Duration;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.store.utils.OptionsUtils.formatEnumOption;

/** Options for log store. */
public class LogOptions {

    public static final ConfigOption<LogStartupMode> SCAN =
            ConfigOptions.key("scan")
                    .enumType(LogStartupMode.class)
                    .defaultValue(LogStartupMode.FULL)
                    .withDescription(
                            Description.builder()
                                    .text("Specifies the startup mode for log consumer.")
                                    .linebreak()
                                    .list(formatEnumOption(LogStartupMode.FULL))
                                    .list(formatEnumOption(LogStartupMode.LATEST))
                                    .list(formatEnumOption(LogStartupMode.FROM_TIMESTAMP))
                                    .build());

    public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLS =
            ConfigOptions.key("scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode");

    public static final ConfigOption<Duration> RETENTION =
            ConfigOptions.key("retention")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "It means how long changes log will be kept. The default value is from the log system cluster.");

    public static final ConfigOption<LogConsistency> CONSISTENCY =
            ConfigOptions.key("consistency")
                    .enumType(LogConsistency.class)
                    .defaultValue(LogConsistency.TRANSACTIONAL)
                    .withDescription(
                            Description.builder()
                                    .text("Specifies the log consistency mode for table.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(LogConsistency.TRANSACTIONAL),
                                            formatEnumOption(LogConsistency.EVENTUAL))
                                    .build());

    public static final ConfigOption<LogChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(LogChangelogMode.class)
                    .defaultValue(LogChangelogMode.AUTO)
                    .withDescription(
                            Description.builder()
                                    .text("Specifies the log changelog mode for table.")
                                    .linebreak()
                                    .list(
                                            formatEnumOption(LogChangelogMode.AUTO),
                                            formatEnumOption(LogChangelogMode.ALL),
                                            formatEnumOption(LogChangelogMode.UPSERT))
                                    .build());

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key.format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Specifies the key message format of log system with primary key.");

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("debezium-json")
                    .withDescription("Specifies the message format of log system.");

    /** Specifies the startup mode for log consumer. */
    public enum LogStartupMode implements DescribedEnum {
        FULL(
                "full",
                "Performs a snapshot on the table upon first startup,"
                        + " and continue to read the latest changes."),

        LATEST("latest", "Start from the latest."),

        FROM_TIMESTAMP("from-timestamp", "Start from user-supplied timestamp.");

        private final String value;
        private final String description;

        LogStartupMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log consistency mode for table. */
    public enum LogConsistency implements DescribedEnum {
        TRANSACTIONAL(
                "transactional",
                "only the data after the checkpoint can be seen by readers, the latency depends on checkpoint interval."),

        EVENTUAL(
                "eventual",
                "Immediate data visibility, you may see some intermediate states, "
                        + "but eventually the right results will be produced, only works in table with primary key.");

        private final String value;
        private final String description;

        LogConsistency(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log changelog mode for table. */
    public enum LogChangelogMode implements DescribedEnum {
        AUTO("auto", "upsert for table with primary key, all for table without primary key.."),

        ALL("all", "the log system stores all changes including UPDATE_BEFORE."),

        UPSERT(
                "upsert",
                "The log system does not store the UPDATE_BEFORE changes, the log consumed job"
                        + " will automatically add the normalized node, relying on the state"
                        + " to generate the required update_before.");

        private final String value;
        private final String description;

        LogChangelogMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }
}
