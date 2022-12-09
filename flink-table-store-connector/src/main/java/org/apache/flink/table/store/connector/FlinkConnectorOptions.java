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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.FactoryUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/** Options for flink connector. */
public class FlinkConnectorOptions {

    public static final String TABLE_STORE_PREFIX = "table-store.";

    public static final String NONE = "none";

    @Internal
    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> ROOT_PATH =
            ConfigOptions.key("root-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The root file path of the table store in the filesystem.");

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

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Define a custom parallelism for the scan source. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static String relativeTablePath(ObjectIdentifier tableIdentifier) {
        return String.format(
                "%s.catalog/%s.db/%s",
                tableIdentifier.getCatalogName(),
                tableIdentifier.getDatabaseName(),
                tableIdentifier.getObjectName());
    }

    @Internal
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
