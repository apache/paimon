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

package org.apache.paimon.flink.action.cdc.mongodb;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.serialization.CdcDebeziumDeserializationSchema;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;

import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for MongoDB-related actions.
 *
 * <p>This class provides a set of utility methods to facilitate the creation and configuration of
 * MongoDB sources, as well as the construction of Paimon schemas based on MongoDB schemas. It also
 * includes methods for validating MongoDB configurations and fetching MongoDB version information.
 *
 * <p>Key functionalities include:
 *
 * <ul>
 *   <li>Building MongoDB sources with various configurations.
 *   <li>Constructing Paimon schemas based on MongoDB schemas.
 *   <li>Validating essential MongoDB configurations.
 * </ul>
 *
 * <p>Note: This utility class is designed to be used in conjunction with Flink and Paimon
 * integrations.
 */
public class MongoDBActionUtils {

    private static final String INITIAL_MODE = "initial";
    private static final String LATEST_OFFSET_MODE = "latest-offset";
    private static final String TIMESTAMP_MODE = "timestamp";
    private static final String SNAPSHOT_MODE = "snapshot";

    public static final ConfigOption<String> FIELD_NAME =
            ConfigOptions.key("field.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Field names to synchronize when in `specified` mode.");

    public static final ConfigOption<String> PARSER_PATH =
            ConfigOptions.key("parser.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "JSON parsing path for field synchronization in `specified` mode.");

    public static final ConfigOption<String> START_MODE =
            ConfigOptions.key("schema.start.mode")
                    .stringType()
                    .defaultValue("dynamic")
                    .withDescription("Mode selection: `dynamic` or `specified`.");

    public static final ConfigOption<Boolean> DEFAULT_ID_GENERATION =
            ConfigOptions.key("default.id.generation")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Determines whether to use the default MongoDB _id generation strategy. If set to true, the default _id generation will remove the outer $oid nesting. If set to false, no additional processing will be done on the _id field.");

    public static MongoDBSource<CdcSourceRecord> buildMongodbSource(
            Configuration mongodbConfig, String tableList) {
        MongoDBSourceBuilder<CdcSourceRecord> sourceBuilder = MongoDBSource.builder();

        if (mongodbConfig.contains(MongoDBSourceOptions.USERNAME)
                && mongodbConfig.contains(MongoDBSourceOptions.PASSWORD)) {
            sourceBuilder
                    .username(mongodbConfig.get(MongoDBSourceOptions.USERNAME))
                    .password(mongodbConfig.get(MongoDBSourceOptions.PASSWORD));
        }
        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.CONNECTION_OPTIONS))
                .ifPresent(sourceBuilder::connectionOptions);
        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.BATCH_SIZE))
                .ifPresent(sourceBuilder::batchSize);
        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS))
                .ifPresent(sourceBuilder::heartbeatIntervalMillis);
        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.SCHEME))
                .ifPresent(sourceBuilder::scheme);

        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.POLL_MAX_BATCH_SIZE))
                .ifPresent(sourceBuilder::pollMaxBatchSize);

        Optional.ofNullable(mongodbConfig.get(MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS))
                .ifPresent(sourceBuilder::pollAwaitTimeMillis);

        sourceBuilder
                .hosts(mongodbConfig.get(MongoDBSourceOptions.HOSTS))
                .databaseList(mongodbConfig.get(MongoDBSourceOptions.DATABASE))
                .collectionList(tableList);

        String startupMode = mongodbConfig.get(SourceOptions.SCAN_STARTUP_MODE);
        switch (startupMode.toLowerCase()) {
            case INITIAL_MODE:
                sourceBuilder.startupOptions(StartupOptions.initial());
                break;
            case LATEST_OFFSET_MODE:
                sourceBuilder.startupOptions(StartupOptions.latest());
                break;
            case TIMESTAMP_MODE:
                sourceBuilder.startupOptions(
                        StartupOptions.timestamp(
                                mongodbConfig.get(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
                break;
            case SNAPSHOT_MODE:
                sourceBuilder.startupOptions(StartupOptions.snapshot());
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown scan.startup.mode='%s'. Valid scan.startup.mode for MongoDB CDC are [initial, latest-offset, timestamp, snapshot]",
                                startupMode));
        }

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        CdcDebeziumDeserializationSchema schema =
                new CdcDebeziumDeserializationSchema(false, customConverterConfigs);

        return sourceBuilder.deserializer(schema).build();
    }

    public static CdcTimestampExtractor createCdcTimestampExtractor() {
        return new MongoDBCdcTimestampExtractor();
    }
}
