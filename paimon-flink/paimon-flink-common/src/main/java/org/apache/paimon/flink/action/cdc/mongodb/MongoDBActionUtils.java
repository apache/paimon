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

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.ververica.cdc.connectors.base.options.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceBuilder;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.bson.Document;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for MongoDB Action. */
public class MongoDBActionUtils {

    public static final ConfigOption<String> FIELD_NAME =
            ConfigOptions.key("field.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Set the field names to be synchronized in the  `specified` mode.");

    public static final ConfigOption<String> PARSER_PATH =
            ConfigOptions.key("parser.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Configure the JSON parsing path for synchronizing field values in the `specified` mode.");

    public static final ConfigOption<String> START_MODE =
            ConfigOptions.key("schema.start.mode")
                    .stringType()
                    .defaultValue("dynamic")
                    .withDescription("Can choose between the `dynamic` and `specified` modes.");

    static MongoDBSource<String> buildMongodbSource(Configuration mongodbConfig, String tableList) {
        validateMongodbConfig(mongodbConfig);
        MongoDBSourceBuilder<String> sourceBuilder = MongoDBSource.builder();

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
        if ("initial".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if ("latest-offset".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        } else if ("timestamp".equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(
                    StartupOptions.timestamp(
                            mongodbConfig.get(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Map<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
        JsonDebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        return sourceBuilder.deserializer(schema).build();
    }

    private static void validateMongodbConfig(Configuration mongodbConfig) {
        checkArgument(
                mongodbConfig.get(MongoDBSourceOptions.HOSTS) != null,
                String.format(
                        "mongodb-conf [%s] must be specified.", MongoDBSourceOptions.HOSTS.key()));

        checkArgument(
                mongodbConfig.get(MongoDBSourceOptions.DATABASE) != null,
                String.format(
                        "mongodb-conf [%s] must be specified.",
                        MongoDBSourceOptions.DATABASE.key()));
    }

    static Schema buildPaimonSchema(
            MongodbSchema mongodbSchema,
            List<String> specifiedPartitionKeys,
            Map<String, String> paimonConfig,
            boolean caseSensitive) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);

        Map<String, DataType> mongodbFields;
        if (caseSensitive) {
            mongodbFields = mongodbSchema.fields();
        } else {
            mongodbFields = new LinkedHashMap<>();
            for (Map.Entry<String, DataType> entry : mongodbSchema.fields().entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !mongodbFields.containsKey(fieldName.toLowerCase()),
                        String.format(
                                "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                                fieldName, mongodbSchema.tableName()));
                mongodbFields.put(fieldName.toLowerCase(), entry.getValue());
            }
        }

        for (Map.Entry<String, DataType> entry : mongodbFields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }

        builder.primaryKey(Lists.newArrayList("_id"));

        if (specifiedPartitionKeys.size() > 0) {
            builder.partitionKeys(specifiedPartitionKeys);
        }

        return builder.build();
    }

    public static int getMongoDBVersion(Configuration mongodbConfig) {
        String hosts = mongodbConfig.get(MongoDBSourceOptions.HOSTS);
        String databaseName = mongodbConfig.get(MongoDBSourceOptions.DATABASE);

        String url = String.format("mongodb://%s/%s", hosts, databaseName);
        try (MongoClient mongoClient = MongoClients.create(url)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            Document buildInfo = database.runCommand(new Document("buildInfo", 1));
            String[] split = ((String) buildInfo.get("version")).split("\\.");
            return Integer.parseInt(split[0]);
        }
    }
}
