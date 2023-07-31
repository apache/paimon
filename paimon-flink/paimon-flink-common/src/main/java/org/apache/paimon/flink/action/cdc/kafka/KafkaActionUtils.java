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

package org.apache.paimon.flink.action.cdc.kafka;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.Expression;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

class KafkaActionUtils {

    public static final String PROPERTIES_PREFIX = "properties.";

    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    static void assertSchemaCompatible(TableSchema tableSchema, Schema kafkaCanalSchema) {
        if (!schemaCompatible(tableSchema, kafkaCanalSchema)) {
            throw new IllegalArgumentException(
                    "Paimon schema and Kafka schema are not compatible.\n"
                            + "Paimon fields are: "
                            + tableSchema.fields()
                            + ".\nKafka fields are: "
                            + kafkaCanalSchema.fields());
        }
    }

    static boolean schemaCompatible(TableSchema paimonSchema, Schema kafkaCanalSchema) {
        for (DataField field : kafkaCanalSchema.fields()) {
            int idx = paimonSchema.fieldNames().indexOf(field.name());
            if (idx < 0) {
                return false;
            }
            DataType type = paimonSchema.fields().get(idx).type();
            if (UpdatedDataFieldsProcessFunction.canConvert(field.type(), type)
                    != UpdatedDataFieldsProcessFunction.ConvertAction.CONVERT) {
                return false;
            }
        }
        return true;
    }

    static Schema buildPaimonSchema(
            KafkaSchema kafkaSchema,
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            List<ComputedColumn> computedColumns,
            Map<String, String> paimonConfig,
            boolean caseSensitive) {
        Schema.Builder builder = Schema.newBuilder();
        builder.options(paimonConfig);

        // build columns and primary keys from mySqlSchema
        Map<String, DataType> mySqlFields;
        List<String> mySqlPrimaryKeys;
        if (caseSensitive) {
            mySqlFields = kafkaSchema.fields();
            mySqlPrimaryKeys = kafkaSchema.primaryKeys();
        } else {
            mySqlFields = new LinkedHashMap<>();
            for (Map.Entry<String, DataType> entry : kafkaSchema.fields().entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !mySqlFields.containsKey(fieldName.toLowerCase()),
                        String.format(
                                "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                                fieldName, kafkaSchema.tableName()));
                mySqlFields.put(fieldName.toLowerCase(), entry.getValue());
            }
            mySqlPrimaryKeys =
                    kafkaSchema.primaryKeys().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toList());
        }

        for (Map.Entry<String, DataType> entry : mySqlFields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue(), null);
        }

        for (ComputedColumn computedColumn : computedColumns) {
            builder.column(computedColumn.columnName(), computedColumn.columnType());
        }

        if (specifiedPrimaryKeys.size() > 0) {
            for (String key : specifiedPrimaryKeys) {
                if (!mySqlFields.containsKey(key)
                        && computedColumns.stream().noneMatch(c -> c.columnName().equals(key))) {
                    throw new IllegalArgumentException(
                            "Specified primary key "
                                    + key
                                    + " does not exist in kafka topic's table or computed columns.");
                }
            }
            builder.primaryKey(specifiedPrimaryKeys);
        } else if (mySqlPrimaryKeys.size() > 0) {
            builder.primaryKey(mySqlPrimaryKeys);
        } else {
            throw new IllegalArgumentException(
                    "Primary keys are not specified. "
                            + "Also, can't infer primary keys from kafka topic's table schemas because "
                            + "Kafka topic's table have no primary keys or have different primary keys.");
        }

        if (specifiedPartitionKeys.size() > 0) {
            builder.partitionKeys(specifiedPartitionKeys);
        }

        return builder.build();
    }

    static KafkaSource<String> buildKafkaSource(Configuration kafkaConfig) {
        KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.builder();
        String groupId = kafkaConfig.get(KafkaConnectorOptions.PROPS_GROUP_ID);
        kafkaSourceBuilder
                .setTopics(kafkaConfig.get(KafkaConnectorOptions.TOPIC))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId(StringUtils.isEmpty(groupId) ? UUID.randomUUID().toString() : groupId);
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : kafkaConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                properties.put(key.substring(PROPERTIES_PREFIX.length()), value);
            }
        }

        StartupMode startupMode =
                fromOption(kafkaConfig.get(KafkaConnectorOptions.SCAN_STARTUP_MODE));
        // see
        // https://github.com/apache/flink/blob/f32052a12309cfe38f66344cf6d4ab39717e44c8/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/streaming/connectors/kafka/table/KafkaDynamicSource.java#L434
        switch (startupMode) {
            case EARLIEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case LATEST:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        properties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetStrategy));
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                String topic = kafkaConfig.get(KafkaConnectorOptions.TOPIC).get(0);

                String specificOffsetsStrOpt = kafkaConfig.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
                final Map<Integer, Long> offsetMap =
                        parseSpecificOffsets(
                                specificOffsetsStrOpt, SCAN_STARTUP_SPECIFIC_OFFSETS.key());
                offsetMap.forEach(
                        (partition, offset) -> {
                            final TopicPartition topicPartition =
                                    new TopicPartition(topic, partition);
                            offsets.put(topicPartition, offset);
                        });

                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                long startupTimestampMillis =
                        kafkaConfig.get(KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(startupTimestampMillis));
                break;
        }

        kafkaSourceBuilder.setProperties(properties);

        return kafkaSourceBuilder.build();
    }

    /**
     * Returns the {@link StartupMode} of Kafka Consumer by passed-in table-specific {@link
     * ScanStartupMode}.
     */
    private static StartupMode fromOption(ScanStartupMode scanStartupMode) {
        switch (scanStartupMode) {
            case EARLIEST_OFFSET:
                return StartupMode.EARLIEST;
            case LATEST_OFFSET:
                return StartupMode.LATEST;
            case GROUP_OFFSETS:
                return StartupMode.GROUP_OFFSETS;
            case SPECIFIC_OFFSETS:
                return StartupMode.SPECIFIC_OFFSETS;
            case TIMESTAMP:
                return StartupMode.TIMESTAMP;

            default:
                throw new TableException(
                        "Unsupported startup mode. Validator should have checked that.");
        }
    }

    private static OffsetResetStrategy getResetStrategy(String offsetResetConfig) {
        return Arrays.stream(OffsetResetStrategy.values())
                .filter(ors -> ors.name().equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                offsetResetConfig,
                                                Arrays.stream(OffsetResetStrategy.values())
                                                        .map(Enum::name)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }

    static List<ComputedColumn> buildComputedColumns(
            List<String> computedColumnArgs, Map<String, DataType> typeMapping) {
        List<ComputedColumn> computedColumns = new ArrayList<>();
        for (String columnArg : computedColumnArgs) {
            String[] kv = columnArg.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid computed column argument: %s. Please use format 'column-name=expr-name(args, ...)'.",
                                columnArg));
            }
            String columnName = kv[0].trim();
            String expression = kv[1].trim();
            // parse expression
            int left = expression.indexOf('(');
            int right = expression.indexOf(')');
            Preconditions.checkArgument(
                    left > 0 && right > left,
                    String.format(
                            "Invalid expression: %s. Please use format 'expr-name(args, ...)'.",
                            expression));

            String exprName = expression.substring(0, left);
            String[] args = expression.substring(left + 1, right).split(",");
            checkArgument(args.length >= 1, "Computed column needs at least one argument.");

            String fieldReference = args[0].trim();
            String[] literals =
                    Arrays.stream(args).skip(1).map(String::trim).toArray(String[]::new);
            checkArgument(
                    typeMapping.containsKey(fieldReference),
                    String.format(
                            "Referenced field '%s' is not in given MySQL fields: %s.",
                            fieldReference, typeMapping.keySet()));

            computedColumns.add(
                    new ComputedColumn(
                            columnName,
                            Expression.create(
                                    exprName,
                                    fieldReference,
                                    typeMapping.get(fieldReference),
                                    literals)));
        }

        return computedColumns;
    }

    /**
     * Parses specificOffsets String to Map.
     *
     * <p>specificOffsets String format was given as following:
     *
     * <pre>
     *     scan.startup.specific-offsets = partition:0,offset:42;partition:1,offset:300
     * </pre>
     *
     * @return specificOffsets with Map format, key is partition, and value is offset
     */
    public static Map<Integer, Long> parseSpecificOffsets(
            String specificOffsetsStr, String optionKey) {
        final Map<Integer, Long> offsetMap = new HashMap<>();
        final String[] pairs = specificOffsetsStr.split(";");
        final String validationExceptionMessage =
                String.format(
                        "Invalid properties '%s' should follow the format "
                                + "'partition:0,offset:42;partition:1,offset:300', but is '%s'.",
                        optionKey, specificOffsetsStr);

        if (pairs.length == 0) {
            throw new ValidationException(validationExceptionMessage);
        }

        for (String pair : pairs) {
            if (null == pair || !pair.contains(",")) {
                throw new ValidationException(validationExceptionMessage);
            }

            final String[] kv = pair.split(",");
            if (kv.length != 2
                    || !kv[0].startsWith(PARTITION + ':')
                    || !kv[1].startsWith(OFFSET + ':')) {
                throw new ValidationException(validationExceptionMessage);
            }

            String partitionValue = kv[0].substring(kv[0].indexOf(":") + 1);
            String offsetValue = kv[1].substring(kv[1].indexOf(":") + 1);
            try {
                final Integer partition = Integer.valueOf(partitionValue);
                final Long offset = Long.valueOf(offsetValue);
                offsetMap.put(partition, offset);
            } catch (NumberFormatException e) {
                throw new ValidationException(validationExceptionMessage, e);
            }
        }
        return offsetMap;
    }
}
