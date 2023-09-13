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

import org.apache.paimon.flink.action.cdc.kafka.format.RecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.CollectionUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
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

    static KafkaSource<List<RichCdcMultiplexRecord>> buildKafkaSource(
            Configuration kafkaConfig, RecordParser recordParser) {
        validateKafkaConfig(kafkaConfig);
        List<String> topics =
                kafkaConfig.get(KafkaConnectorOptions.TOPIC).stream()
                        .flatMap(topic -> Arrays.stream(topic.split(",")))
                        .collect(Collectors.toList());
        Properties properties = subset(kafkaConfig, PROPERTIES_PREFIX, true);
        KafkaSourceBuilder<List<RichCdcMultiplexRecord>> kafkaSourceBuilder = KafkaSource.builder();
        kafkaSourceBuilder
                .setTopics(topics)
                .setDeserializer(new KafkaCdcRecordDeserializationSchema(recordParser))
                .setGroupId(kafkaPropertiesGroupId(kafkaConfig));
        kafkaSourceBuilder.setStartingOffsets(getStartingOffsets(kafkaConfig, properties));
        kafkaSourceBuilder.setProperties(properties);

        return kafkaSourceBuilder.build();
    }

    private static Properties subset(
            Configuration kafkaConfig, String prefix, boolean removePrefix) {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : kafkaConfig.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(prefix)) {
                key = removePrefix ? key.substring(prefix.length()) : key;
                properties.put(key, value);
            }
        }
        return properties;
    }

    private static OffsetsInitializer getStartingOffsets(
            Configuration kafkaConfig, Properties properties) {
        StartupMode startupMode =
                fromOption(kafkaConfig.get(KafkaConnectorOptions.SCAN_STARTUP_MODE));
        // see
        // https://github.com/apache/flink/blob/f32052a12309cfe38f66344cf6d4ab39717e44c8/flink-connectors/flink-connector-kafka/src/main/java/org/apache/flink/streaming/connectors/kafka/table/KafkaDynamicSource.java#L434
        switch (startupMode) {
            case EARLIEST:
                return OffsetsInitializer.earliest();
            case LATEST:
                return OffsetsInitializer.latest();
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        properties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                return OffsetsInitializer.committedOffsets(offsetResetStrategy);
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

                return OffsetsInitializer.offsets(offsets);
            case TIMESTAMP:
                long startupTimestampMillis =
                        kafkaConfig.get(KafkaConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
                return OffsetsInitializer.timestamp(startupTimestampMillis);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "kafka-conf [%s] value [%s] is not supported.",
                                KafkaConnectorOptions.SCAN_STARTUP_MODE.key(), startupMode));
        }
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

    private static void validateKafkaConfig(Configuration kafkaConfig) {
        checkArgument(
                kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT) != null,
                String.format(
                        "kafka-conf [%s] must be specified.",
                        KafkaConnectorOptions.VALUE_FORMAT.key()));

        checkArgument(
                !CollectionUtil.isNullOrEmpty(kafkaConfig.get(KafkaConnectorOptions.TOPIC)),
                String.format(
                        "kafka-conf [%s] must be specified.", KafkaConnectorOptions.TOPIC.key()));

        checkArgument(
                kafkaConfig.get(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS) != null,
                String.format(
                        "kafka-conf [%s] must be specified.",
                        KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS.key()));
    }

    public static String kafkaPropertiesGroupId(Configuration kafkaConfig) {
        String groupId = kafkaConfig.get(KafkaConnectorOptions.PROPS_GROUP_ID);
        if (StringUtils.isEmpty(groupId)) {
            groupId = UUID.randomUUID().toString();
            kafkaConfig.set(KafkaConnectorOptions.PROPS_GROUP_ID, groupId);
        }
        return groupId;
    }
}
