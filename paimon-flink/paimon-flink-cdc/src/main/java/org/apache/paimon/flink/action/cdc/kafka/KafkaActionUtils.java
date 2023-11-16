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

import org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils;
import org.apache.paimon.flink.action.cdc.format.DataFormat;
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
import org.apache.flink.util.CollectionUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for Kafka Action. */
public class KafkaActionUtils {

    public static final String PROPERTIES_PREFIX = "properties.";

    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    public static KafkaSource<String> buildKafkaSource(Configuration kafkaConfig) {
        validateKafkaConfig(kafkaConfig);
        KafkaSourceBuilder<String> kafkaSourceBuilder = KafkaSource.builder();

        List<String> topics =
                kafkaConfig.get(KafkaConnectorOptions.TOPIC).stream()
                        .flatMap(topic -> Arrays.stream(topic.split(",")))
                        .collect(Collectors.toList());

        kafkaSourceBuilder
                .setTopics(topics)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId(kafkaPropertiesGroupId(kafkaConfig));
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
    private static Map<Integer, Long> parseSpecificOffsets(
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

    private static String kafkaPropertiesGroupId(Configuration kafkaConfig) {
        String groupId = kafkaConfig.get(KafkaConnectorOptions.PROPS_GROUP_ID);
        if (StringUtils.isEmpty(groupId)) {
            groupId = UUID.randomUUID().toString();
            kafkaConfig.set(KafkaConnectorOptions.PROPS_GROUP_ID, groupId);
        }
        return groupId;
    }

    static DataFormat getDataFormat(Configuration kafkaConfig) {
        return DataFormat.fromConfigString(kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT));
    }

    static MessageQueueSchemaUtils.ConsumerWrapper getKafkaEarliestConsumer(
            Configuration kafkaConfig, String topic) {
        Properties props = new Properties();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaConfig.get(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaPropertiesGroupId(kafkaConfig));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // the return may be null in older versions of the Kafka client
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos == null || partitionInfos.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to find partition information for topic '%s'. Please check your "
                                    + "'topic' and 'bootstrap.servers' config.",
                            topic));
        }
        int firstPartition =
                partitionInfos.stream().map(PartitionInfo::partition).sorted().findFirst().get();
        Collection<TopicPartition> topicPartitions =
                Collections.singletonList(new TopicPartition(topic, firstPartition));
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        return new KafkaConsumerWrapper(consumer);
    }

    private static class KafkaConsumerWrapper implements MessageQueueSchemaUtils.ConsumerWrapper {

        private final KafkaConsumer<String, String> consumer;

        KafkaConsumerWrapper(KafkaConsumer<String, String> kafkaConsumer) {
            this.consumer = kafkaConsumer;
        }

        @Override
        public List<String> getRecords(String topic, int pollTimeOutMills) {
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(pollTimeOutMills));
            return StreamSupport.stream(consumerRecords.records(topic).spliterator(), false)
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
