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

import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.formats.DataFormat;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.types.DataType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.kafkaPropertiesGroupId;
import static org.apache.paimon.flink.action.cdc.kafka.formats.DataFormat.getDataFormat;

/** Utility class to load canal kafka schema. */
public class KafkaSchema {

    private static final int MAX_RETRY = 5;
    private static final int POLL_TIMEOUT_MILLIS = 1000;
    private final String databaseName;
    private final String tableName;
    private final Map<String, DataType> fields;
    private final List<String> primaryKeys;

    public KafkaSchema(
            String databaseName,
            String tableName,
            Map<String, DataType> fields,
            List<String> primaryKeys) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public String tableName() {
        return tableName;
    }

    public String databaseName() {
        return databaseName;
    }

    public Map<String, DataType> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    private static KafkaConsumer<String, String> getKafkaEarliestConsumer(
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

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos.isEmpty()) {
            throw new IllegalArgumentException(
                    "Failed to find partition information for topic " + topic);
        }
        int firstPartition =
                partitionInfos.stream().map(PartitionInfo::partition).sorted().findFirst().get();
        Collection<TopicPartition> topicPartitions =
                Collections.singletonList(new TopicPartition(topic, firstPartition));
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        return consumer;
    }

    /**
     * Retrieves the Kafka schema for a given topic.
     *
     * @param kafkaConfig The configuration for Kafka.
     * @param topic The topic to retrieve the schema for.
     * @return The Kafka schema for the topic.
     * @throws KafkaSchemaRetrievalException If unable to retrieve the schema after max retries.
     */
    public static KafkaSchema getKafkaSchema(Configuration kafkaConfig, String topic)
            throws KafkaSchemaRetrievalException {
        KafkaConsumer<String, String> consumer = getKafkaEarliestConsumer(kafkaConfig, topic);
        int retry = 0;
        int retryInterval = 1000;

        DataFormat format = getDataFormat(kafkaConfig);
        RecordParser recordParser =
                format.createParser(true, new TableNameConverter(true), Collections.emptyList());

        while (true) {
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MILLIS));
            Iterable<ConsumerRecord<String, String>> records = consumerRecords.records(topic);
            Stream<ConsumerRecord<String, String>> recordStream =
                    StreamSupport.stream(records.spliterator(), false);

            Optional<KafkaSchema> kafkaSchema =
                    recordStream
                            .map(record -> recordParser.getKafkaSchema(record.value()))
                            .filter(Objects::nonNull)
                            .findFirst();

            if (kafkaSchema.isPresent()) {
                return kafkaSchema.get();
            }

            if (retry >= MAX_RETRY) {
                throw new KafkaSchemaRetrievalException(
                        String.format("Could not get metadata from server, topic: %s", topic));
            }

            sleepSafely(retryInterval);
            retryInterval *= 2;
            retry++;
        }
    }

    private static void sleepSafely(int duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Custom exception to indicate issues with Kafka schema retrieval. */
    public static class KafkaSchemaRetrievalException extends Exception {
        public KafkaSchemaRetrievalException(String message) {
            super(message);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaSchema)) {
            return false;
        }
        KafkaSchema that = (KafkaSchema) o;
        return databaseName.equals(that.databaseName)
                && tableName.equals(that.tableName)
                && fields.equals(that.fields)
                && primaryKeys.equals(that.primaryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName, fields, primaryKeys);
    }
}
