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

import org.apache.paimon.flink.action.cdc.kafka.canal.CanalRecordParser;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.paimon.flink.action.cdc.kafka.KafkaActionUtils.kafkaPropertiesGroupId;

/** Utility class to load canal kafka schema. */
public class KafkaSchema {

    private static final int MAX_RETRY = 100;

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
        Collection<TopicPartition> topicPartitions = Collections.singletonList(new TopicPartition(topic, firstPartition));
        consumer.assign(topicPartitions);

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
        for (TopicPartition tp : topicPartitions) {
            Long offset = beginningOffsets.get(tp);
            consumer.seek(tp, offset);
        }

        return consumer;
    }

    public static KafkaSchema getKafkaSchema(Configuration kafkaConfig, String topic)
            throws Exception {
        KafkaConsumer<String, String> consumer = getKafkaEarliestConsumer(kafkaConfig, topic);

        int retry = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String format = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
                if ("canal-json".equals(format)) {
                    CanalRecordParser parser = new CanalRecordParser(true, Collections.emptyList());
                    KafkaSchema kafkaSchema = parser.getKafkaSchema(record.value());
                    if (kafkaSchema != null) {
                        return kafkaSchema;
                    }
                } else {
                    throw new UnsupportedOperationException(
                            "This format: " + format + " is not support.");
                }
            }
            if (retry == MAX_RETRY) {
                throw new Exception("Could not get metadata from server,topic :" + topic);
            }
            Thread.sleep(100);
            retry++;
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
