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

package org.apache.paimon.flink.kafka;

import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.log.LogSourceProvider;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** A Kafka {@link LogSourceProvider}. */
public class KafkaLogSourceProvider implements LogSourceProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    private final DataType physicalType;

    private final int[] primaryKey;

    @Nullable private final DeserializationSchema<RowData> primaryKeyDeserializer;

    private final DeserializationSchema<RowData> valueDeserializer;

    @Nullable private final int[][] projectFields;

    private final LogConsistency consistency;

    private final StartupMode scanMode;

    @Nullable private final Long timestampMills;

    public KafkaLogSourceProvider(
            String topic,
            Properties properties,
            DataType physicalType,
            int[] primaryKey,
            @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            @Nullable int[][] projectFields,
            LogConsistency consistency,
            StartupMode scanMode,
            @Nullable Long timestampMills) {
        this.topic = topic;
        this.properties = properties;
        this.physicalType = physicalType;
        this.primaryKey = primaryKey;
        this.primaryKeyDeserializer = primaryKeyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.projectFields = projectFields;
        this.consistency = consistency;
        this.scanMode = scanMode;
        this.timestampMills = timestampMills;
    }

    @Override
    public void preCreateSource() {
        // nothing to do before log source creating
    }

    @Override
    public KafkaSource<RowData> createSource(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (consistency) {
            case TRANSACTIONAL:
                // Add read committed for transactional consistency mode.
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
                break;
            case EVENTUAL:
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
                break;
        }

        return KafkaSource.<RowData>builder()
                .setTopics(topic)
                .setStartingOffsets(toOffsetsInitializer(bucketOffsets))
                .setDeserializer(createDeserializationSchema())
                .setGroupId(UUID.randomUUID().toString())
                .setProperties(properties)
                .build();
    }

    @VisibleForTesting
    KafkaRecordDeserializationSchema<RowData> createDeserializationSchema() {
        return KafkaRecordDeserializationSchema.of(
                new KafkaLogDeserializationSchema(
                        physicalType,
                        primaryKey,
                        primaryKeyDeserializer,
                        valueDeserializer,
                        projectFields));
    }

    private OffsetsInitializer toOffsetsInitializer(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (scanMode) {
            case LATEST_FULL:
                return bucketOffsets == null
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.offsets(toKafkaOffsets(bucketOffsets));
            case LATEST:
                return OffsetsInitializer.latest();
            case FROM_TIMESTAMP:
                if (timestampMills == null) {
                    throw new NullPointerException(
                            "Must specify a timestamp if you choose timestamp startup mode.");
                }
                return OffsetsInitializer.timestamp(timestampMills);
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + scanMode);
        }
    }

    private Map<TopicPartition, Long> toKafkaOffsets(Map<Integer, Long> bucketOffsets) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        bucketOffsets.forEach(
                (bucket, offset) -> offsets.put(new TopicPartition(topic, bucket), offset));
        return offsets;
    }
}
