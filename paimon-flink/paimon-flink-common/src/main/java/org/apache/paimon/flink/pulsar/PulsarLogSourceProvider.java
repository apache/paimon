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

package org.apache.paimon.flink.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.paimon.CoreOptions.LogConsistency;
import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * A Kafka {@link LogSourceProvider}.
 */
public class PulsarLogSourceProvider implements LogSourceProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    private final DataType physicalType;

    private final int[] primaryKey;

    @Nullable
    private final DeserializationSchema<RowData> primaryKeyDeserializer;

    private final DeserializationSchema<RowData> valueDeserializer;

    @Nullable
    private final int[][] projectFields;

    private final LogConsistency consistency;

    private final StartupMode scanMode;

    @Nullable
    private final Long timestampMills;

    public PulsarLogSourceProvider(
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
    public PulsarSource<RowData> createSource(@Nullable Map<Integer, Long> bucketOffsets) {
        // Pulsar read committed for transactional consistency mode is not supported.
        return PulsarSource.<RowData>builder()
                .setTopics(topic)
                .setStartCursor(toStartCursor(bucketOffsets))
                .setProperties(properties)
                .setDeserializationSchema(createDeserializationSchema())
                .setSubscriptionName(UUID.randomUUID().toString())
                .build();
    }

    @VisibleForTesting
    PulsarDeserializationSchema<RowData> createDeserializationSchema() {
        return new PulsarLogDeserializationSchema(
                physicalType,
                primaryKey,
                primaryKeyDeserializer,
                valueDeserializer,
                projectFields);
    }

    private StartCursor toStartCursor(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (scanMode) {
            case LATEST_FULL:
                return bucketOffsets == null
                        ? StartCursor.earliest()
                        : toPartitionedStartCursor(bucketOffsets);
            case LATEST:
                return StartCursor.latest();
            case FROM_TIMESTAMP:
                if (timestampMills == null) {
                    throw new NullPointerException(
                            "Must specify a timestamp if you choose timestamp startup mode.");
                }
                return StartCursor.fromPublishTime(timestampMills);
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + scanMode);
        }
    }

    private StartCursor toPartitionedStartCursor(Map<Integer, Long> bucketOffsets) {
        Map<Integer, MessageId> messageIdMap = new HashMap<>();
        bucketOffsets.forEach((k, v) -> {
            messageIdMap.put(k,  getMessageId(v));
        });
        return new PartitionedStartCursor(messageIdMap);
    }

    private MessageId getMessageId(long sequenceId) {
        // Demultiplex ledgerId and entryId from offset
        long ledgerId = sequenceId >>> 28;
        long entryId = sequenceId & 0x0F_FF_FF_FFL;

        return new MessageIdImpl(ledgerId, entryId, -1);
    }
}
