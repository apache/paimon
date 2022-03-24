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

package org.apache.flink.table.store.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.log.LogOptions.LogChangelogMode;
import org.apache.flink.table.store.log.LogOptions.LogConsistency;
import org.apache.flink.table.store.log.LogSinkProvider;
import org.apache.flink.table.store.sink.SinkRecord;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.function.Consumer;

/** A Kafka {@link LogSinkProvider}. */
public class KafkaLogSinkProvider implements LogSinkProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    @Nullable private final SerializationSchema<RowData> keySerializer;

    private final SerializationSchema<RowData> valueSerializer;

    private final LogConsistency consistency;

    private final LogChangelogMode changelogMode;

    public KafkaLogSinkProvider(
            String topic,
            Properties properties,
            @Nullable SerializationSchema<RowData> keySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogConsistency consistency,
            LogChangelogMode changelogMode) {
        this.topic = topic;
        this.properties = properties;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.consistency = consistency;
        this.changelogMode = changelogMode;
    }

    @Override
    public KafkaSink<SinkRecord> createSink() {
        KafkaSinkBuilder<SinkRecord> builder = KafkaSink.builder();
        switch (consistency) {
            case TRANSACTIONAL:
                builder.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("log-store-" + topic);
                break;
            case EVENTUAL:
                if (keySerializer == null) {
                    throw new IllegalArgumentException(
                            "Can not use EVENTUAL consistency mode for non-pk table.");
                }
                builder.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
                break;
        }

        return builder.setBootstrapServers(
                        properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString())
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(createSerializationSchema())
                .build();
    }

    @Override
    public Consumer<RecordMetadata> createMetadataConsumer(WriteCallback callback) {
        return meta -> callback.onCompletion(meta.partition(), meta.offset());
    }

    @VisibleForTesting
    KafkaLogSerializationSchema createSerializationSchema() {
        return new KafkaLogSerializationSchema(
                topic, keySerializer, valueSerializer, changelogMode);
    }
}
