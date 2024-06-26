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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.serialization.ConfluentAvroDeserializationSchema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;
import static org.apache.paimon.flink.action.cdc.MessageQueueSchemaUtils.SCHEMA_REGISTRY_URL;

/** A simple deserialization schema for {@link CdcSourceRecord}. */
public class KafkaDebeziumAvroDeserializationSchema
        implements KafkaDeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_IDENTITY_MAP_CAPACITY = 1000;

    private final String topic;

    /** The deserializer to deserialize Debezium Avro data. */
    private final ConfluentAvroDeserializationSchema avroDeserializer;

    public KafkaDebeziumAvroDeserializationSchema(Configuration cdcSourceConfig) {
        this.topic = KafkaActionUtils.findOneTopic(cdcSourceConfig);
        String schemaRegistryUrl = cdcSourceConfig.getString(SCHEMA_REGISTRY_URL);
        this.avroDeserializer =
                new ConfluentAvroDeserializationSchema(
                        new CachedSchemaRegistryClient(
                                schemaRegistryUrl, DEFAULT_IDENTITY_MAP_CAPACITY));
    }

    @Override
    public CdcSourceRecord deserialize(ConsumerRecord<byte[], byte[]> message) throws IOException {
        if (message.value() == null) {
            // skip tombstone messages
            return null;
        }

        GenericContainerWithVersion keyContainerWithVersion =
                this.avroDeserializer.deserialize(topic, true, message.key());
        GenericContainerWithVersion valueContainerWithVersion =
                this.avroDeserializer.deserialize(topic, false, message.value());
        GenericRecord key = null;
        if (keyContainerWithVersion != null) {
            key = (GenericRecord) keyContainerWithVersion.container();
        }
        GenericRecord value = (GenericRecord) valueContainerWithVersion.container();
        return new CdcSourceRecord(topic, key, value);
    }

    @Override
    public boolean isEndOfStream(CdcSourceRecord nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
