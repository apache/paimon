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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.format.AbstractDataFormat;
import org.apache.paimon.flink.action.cdc.format.RecordParserFactory;
import org.apache.paimon.flink.action.cdc.kafka.KafkaDebeziumAvroDeserializationSchema;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarDebeziumAvroDeserializationSchema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.function.Function;

/**
 * Supports the message queue's debezium avro data format and provides definitions for the message
 * queue's record avro deserialization class and parsing class {@link DebeziumAvroRecordParser}.
 */
public class DebeziumAvroDataFormat extends AbstractDataFormat {

    @Override
    protected RecordParserFactory parser() {
        return DebeziumAvroRecordParser::new;
    }

    @Override
    protected Function<Configuration, KafkaDeserializationSchema<CdcSourceRecord>>
            kafkaDeserializer() {
        return KafkaDebeziumAvroDeserializationSchema::new;
    }

    @Override
    protected Function<Configuration, DeserializationSchema<CdcSourceRecord>> pulsarDeserializer() {
        return PulsarDebeziumAvroDeserializationSchema::new;
    }
}
