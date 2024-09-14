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

package org.apache.paimon.flink.action.cdc.format;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.function.Function;

/** Data format common implementation of {@link DataFormat}. */
public abstract class AbstractDataFormat implements DataFormat {

    /** Factory for creating AbstractRecordParser. */
    protected abstract RecordParserFactory parser();

    /** Deserializer for Kafka Record. */
    protected abstract Function<Configuration, KafkaDeserializationSchema<CdcSourceRecord>>
            kafkaDeserializer();

    /** Deserializer for Pulsar Record. */
    protected abstract Function<Configuration, DeserializationSchema<CdcSourceRecord>>
            pulsarDeserializer();

    @Override
    public AbstractRecordParser createParser(TypeMapping typeMapping) {
        return parser().createParser(typeMapping);
    }

    @Override
    public KafkaDeserializationSchema<CdcSourceRecord> createKafkaDeserializer(
            Configuration cdcSourceConfig) {
        return kafkaDeserializer().apply(cdcSourceConfig);
    }

    @Override
    public DeserializationSchema<CdcSourceRecord> createPulsarDeserializer(
            Configuration cdcSourceConfig) {
        return pulsarDeserializer().apply(cdcSourceConfig);
    }
}
