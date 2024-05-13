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
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.canal.CanalRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumAvroRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumJsonRecordParser;
import org.apache.paimon.flink.action.cdc.format.maxwell.MaxwellRecordParser;
import org.apache.paimon.flink.action.cdc.format.ogg.OggRecordParser;
import org.apache.paimon.flink.action.cdc.kafka.KafkaDebeziumAvroDeserializationSchema;
import org.apache.paimon.flink.action.cdc.kafka.KafkaDebeziumJsonDeserializationSchema;
import org.apache.paimon.flink.action.cdc.pulsar.PulsarDebeziumAvroDeserializationSchema;
import org.apache.paimon.flink.action.cdc.serialization.CdcJsonDeserializationSchema;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.List;
import java.util.function.Function;

/**
 * Enumerates the supported data formats for message queue and provides a mechanism to create their
 * associated {@link RecordParser}.
 *
 * <p>Each data format is associated with a specific implementation of {@link RecordParserFactory},
 * which can be used to create instances of {@link RecordParser} for that format.
 */
public enum DataFormat {
    CANAL_JSON(
            CanalRecordParser::new,
            KafkaDebeziumJsonDeserializationSchema::new,
            CdcJsonDeserializationSchema::new),
    OGG_JSON(
            OggRecordParser::new,
            KafkaDebeziumJsonDeserializationSchema::new,
            CdcJsonDeserializationSchema::new),
    MAXWELL_JSON(
            MaxwellRecordParser::new,
            KafkaDebeziumJsonDeserializationSchema::new,
            CdcJsonDeserializationSchema::new),
    DEBEZIUM_JSON(
            DebeziumJsonRecordParser::new,
            KafkaDebeziumJsonDeserializationSchema::new,
            CdcJsonDeserializationSchema::new),
    DEBEZIUM_AVRO(
            DebeziumAvroRecordParser::new,
            KafkaDebeziumAvroDeserializationSchema::new,
            PulsarDebeziumAvroDeserializationSchema::new);
    // Add more data formats here if needed

    private final RecordParserFactory parser;
    // Deserializer for Kafka
    private final Function<Configuration, KafkaDeserializationSchema<CdcSourceRecord>>
            kafkaDeserializer;
    // Deserializer for Pulsar
    private final Function<Configuration, DeserializationSchema<CdcSourceRecord>>
            pulsarDeserializer;

    DataFormat(
            RecordParserFactory parser,
            Function<Configuration, KafkaDeserializationSchema<CdcSourceRecord>> kafkaDeserializer,
            Function<Configuration, DeserializationSchema<CdcSourceRecord>> pulsarDeserializer) {
        this.parser = parser;
        this.kafkaDeserializer = kafkaDeserializer;
        this.pulsarDeserializer = pulsarDeserializer;
    }

    /**
     * Creates a new instance of {@link RecordParser} for this data format with the specified
     * configurations.
     *
     * @param caseSensitive Indicates whether the parser should be case-sensitive.
     * @param computedColumns List of computed columns to be considered by the parser.
     * @return A new instance of {@link RecordParser}.
     */
    public RecordParser createParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        return parser.createParser(caseSensitive, typeMapping, computedColumns);
    }

    public KafkaDeserializationSchema<CdcSourceRecord> createKafkaDeserializer(
            Configuration cdcSourceConfig) {
        return kafkaDeserializer.apply(cdcSourceConfig);
    }

    public DeserializationSchema<CdcSourceRecord> createPulsarDeserializer(
            Configuration cdcSourceConfig) {
        return pulsarDeserializer.apply(cdcSourceConfig);
    }

    /** Returns the configuration string representation of this data format. */
    public String asConfigString() {
        return this.name().toLowerCase().replace("_", "-");
    }

    public static DataFormat fromConfigString(String format) {
        try {
            return DataFormat.valueOf(format.replace("-", "_").toUpperCase());
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    String.format("This format: %s is not supported.", format));
        }
    }
}
