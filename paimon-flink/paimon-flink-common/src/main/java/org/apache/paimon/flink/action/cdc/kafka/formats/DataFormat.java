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

package org.apache.paimon.flink.action.cdc.kafka.formats;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.formats.canal.CanalRecordParser;
import org.apache.paimon.flink.action.cdc.kafka.formats.ogg.OggRecordParser;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import java.util.List;

/**
 * Enumerates the supported data formats and provides a mechanism to create their associated {@link
 * RecordParser}.
 *
 * <p>Each data format is associated with a specific implementation of {@link RecordParserFactory},
 * which can be used to create instances of {@link RecordParser} for that format.
 */
public enum DataFormat {
    CANAL_JSON(CanalRecordParser::new),
    OGG_JSON(OggRecordParser::new);
    // Add more data formats here if needed

    private final RecordParserFactory parser;

    DataFormat(RecordParserFactory parser) {
        this.parser = parser;
    }

    /**
     * Creates a new instance of {@link RecordParser} for this data format with the specified
     * configurations.
     *
     * @param caseSensitive Indicates whether the parser should be case-sensitive.
     * @param tableNameConverter Converter to transform table names.
     * @param computedColumns List of computed columns to be considered by the parser.
     * @return A new instance of {@link RecordParser}.
     */
    public RecordParser createParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        return parser.createParser(caseSensitive, tableNameConverter, computedColumns);
    }

    /**
     * Determines the appropriate {@link DataFormat} based on the provided Kafka configuration.
     *
     * @param kafkaConfig The Kafka configuration containing the desired data format.
     * @return The corresponding {@link DataFormat}.
     * @throws UnsupportedOperationException If the specified format in the configuration is not
     *     supported.
     */
    public static DataFormat getDataFormat(Configuration kafkaConfig) {
        String formatStr = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        try {
            return DataFormat.valueOf(formatStr.replace("-", "_").toUpperCase());
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    String.format("This format: %s is not supported.", formatStr));
        }
    }
}
