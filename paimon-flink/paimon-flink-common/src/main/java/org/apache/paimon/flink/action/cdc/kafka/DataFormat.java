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

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.parser.RecordParser;
import org.apache.paimon.flink.action.cdc.kafka.parser.canal.CanalRecordParser;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;

import java.util.List;

/** supported formats. */
public enum DataFormat {
    CANAL_JSON(CanalRecordParser::new);
    // Add more data formats here if needed

    private final DataFormatParser parser;

    DataFormat(DataFormatParser parser) {
        this.parser = parser;
    }

    public RecordParser createParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        return parser.createParser(caseSensitive, tableNameConverter, computedColumns);
    }

    public static DataFormat getDataFormat(Configuration kafkaConfig) {
        String formatStr = kafkaConfig.get(KafkaConnectorOptions.VALUE_FORMAT);
        try {
            return DataFormat.valueOf(formatStr.replace("-", "_").toUpperCase());
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    "This format: " + formatStr + " is not support.");
        }
    }

    /** Data format parser. */
    @FunctionalInterface
    public interface DataFormatParser {
        RecordParser createParser(
                boolean caseSensitive,
                TableNameConverter tableNameConverter,
                List<ComputedColumn> computedColumns);
    }
}
