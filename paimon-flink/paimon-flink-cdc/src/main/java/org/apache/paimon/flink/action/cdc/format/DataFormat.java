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

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.DatabaseSyncTableFilter;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.canal.CanalRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumRecordParser;
import org.apache.paimon.flink.action.cdc.format.json.JsonRecordParser;
import org.apache.paimon.flink.action.cdc.format.maxwell.MaxwellRecordParser;
import org.apache.paimon.flink.action.cdc.format.ogg.OggRecordParser;

import java.util.List;

/**
 * Enumerates the supported data formats for message queue and provides a mechanism to create their
 * associated {@link RecordParser}.
 *
 * <p>Each data format is associated with a specific implementation of {@link RecordParserFactory},
 * which can be used to create instances of {@link RecordParser} for that format.
 */
public enum DataFormat {
    CANAL_JSON(CanalRecordParser::new),
    OGG_JSON(OggRecordParser::new),
    MAXWELL_JSON(MaxwellRecordParser::new),
    DEBEZIUM_JSON(DebeziumRecordParser::new),
    JSON(JsonRecordParser::new);
    // Add more data formats here if needed

    private final RecordParserFactory parser;

    DataFormat(RecordParserFactory parser) {
        this.parser = parser;
    }

    /**
     * Creates a new instance of {@link RecordParser} for this data format with the specified
     * configurations.
     *
     * @param computedColumns List of computed columns to be considered by the parser.
     * @return A new instance of {@link RecordParser}.
     */
    public RecordParser createParser(
            TypeMapping typeMapping,
            List<ComputedColumn> computedColumns,
            DatabaseSyncTableFilter databaseSyncTableFilter) {
        return parser.createParser(typeMapping, computedColumns, databaseSyncTableFilter);
    }

    public RecordParser createParser(
            TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        return parser.createParser(typeMapping, computedColumns, null);
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
