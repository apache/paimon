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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.AbstractSourceInfo;

import java.util.Collections;
import java.util.Map;

/**
 * Enumerates the metadata processing behaviors for MySQL CDC related data.
 *
 * <p>This enumeration provides definitions for various MySQL CDC metadata keys along with their
 * associated data types and converters. Each enum entry represents a specific type of metadata
 * related to MySQL CDC and provides a mechanism to read and process this metadata from a given
 * {@link JsonNode} source.
 *
 * <p>The provided converters, which are of type {@link CdcMetadataConverter}, define how the raw
 * metadata is transformed or processed for each specific metadata key.
 */
public enum MySqlMetadataProcessor {
    /** Name of the table that contain the row. */
    TABLE_NAME(
            "table_name",
            DataTypes.STRING().notNull(),
            new CdcMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<String, String> read(JsonNode record) {
                    String table =
                            record.get("source").get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
                    return Collections.singletonMap("table_name", table);
                }

                @Override
                public DataType getDataType() {
                    return DataTypes.STRING().notNull();
                }

                @Override
                public String getColumnName() {
                    return "table_name";
                }
            }),

    /** Name of the database that contain the row. */
    DATABASE_NAME(
            "database_name",
            DataTypes.STRING().notNull(),
            new CdcMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<String, String> read(JsonNode record) {
                    String database =
                            record.get("source").get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();
                    return Collections.singletonMap("database_name", database);
                }

                @Override
                public DataType getDataType() {
                    return DataTypes.STRING().notNull();
                }

                @Override
                public String getColumnName() {
                    return "database_name";
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    OP_TS(
            "op_ts",
            DataTypes.TIMESTAMP(3).notNull(),
            new CdcMetadataConverter() {
                private static final long serialVersionUID = 1L;

                @Override
                public Map<String, String> read(JsonNode record) {
                    String timestamp =
                            record.get("source").get(AbstractSourceInfo.TIMESTAMP_KEY).asText();
                    return Collections.singletonMap("op_ts", timestamp);
                }

                @Override
                public DataType getDataType() {
                    return DataTypes.TIMESTAMP(3).notNull();
                }

                @Override
                public String getColumnName() {
                    return "op_ts";
                }
            });

    private final String key;

    private final DataType dataType;

    private final CdcMetadataConverter converter;

    MySqlMetadataProcessor(String key, DataType dataType, CdcMetadataConverter converter) {
        this.key = key;
        this.dataType = dataType;
        this.converter = converter;
    }

    public String getKey() {
        return key;
    }

    public DataType getDataType() {
        return dataType;
    }

    public CdcMetadataConverter getConverter() {
        return converter;
    }
}
