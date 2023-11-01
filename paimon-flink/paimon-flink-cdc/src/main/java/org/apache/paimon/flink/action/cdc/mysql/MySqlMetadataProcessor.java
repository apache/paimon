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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

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
            new CdcMetadataConverter<DebeziumEvent.Source>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String read(DebeziumEvent.Source source) {
                    return source.table();
                }

                @Override
                public DataType dataType() {
                    return DataTypes.STRING().notNull();
                }

                @Override
                public String columnName() {
                    return "table_name";
                }
            }),

    /** Name of the database that contain the row. */
    DATABASE_NAME(
            new CdcMetadataConverter<DebeziumEvent.Source>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String read(DebeziumEvent.Source source) {
                    return source.db();
                }

                @Override
                public DataType dataType() {
                    return DataTypes.STRING().notNull();
                }

                @Override
                public String columnName() {
                    return "database_name";
                }
            }),

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    OP_TS(
            new CdcMetadataConverter<DebeziumEvent.Source>() {
                private static final long serialVersionUID = 1L;

                @Override
                public String read(DebeziumEvent.Source source) {
                    return DateTimeUtils.formatTimestamp(
                            Timestamp.fromEpochMillis(source.tsMs()), DateTimeUtils.LOCAL_TZ, 3);
                }

                @Override
                public DataType dataType() {
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
                }

                @Override
                public String columnName() {
                    return "op_ts";
                }
            });

    private final CdcMetadataConverter<?> converter;

    MySqlMetadataProcessor(CdcMetadataConverter<?> converter) {
        this.converter = converter;
    }

    private static final Map<String, CdcMetadataConverter<?>> CONVERTERS =
            Arrays.stream(MySqlMetadataProcessor.values())
                    .collect(
                            Collectors.toMap(
                                    value -> value.converter.columnName(),
                                    MySqlMetadataProcessor::converter));

    public static CdcMetadataConverter<?> converter(String column) {
        return checkNotNull(CONVERTERS.get(column));
    }

    private CdcMetadataConverter<?> converter() {
        return converter;
    }
}
