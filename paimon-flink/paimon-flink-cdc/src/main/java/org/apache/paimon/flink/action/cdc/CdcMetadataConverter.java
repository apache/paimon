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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.AbstractSourceInfo;

import java.io.Serializable;
import java.util.TimeZone;

/**
 * A functional interface for converting CDC metadata.
 *
 * <p>This interface provides a mechanism to convert Change Data Capture (CDC) metadata from a given
 * {@link JsonNode} source. Implementations of this interface can be used to process and transform
 * metadata entries from CDC sources.
 */
public interface CdcMetadataConverter extends Serializable {

    String read(JsonNode payload);

    DataType dataType();

    String columnName();

    /** Name of the database that contain the row. */
    class DatabaseNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "database_name";
        }
    }

    /** Name of the table that contain the row. */
    class TableNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "table_name";
        }
    }

    /** Name of the schema that contain the row. */
    class SchemaNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.SCHEMA_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "schema_name";
        }
    }

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    class OpTsConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return DateTimeUtils.formatTimestamp(
                    Timestamp.fromEpochMillis(
                            source.get(AbstractSourceInfo.TIMESTAMP_KEY).asLong()),
                    TimeZone.getDefault(),
                    3);
        }

        @Override
        public DataType dataType() {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
        }

        @Override
        public String columnName() {
            return "op_ts";
        }
    }
}
