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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.TimeZone;

/**
 * Kafka-specific implementations of {@link CdcMetadataConverter} for extracting Kafka message
 * metadata.
 *
 * <p>These converters read from the generic metadata map in {@link CdcSourceRecord} to extract
 * Kafka-specific metadata like topic, partition, offset, timestamp, and timestamp type.
 */
public class KafkaMetadataConverter implements CdcMetadataConverter {

    protected static final String KAFKA_METADATA_COLUMN_PREFIX = "__kafka_";
    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final DataType dataType;

    public KafkaMetadataConverter(String fieldName, DataType dataType) {
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    @Override
    public String read(JsonNode source) {
        throw new UnsupportedOperationException(
                "Kafka metadata converters should be used with CdcSourceRecord, not JsonNode");
    }

    @Override
    public String read(CdcSourceRecord record) {
        Object metadata = record.getMetadata(this.fieldName);
        return metadata != null ? metadata.toString() : null;
    }

    @Override
    public DataType dataType() {
        return this.dataType;
    }

    @Override
    public String columnName() {
        return this.fieldName;
    }

    /** Converter for Kafka topic name. */
    public static class TopicConverter extends KafkaMetadataConverter {
        private static final long serialVersionUID = 1L;

        public TopicConverter() {
            super("topic", DataTypes.STRING());
        }
    }

    /** Converter for Kafka partition number. */
    public static class PartitionConverter extends KafkaMetadataConverter {
        private static final long serialVersionUID = 1L;

        public PartitionConverter() {
            super("partition", DataTypes.INT());
        }
    }

    /** Converter for Kafka message offset. */
    public static class OffsetConverter extends KafkaMetadataConverter {
        private static final long serialVersionUID = 1L;

        public OffsetConverter() {
            super("offset", DataTypes.BIGINT());
        }
    }

    /** Converter for Kafka message timestamp. */
    public static class TimestampConverter extends KafkaMetadataConverter {
        private static final long serialVersionUID = 1L;

        public TimestampConverter() {
            super("timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        }

        @Override
        public String read(CdcSourceRecord record) {
            Object timestamp = record.getMetadata("timestamp");
            if (timestamp instanceof Long) {
                return DateTimeUtils.formatTimestamp(
                        Timestamp.fromEpochMillis((Long) timestamp), TimeZone.getDefault(), 3);
            }
            return null;
        }
    }

    /** Converter for Kafka timestamp type. */
    public static class TimestampTypeConverter extends KafkaMetadataConverter {
        private static final long serialVersionUID = 1L;

        public TimestampTypeConverter() {
            super("timestamp_type", DataTypes.STRING());
        }
    }
}
