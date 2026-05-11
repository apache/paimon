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

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaMetadataConverter}. */
public class KafkaMetadataConverterTest {

    @Test
    public void testTopicConverter() {
        KafkaMetadataConverter.TopicConverter converter =
                new KafkaMetadataConverter.TopicConverter();

        // Test data type and column name
        assertThat(converter.dataType()).isEqualTo(DataTypes.STRING());
        assertThat(converter.columnName()).isEqualTo("topic");

        // Test reading from CdcSourceRecord
        CdcSourceRecord record = new CdcSourceRecord("test-topic", null, "value");
        assertThat(converter.read(record)).isEqualTo("test-topic");

        // Test with null topic
        CdcSourceRecord recordWithNullTopic = new CdcSourceRecord(null, null, "value");
        assertThat(converter.read(recordWithNullTopic)).isNull();

        // Test JsonNode method throws exception
        assertThatThrownBy(
                        () ->
                                converter.read(
                                        (org.apache.paimon.shade.jackson2.com.fasterxml.jackson
                                                        .databind.JsonNode)
                                                null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(
                        "Kafka metadata converters should be used with CdcSourceRecord");
    }

    @Test
    public void testPartitionConverter() {
        KafkaMetadataConverter.PartitionConverter converter =
                new KafkaMetadataConverter.PartitionConverter();

        // Test data type and column name
        assertThat(converter.dataType()).isEqualTo(DataTypes.INT());
        assertThat(converter.columnName()).isEqualTo("partition");

        // Test reading from CdcSourceRecord with metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("partition", 5);
        CdcSourceRecord record = new CdcSourceRecord("topic", null, "value", metadata);
        assertThat(converter.read(record)).isEqualTo("5");

        // Test with missing partition metadata
        CdcSourceRecord recordWithoutPartition = new CdcSourceRecord("topic", null, "value");
        assertThat(converter.read(recordWithoutPartition)).isNull();
    }

    @Test
    public void testOffsetConverter() {
        KafkaMetadataConverter.OffsetConverter converter =
                new KafkaMetadataConverter.OffsetConverter();

        // Test data type and column name
        assertThat(converter.dataType()).isEqualTo(DataTypes.BIGINT());
        assertThat(converter.columnName()).isEqualTo("offset");

        // Test reading from CdcSourceRecord with metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("offset", 12345L);
        CdcSourceRecord record = new CdcSourceRecord("topic", null, "value", metadata);
        assertThat(converter.read(record)).isEqualTo("12345");

        // Test with missing offset metadata
        CdcSourceRecord recordWithoutOffset = new CdcSourceRecord("topic", null, "value");
        assertThat(converter.read(recordWithoutOffset)).isNull();
    }

    @Test
    public void testTimestampConverter() {
        KafkaMetadataConverter.TimestampConverter converter =
                new KafkaMetadataConverter.TimestampConverter();

        // Test data type and column name
        assertThat(converter.dataType()).isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        assertThat(converter.columnName()).isEqualTo("timestamp");

        // Test reading from CdcSourceRecord with metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", 1640995200000L); // 2022-01-01 00:00:00 UTC
        CdcSourceRecord record = new CdcSourceRecord("topic", null, "value", metadata);
        String result = converter.read(record);
        assertThat(result).isNotNull();
        // Result depends on system timezone, just verify it's a valid timestamp string
        assertThat(result).matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}");

        // Test with missing timestamp metadata
        CdcSourceRecord recordWithoutTimestamp = new CdcSourceRecord("topic", null, "value");
        assertThat(converter.read(recordWithoutTimestamp)).isNull();

        // Test with non-Long timestamp
        Map<String, Object> invalidMetadata = new HashMap<>();
        invalidMetadata.put("timestamp", "not-a-long");
        CdcSourceRecord recordWithInvalidTimestamp =
                new CdcSourceRecord("topic", null, "value", invalidMetadata);
        assertThat(converter.read(recordWithInvalidTimestamp)).isNull();
    }

    @Test
    public void testTimestampTypeConverter() {
        KafkaMetadataConverter.TimestampTypeConverter converter =
                new KafkaMetadataConverter.TimestampTypeConverter();

        // Test data type and column name
        assertThat(converter.dataType()).isEqualTo(DataTypes.STRING());
        assertThat(converter.columnName()).isEqualTo("timestamp_type");

        // Test reading from CdcSourceRecord with metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp_type", "CreateTime");
        CdcSourceRecord record = new CdcSourceRecord("topic", null, "value", metadata);
        assertThat(converter.read(record)).isEqualTo("CreateTime");

        // Test with LogAppendTime
        metadata.put("timestamp_type", "LogAppendTime");
        CdcSourceRecord recordLogAppend = new CdcSourceRecord("topic", null, "value", metadata);
        assertThat(converter.read(recordLogAppend)).isEqualTo("LogAppendTime");

        // Test with NoTimestampType
        metadata.put("timestamp_type", "NoTimestampType");
        CdcSourceRecord recordNoTimestamp = new CdcSourceRecord("topic", null, "value", metadata);
        assertThat(converter.read(recordNoTimestamp)).isEqualTo("NoTimestampType");

        // Test with missing timestamp_type metadata
        CdcSourceRecord recordWithoutTimestampType = new CdcSourceRecord("topic", null, "value");
        assertThat(converter.read(recordWithoutTimestampType)).isNull();
    }

    @Test
    public void testAllConvertersWithCompleteMetadata() {
        // Create a CdcSourceRecord with all Kafka metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("topic", "my-topic");
        metadata.put("partition", 3);
        metadata.put("offset", 9876L);
        metadata.put("timestamp", 1640995200000L);
        metadata.put("timestamp_type", "CreateTime");

        CdcSourceRecord record = new CdcSourceRecord("my-topic", "key", "value", metadata);

        // Test all converters
        KafkaMetadataConverter.TopicConverter topicConverter =
                new KafkaMetadataConverter.TopicConverter();
        KafkaMetadataConverter.PartitionConverter partitionConverter =
                new KafkaMetadataConverter.PartitionConverter();
        KafkaMetadataConverter.OffsetConverter offsetConverter =
                new KafkaMetadataConverter.OffsetConverter();
        KafkaMetadataConverter.TimestampConverter timestampConverter =
                new KafkaMetadataConverter.TimestampConverter();
        KafkaMetadataConverter.TimestampTypeConverter timestampTypeConverter =
                new KafkaMetadataConverter.TimestampTypeConverter();

        assertThat(topicConverter.read(record)).isEqualTo("my-topic");
        assertThat(partitionConverter.read(record)).isEqualTo("3");
        assertThat(offsetConverter.read(record)).isEqualTo("9876");
        assertThat(timestampConverter.read(record)).isNotNull();
        assertThat(timestampTypeConverter.read(record)).isEqualTo("CreateTime");
    }
}
