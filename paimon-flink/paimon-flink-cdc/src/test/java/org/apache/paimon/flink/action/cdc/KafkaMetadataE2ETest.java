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

import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumAvroRecordParser;
import org.apache.paimon.flink.action.cdc.kafka.KafkaMetadataConverter;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataTypes;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end unit test for Kafka metadata column support.
 *
 * <p>This test validates the complete flow from Kafka ConsumerRecord through deserialization,
 * metadata extraction, and final Paimon row creation with metadata columns.
 */
public class KafkaMetadataE2ETest {

    private static final String TEST_TOPIC = "test-topic";
    private static final int TEST_PARTITION = 5;
    private static final long TEST_OFFSET = 12345L;
    private static final long TEST_TIMESTAMP = 1640995200000L;
    private static final String TEST_TIMESTAMP_TYPE = "CreateTime";

    @Test
    public void testKafkaMetadataEndToEnd() throws Exception {
        Map<String, Object> kafkaMetadata = createKafkaMetadata();
        GenericRecord valueRecord = createDebeziumAvroRecord();
        CdcSourceRecord cdcSourceRecord =
                new CdcSourceRecord(TEST_TOPIC, null, valueRecord, kafkaMetadata);

        assertThat(cdcSourceRecord.getMetadata()).isNotNull();
        assertThat(cdcSourceRecord.getMetadata()).hasSize(5);
        assertThat(cdcSourceRecord.getMetadata("topic")).isEqualTo(TEST_TOPIC);
        assertThat(cdcSourceRecord.getMetadata("partition")).isEqualTo(TEST_PARTITION);
        assertThat(cdcSourceRecord.getMetadata("offset")).isEqualTo(TEST_OFFSET);
        assertThat(cdcSourceRecord.getMetadata("timestamp")).isEqualTo(TEST_TIMESTAMP);
        assertThat(cdcSourceRecord.getMetadata("timestamp_type")).isEqualTo(TEST_TIMESTAMP_TYPE);

        CdcMetadataConverter[] metadataConverters = createKafkaMetadataConverters();
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(
                        TypeMapping.defaultMapping(), Collections.emptyList(), metadataConverters);

        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                cdcSourceRecord,
                new org.apache.flink.util.Collector<RichCdcMultiplexRecord>() {
                    @Override
                    public void collect(RichCdcMultiplexRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(records).hasSize(1);
        RichCdcMultiplexRecord richRecord = records.get(0);

        org.apache.paimon.flink.sink.cdc.CdcSchema cdcSchema = richRecord.cdcSchema();
        assertThat(cdcSchema.fields()).isNotEmpty();

        assertThat(cdcSchema.fields().stream().anyMatch(f -> f.name().equals("topic"))).isTrue();
        assertThat(cdcSchema.fields().stream().anyMatch(f -> f.name().equals("partition")))
                .isTrue();
        assertThat(cdcSchema.fields().stream().anyMatch(f -> f.name().equals("offset"))).isTrue();
        assertThat(cdcSchema.fields().stream().anyMatch(f -> f.name().equals("timestamp")))
                .isTrue();
        assertThat(cdcSchema.fields().stream().anyMatch(f -> f.name().equals("timestamp_type")))
                .isTrue();

        Map<String, String> rowData = richRecord.toRichCdcRecord().toCdcRecord().data();
        assertThat(rowData).containsKey("topic");
        assertThat(rowData.get("topic")).isEqualTo(TEST_TOPIC);
        assertThat(rowData).containsKey("partition");
        assertThat(rowData.get("partition")).isEqualTo(String.valueOf(TEST_PARTITION));
        assertThat(rowData).containsKey("offset");
        assertThat(rowData.get("offset")).isEqualTo(String.valueOf(TEST_OFFSET));
        assertThat(rowData).containsKey("timestamp");
        assertThat(rowData.get("timestamp")).isNotNull();
        assertThat(rowData).containsKey("timestamp_type");
        assertThat(rowData.get("timestamp_type")).isEqualTo(TEST_TIMESTAMP_TYPE);

        assertThat(rowData).containsKey("id");
        assertThat(rowData.get("id")).isEqualTo("1");
        assertThat(rowData).containsKey("name");
        assertThat(rowData.get("name")).isEqualTo("test_user");
    }

    @Test
    public void testMetadataConverterLookup() {
        assertThat(CdcMetadataProcessor.converter(SyncJobHandler.SourceType.KAFKA, "topic"))
                .isNotNull()
                .isInstanceOf(KafkaMetadataConverter.TopicConverter.class);

        assertThat(CdcMetadataProcessor.converter(SyncJobHandler.SourceType.KAFKA, "partition"))
                .isNotNull()
                .isInstanceOf(KafkaMetadataConverter.PartitionConverter.class);

        assertThat(CdcMetadataProcessor.converter(SyncJobHandler.SourceType.KAFKA, "offset"))
                .isNotNull()
                .isInstanceOf(KafkaMetadataConverter.OffsetConverter.class);

        assertThat(CdcMetadataProcessor.converter(SyncJobHandler.SourceType.KAFKA, "timestamp"))
                .isNotNull()
                .isInstanceOf(KafkaMetadataConverter.TimestampConverter.class);

        assertThat(
                        CdcMetadataProcessor.converter(
                                SyncJobHandler.SourceType.KAFKA, "timestamp_type"))
                .isNotNull()
                .isInstanceOf(KafkaMetadataConverter.TimestampTypeConverter.class);

        assertThatThrownBy(
                        () ->
                                CdcMetadataProcessor.converter(
                                        SyncJobHandler.SourceType.KAFKA, "invalid_column"))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testPartialMetadata() throws Exception {
        Map<String, Object> partialMetadata = new HashMap<>();
        partialMetadata.put("topic", TEST_TOPIC);
        partialMetadata.put("partition", TEST_PARTITION);

        GenericRecord valueRecord = createDebeziumAvroRecord();
        CdcSourceRecord cdcSourceRecord =
                new CdcSourceRecord(TEST_TOPIC, null, valueRecord, partialMetadata);

        CdcMetadataConverter[] metadataConverters = createKafkaMetadataConverters();
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(
                        TypeMapping.defaultMapping(), Collections.emptyList(), metadataConverters);

        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                cdcSourceRecord,
                new org.apache.flink.util.Collector<RichCdcMultiplexRecord>() {
                    @Override
                    public void collect(RichCdcMultiplexRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(records).hasSize(1);
        RichCdcMultiplexRecord richRecord = records.get(0);

        Map<String, String> rowData = richRecord.toRichCdcRecord().toCdcRecord().data();
        assertThat(rowData.get("topic")).isEqualTo(TEST_TOPIC);
        assertThat(rowData.get("partition")).isEqualTo(String.valueOf(TEST_PARTITION));
        assertThat(rowData.get("offset")).isNull();
        assertThat(rowData.get("timestamp")).isNull();
        assertThat(rowData.get("timestamp_type")).isNull();
    }

    @Test
    public void testMetadataWithoutConverters() throws Exception {
        Map<String, Object> kafkaMetadata = createKafkaMetadata();
        GenericRecord valueRecord = createDebeziumAvroRecord();
        CdcSourceRecord cdcSourceRecord =
                new CdcSourceRecord(TEST_TOPIC, null, valueRecord, kafkaMetadata);

        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());

        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                cdcSourceRecord,
                new org.apache.flink.util.Collector<RichCdcMultiplexRecord>() {
                    @Override
                    public void collect(RichCdcMultiplexRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(records).hasSize(1);
        RichCdcMultiplexRecord richRecord = records.get(0);

        Map<String, String> rowData = richRecord.toRichCdcRecord().toCdcRecord().data();
        assertThat(rowData).doesNotContainKey("topic");
        assertThat(rowData).doesNotContainKey("partition");
        assertThat(rowData).doesNotContainKey("offset");

        assertThat(rowData).containsKey("id");
        assertThat(rowData).containsKey("name");
    }

    @Test
    public void testAllMetadataConvertersDataTypes() {
        KafkaMetadataConverter.TopicConverter topicConverter =
                new KafkaMetadataConverter.TopicConverter();
        assertThat(topicConverter.dataType()).isEqualTo(DataTypes.STRING());
        assertThat(topicConverter.columnName()).isEqualTo("topic");

        KafkaMetadataConverter.PartitionConverter partitionConverter =
                new KafkaMetadataConverter.PartitionConverter();
        assertThat(partitionConverter.dataType()).isEqualTo(DataTypes.INT());
        assertThat(partitionConverter.columnName()).isEqualTo("partition");

        KafkaMetadataConverter.OffsetConverter offsetConverter =
                new KafkaMetadataConverter.OffsetConverter();
        assertThat(offsetConverter.dataType()).isEqualTo(DataTypes.BIGINT());
        assertThat(offsetConverter.columnName()).isEqualTo("offset");

        KafkaMetadataConverter.TimestampConverter timestampConverter =
                new KafkaMetadataConverter.TimestampConverter();
        assertThat(timestampConverter.dataType())
                .isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
        assertThat(timestampConverter.columnName()).isEqualTo("timestamp");

        KafkaMetadataConverter.TimestampTypeConverter timestampTypeConverter =
                new KafkaMetadataConverter.TimestampTypeConverter();
        assertThat(timestampTypeConverter.dataType()).isEqualTo(DataTypes.STRING());
        assertThat(timestampTypeConverter.columnName()).isEqualTo("timestamp_type");
    }

    private Map<String, Object> createKafkaMetadata() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("topic", TEST_TOPIC);
        metadata.put("partition", TEST_PARTITION);
        metadata.put("offset", TEST_OFFSET);
        metadata.put("timestamp", TEST_TIMESTAMP);
        metadata.put("timestamp_type", TEST_TIMESTAMP_TYPE);
        return metadata;
    }

    private CdcMetadataConverter[] createKafkaMetadataConverters() {
        return new CdcMetadataConverter[] {
            new KafkaMetadataConverter.TopicConverter(),
            new KafkaMetadataConverter.PartitionConverter(),
            new KafkaMetadataConverter.OffsetConverter(),
            new KafkaMetadataConverter.TimestampConverter(),
            new KafkaMetadataConverter.TimestampTypeConverter()
        };
    }

    private GenericRecord createDebeziumAvroRecord() {
        Schema afterSchema =
                SchemaBuilder.record("after")
                        .fields()
                        .name("id")
                        .type()
                        .intType()
                        .noDefault()
                        .name("name")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();

        Schema sourceSchema =
                SchemaBuilder.record("source")
                        .fields()
                        .name("db")
                        .type()
                        .stringType()
                        .noDefault()
                        .name("table")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();

        Schema envelopeSchema =
                SchemaBuilder.record("envelope")
                        .fields()
                        .name("before")
                        .type()
                        .nullable()
                        .record("before_record")
                        .fields()
                        .name("id")
                        .type()
                        .intType()
                        .noDefault()
                        .name("name")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord()
                        .noDefault()
                        .name("after")
                        .type()
                        .nullable()
                        .record("after_record")
                        .fields()
                        .name("id")
                        .type()
                        .intType()
                        .noDefault()
                        .name("name")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord()
                        .noDefault()
                        .name("source")
                        .type(sourceSchema)
                        .noDefault()
                        .name("op")
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord();

        GenericRecord afterRecord = new GenericData.Record(afterSchema);
        afterRecord.put("id", 1);
        afterRecord.put("name", "test_user");

        GenericRecord sourceRecord = new GenericData.Record(sourceSchema);
        sourceRecord.put("db", "test_db");
        sourceRecord.put("table", "test_table");

        GenericRecord envelopeRecord = new GenericData.Record(envelopeSchema);
        envelopeRecord.put("before", null);
        envelopeRecord.put("after", afterRecord);
        envelopeRecord.put("source", sourceRecord);
        envelopeRecord.put("op", "c");

        return envelopeRecord;
    }
}
