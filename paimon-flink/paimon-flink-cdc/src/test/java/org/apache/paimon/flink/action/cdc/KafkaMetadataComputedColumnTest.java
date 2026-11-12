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

import org.apache.paimon.flink.action.cdc.format.AbstractRecordParser;
import org.apache.paimon.flink.action.cdc.format.aliyun.AliyunRecordParser;
import org.apache.paimon.flink.action.cdc.format.canal.CanalRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumAvroRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBsonRecordParser;
import org.apache.paimon.flink.action.cdc.format.debezium.DebeziumJsonRecordParser;
import org.apache.paimon.flink.action.cdc.format.json.JsonRecordParser;
import org.apache.paimon.flink.action.cdc.kafka.KafkaMetadataConverter;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.assertj.core.api.Assertions.assertThat;

/** Kafka metadata must be available when a record's computed columns are evaluated. */
class KafkaMetadataComputedColumnTest {

    @ParameterizedTest
    @MethodSource("formatsAndPrefixes")
    void testComputedColumnsReferenceMetadata(String format, String prefix) throws Exception {
        CdcMetadataConverter timestamp =
                new PrefixedMetadataConverter(
                        new KafkaMetadataConverter.TimestampConverter(), prefix);
        CdcMetadataConverter topic =
                new PrefixedMetadataConverter(new KafkaMetadataConverter.TopicConverter(), prefix);
        List<ComputedColumn> computedColumns =
                buildComputedColumns(
                        Arrays.asList(
                                "yr=substring(dt,0,4)",
                                "dt=date_format(" + timestamp.columnName() + ",yyyy-MM-dd)",
                                "source=substring(" + topic.columnName() + ",0)"),
                        Arrays.asList(
                                new DataField(0, timestamp.columnName(), timestamp.dataType()),
                                new DataField(1, topic.columnName(), topic.dataType())));
        AbstractRecordParser parser = createParser(format, computedColumns);
        parser.withMetadataConverters(new CdcMetadataConverter[] {timestamp, topic});
        CdcSourceRecord input =
                new CdcSourceRecord(
                        "test-topic",
                        null,
                        createValue(format),
                        Collections.singletonMap("timestamp", 1700000000000L));
        String timestampValue = timestamp.read(input);
        String date = timestampValue.substring(0, 10);

        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(input, new ListCollector<>(records));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).cdcSchema().fields())
                .extracting(DataField::name)
                .containsExactly(
                        format.equals("debezium-bson") ? "_id" : "id",
                        "dt",
                        "source",
                        "yr",
                        timestamp.columnName(),
                        topic.columnName());
        assertThat(records.get(0).toRichCdcRecord().toCdcRecord().data())
                .containsEntry(timestamp.columnName(), timestampValue)
                .containsEntry("dt", date)
                .containsEntry("yr", date.substring(0, 4))
                .containsEntry("source", "test-topic");

        // Reuse the parser with a missing timestamp to check null propagation and stale values.
        records.clear();
        parser.flatMap(
                new CdcSourceRecord("test-topic", null, createValue(format)),
                new ListCollector<>(records));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).toRichCdcRecord().toCdcRecord().data())
                .containsEntry("dt", null)
                .containsEntry("yr", null)
                .containsEntry("source", "test-topic");
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void testAvroBinarySourceFieldWithMetadataName(boolean caseSensitive, boolean nullValue) {
        CdcMetadataConverter topic = new KafkaMetadataConverter.TopicConverter();
        String sourceName = caseSensitive ? "topic" : "TOPIC";
        List<ComputedColumn> columns =
                buildComputedColumns(
                        Collections.singletonList("s=substring(topic,0)"),
                        Collections.singletonList(new DataField(0, sourceName, DataTypes.BYTES())),
                        new CdcMetadataConverter[] {topic},
                        caseSensitive);
        Schema payloadSchema =
                SchemaBuilder.record("row")
                        .fields()
                        .requiredInt("id")
                        .optionalBytes(sourceName)
                        .endRecord();
        GenericRecord value = createAvroValue(payloadSchema);
        ((GenericRecord) value.get("after"))
                .put(
                        sourceName,
                        nullValue
                                ? null
                                : ByteBuffer.wrap("source-value".getBytes(StandardCharsets.UTF_8)));
        AbstractRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), columns)
                        .withMetadataConverters(new CdcMetadataConverter[] {topic});
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                new CdcSourceRecord("wire-topic", null, value), new ListCollector<>(records));

        assertThat(records).hasSize(1);
        assertThat(records.get(0).toRichCdcRecord().toCdcRecord().data())
                .containsEntry("s", nullValue ? null : "source-value");
    }

    private static Stream<Arguments> formatsAndPrefixes() {
        return Stream.of(
                        "avro",
                        "json",
                        "debezium-json",
                        "debezium-bson",
                        "canal-json",
                        "aliyun-json")
                .flatMap(
                        format ->
                                Stream.of("", "__kafka_", "__custom_")
                                        .map(prefix -> Arguments.of(format, prefix)));
    }

    private static AbstractRecordParser createParser(String format, List<ComputedColumn> columns) {
        TypeMapping mapping = TypeMapping.defaultMapping();
        switch (format) {
            case "avro":
                return new DebeziumAvroRecordParser(mapping, columns);
            case "json":
                return new JsonRecordParser(mapping, columns);
            case "debezium-json":
                return new DebeziumJsonRecordParser(mapping, columns);
            case "debezium-bson":
                return new DebeziumBsonRecordParser(mapping, columns);
            case "canal-json":
                return new CanalRecordParser(mapping, columns);
            case "aliyun-json":
                return new AliyunRecordParser(mapping, columns);
            default:
                throw new IllegalArgumentException(format);
        }
    }

    private static Object createValue(String format) {
        switch (format) {
            case "avro":
                return createAvroValue();
            case "json":
                return json("{\"id\":1}");
            case "debezium-json":
                return json("{\"op\":\"c\",\"after\":{\"id\":1}}");
            case "debezium-bson":
                return json("{\"op\":\"c\",\"after\":\"{\\\"_id\\\":1}\"}");
            case "canal-json":
                return json("{\"type\":\"INSERT\",\"data\":[{\"id\":1}]}");
            case "aliyun-json":
                return json(
                        "{\"schema\":{\"primaryKey\":[\"id\"],\"dataColumn\":[{\"name\":\"id\",\"type\":\"INT\"}]},"
                                + "\"payload\":{\"op\":\"INSERT\",\"after\":{\"dataColumn\":{\"id\":1}}}}");
            default:
                throw new IllegalArgumentException(format);
        }
    }

    private static JsonNode json(String value) {
        return JsonSerdeUtil.fromJson(value, JsonNode.class);
    }

    private static GenericRecord createAvroValue() {
        Schema payloadSchema = SchemaBuilder.record("row").fields().requiredInt("id").endRecord();
        return createAvroValue(payloadSchema);
    }

    private static GenericRecord createAvroValue(Schema payloadSchema) {
        Schema sourceSchema =
                SchemaBuilder.record("source")
                        .fields()
                        .requiredString("db")
                        .requiredString("table")
                        .endRecord();
        Schema envelopeSchema =
                SchemaBuilder.record("envelope")
                        .fields()
                        .requiredString("op")
                        .name("after")
                        .type(payloadSchema)
                        .noDefault()
                        .name("source")
                        .type(sourceSchema)
                        .noDefault()
                        .endRecord();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        payload.put("id", 1);
        GenericRecord source = new GenericData.Record(sourceSchema);
        source.put("db", "test_db");
        source.put("table", "test_table");
        GenericRecord envelope = new GenericData.Record(envelopeSchema);
        envelope.put("op", "c");
        envelope.put("after", payload);
        envelope.put("source", source);
        return envelope;
    }
}
