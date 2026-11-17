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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecordUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertBinaryValues;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertComputedColumns;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertWriteAndReadBinaryValues;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.collectRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for lossless Debezium Avro binary values and ByteBuffer ranges. */
class DebeziumAvroBinaryRecordTest {
    @TempDir java.nio.file.Path tempDir;

    @Test
    void testBinaryValues() throws Exception {
        assertBinaryValues(DebeziumAvroBinaryRecordTest::parse);
    }

    @Test
    void testWriteAndReadBinaryValues() throws Exception {
        assertWriteAndReadBinaryValues(DebeziumAvroBinaryRecordTest::parse, tempDir);
    }

    @Test
    void testComputedColumnsKeepTextSemantics() throws Exception {
        assertComputedColumns(DebeziumAvroBinaryRecordTest::parse);
    }

    @ParameterizedTest
    @ValueSource(strings = {"49", "-1.23", "0.00", "12.34"})
    void testBytesDecimalLogicalType(String value) throws Exception {
        BigDecimal expected = new BigDecimal(value);
        Schema decimalSchema =
                LogicalTypes.decimal(4, expected.scale())
                        .addToSchema(Schema.create(Schema.Type.BYTES));
        Schema optionalDecimal = Schema.createUnion(Schema.create(Schema.Type.NULL), decimalSchema);
        byte[] bytes = expected.unscaledValue().toByteArray();
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length + 2);
        buffer.put((byte) 9).put(bytes).put((byte) 8);
        buffer.position(1);
        buffer.limit(1 + bytes.length);
        buffer = buffer.asReadOnlyBuffer();
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        RichCdcMultiplexRecord rich = collectRecord(parser, avroValue(buffer, optionalDecimal));

        assertThat(rich.toRichCdcRecord().toCdcRecord().base64EncodedFields()).isEmpty();
        assertThat(rich.toRichCdcRecord().toCdcRecord().data()).containsEntry("payload", value);
        assertThat(toRow(rich).getDecimal(0, 4, expected.scale()).toBigDecimal())
                .isEqualByComparingTo(expected);
        assertThat(buffer.position()).isEqualTo(1);
        assertThat(buffer.limit()).isEqualTo(1 + bytes.length);
        assertThat(toRow(collectRecord(parser, avroValue(null, optionalDecimal))).isNullAt(0))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"49", "-1.23", "0.00", "12.34"})
    void testFixedDecimalLogicalType(String value) throws Exception {
        BigDecimal expected = new BigDecimal(value);
        Schema decimalSchema =
                LogicalTypes.decimal(4, expected.scale())
                        .addToSchema(Schema.createFixed("DecimalValue", null, null, 2));
        Schema optionalDecimal = Schema.createUnion(Schema.create(Schema.Type.NULL), decimalSchema);
        byte[] bytes =
                ByteBuffer.allocate(2).putShort(expected.unscaledValue().shortValueExact()).array();
        GenericData.Fixed fixed = new GenericData.Fixed(decimalSchema, bytes);
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        RichCdcMultiplexRecord rich = collectRecord(parser, avroValue(fixed, optionalDecimal));

        assertThat(rich.toRichCdcRecord().toCdcRecord().base64EncodedFields()).isEmpty();
        assertThat(rich.toRichCdcRecord().toCdcRecord().data()).containsEntry("payload", value);
        assertThat(toRow(rich).getDecimal(0, 4, expected.scale()).toBigDecimal())
                .isEqualByComparingTo(expected);
        assertThat(toRow(collectRecord(parser, avroValue(null, optionalDecimal))).isNullAt(0))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"heap", "slice", "direct", "read-only"})
    void testAvroBufferRange(String kind) throws Exception {
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80, 65};
        ByteBuffer buffer =
                kind.equals("direct") ? ByteBuffer.allocateDirect(8) : ByteBuffer.allocate(8);
        buffer.put((byte) 9).put(bytes).put((byte) 8);
        buffer.position(1);
        buffer.limit(5);
        if (kind.equals("slice")) {
            buffer = buffer.slice();
        } else if (kind.equals("read-only")) {
            buffer = buffer.asReadOnlyBuffer();
        }
        int position = buffer.position();
        int limit = buffer.limit();
        RichCdcMultiplexRecord rich = parse(buffer, Collections.emptyList());
        GenericRow row =
                CdcRecordUtils.toGenericRow(
                                rich.toRichCdcRecord().toCdcRecord(),
                                rich.buildSchema().fields(),
                                true)
                        .get();
        assertThat(row.getBinary(0)).containsExactly(bytes);
        assertThat(buffer.position()).isEqualTo(position);
        assertThat(buffer.limit()).isEqualTo(limit);
    }

    @Test
    void testBinaryMarkersDoNotLeakAcrossRecords() throws Exception {
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80, 65};
        RichCdcMultiplexRecord binary = collectRecord(parser, avroValue(ByteBuffer.wrap(bytes)));
        RichCdcMultiplexRecord text =
                collectRecord(parser, avroValue("AP+AQQ==", Schema.create(Schema.Type.STRING)));

        assertThat(text.toRichCdcRecord().toCdcRecord().base64EncodedFields()).isEmpty();
        assertThat(toRow(text).getString(0).toString()).isEqualTo("AP+AQQ==");
        assertThat(binary.toRichCdcRecord().toCdcRecord().base64EncodedFields())
                .containsExactly("payload");
        assertThat(toRow(binary).getBinary(0)).containsExactly(bytes);
    }

    @Test
    void testUpdateAndDeleteBinaryValues() throws Exception {
        DebeziumAvroRecordParser parser =
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        byte[] before = {0, (byte) 0xff};
        byte[] after = {(byte) 0x80, 65};
        GenericRecord value = avroValue(ByteBuffer.wrap(after));
        value.put("op", "u");
        value.put("before", avroValue(ByteBuffer.wrap(before)).get("after"));
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                new CdcSourceRecord("test", null, value),
                new Collector<RichCdcMultiplexRecord>() {
                    @Override
                    public void collect(RichCdcMultiplexRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });
        assertThat(records).hasSize(2);
        assertThat(toRow(records.get(0)).getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(toRow(records.get(0)).getBinary(0)).containsExactly(before);
        assertThat(toRow(records.get(1)).getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(toRow(records.get(1)).getBinary(0)).containsExactly(after);

        value.put("op", "d");
        value.put("before", value.get("after"));
        value.put("after", null);
        GenericRow deleted = toRow(collectRecord(parser, value));
        assertThat(deleted.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(deleted.getBinary(0)).containsExactly(after);
    }

    private static GenericRow toRow(RichCdcMultiplexRecord rich) {
        return CdcRecordUtils.toGenericRow(
                        rich.toRichCdcRecord().toCdcRecord(), rich.buildSchema().fields(), true)
                .get();
    }

    private static RichCdcMultiplexRecord parse(ByteBuffer bytes, List<ComputedColumn> computed)
            throws Exception {
        return collectRecord(
                new DebeziumAvroRecordParser(TypeMapping.defaultMapping(), computed),
                avroValue(bytes));
    }

    private static GenericRecord avroValue(ByteBuffer bytes) {
        return avroValue(
                bytes,
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.BYTES)));
    }

    private static GenericRecord avroValue(Object payload, Schema payloadSchema) {
        Schema row =
                SchemaBuilder.record("BinaryRow")
                        .fields()
                        .name("payload")
                        .type(payloadSchema)
                        .noDefault()
                        .requiredInt("id")
                        .endRecord();
        Schema source =
                SchemaBuilder.record("Source")
                        .fields()
                        .requiredString("db")
                        .requiredString("table")
                        .endRecord();
        Schema optionalRow = Schema.createUnion(Schema.create(Schema.Type.NULL), row);
        Schema envelope =
                SchemaBuilder.record("Envelope")
                        .fields()
                        .requiredString("op")
                        .name("before")
                        .type(optionalRow)
                        .noDefault()
                        .name("after")
                        .type(optionalRow)
                        .noDefault()
                        .name("source")
                        .type(source)
                        .noDefault()
                        .endRecord();
        GenericRecord data = new GenericData.Record(row);
        data.put("payload", payload);
        data.put("id", 1);
        GenericRecord sourceRecord = new GenericData.Record(source);
        sourceRecord.put("db", "test");
        sourceRecord.put("table", "binary_table");
        GenericRecord value = new GenericData.Record(envelope);
        value.put("op", "c");
        value.put("before", null);
        value.put("after", data);
        value.put("source", sourceRecord);
        return value;
    }
}
