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
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.postgres.PostgresRecordParser;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecordUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertBinaryValues;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertComputedColumns;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.assertWriteAndReadBinaryValues;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.collectRecord;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumBinaryRecordTest.jsonValue;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for lossless PostgreSQL Debezium binary values. */
class PostgresBinaryRecordTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testBinaryValues() throws Exception {
        assertBinaryValues(PostgresBinaryRecordTest::parse);
    }

    @Test
    void testWriteAndReadBinaryValues() throws Exception {
        assertWriteAndReadBinaryValues(PostgresBinaryRecordTest::parse, tempDir);
    }

    @Test
    void testComputedColumnsKeepTextSemantics() throws Exception {
        assertComputedColumns(PostgresBinaryRecordTest::parse);
    }

    @Test
    void testSnapshotBinaryValues() throws Exception {
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80, 65};
        ObjectNode value = (ObjectNode) jsonValue(ByteBuffer.wrap(bytes));
        ((ObjectNode) value.get("payload")).put("op", "r");
        RichCdcMultiplexRecord rich = collectRecord(newParser(), value.toString());

        assertThat(toRow(rich).getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(toRow(rich).getBinary(0)).containsExactly(bytes);
    }

    @Test
    void testUpdateAndDeleteBinaryValues() throws Exception {
        PostgresRecordParser parser = newParser();
        byte[] before = {0, (byte) 0xff};
        byte[] after = {(byte) 0x80, 65};
        ObjectNode value = (ObjectNode) jsonValue(ByteBuffer.wrap(after));
        ObjectNode payload = (ObjectNode) value.get("payload");
        payload.put("op", "u");
        payload.set("before", jsonValue(ByteBuffer.wrap(before)).get("payload").get("after"));
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(new CdcSourceRecord(value.toString()), new ListCollector<>(records));

        assertThat(records).hasSize(2);
        // PostgreSQL DELETE records carry no schema; use the target table fields for both rows.
        List<DataField> fields = records.get(1).buildSchema().fields();
        GenericRow deleted = toRow(records.get(0), fields);
        GenericRow inserted = toRow(records.get(1), fields);
        assertThat(deleted.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(deleted.getBinary(0)).containsExactly(before);
        assertThat(inserted.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(inserted.getBinary(0)).containsExactly(after);

        payload.put("op", "d");
        payload.set("before", payload.get("after"));
        payload.putNull("after");
        deleted = toRow(collectRecord(parser, value.toString()), fields);
        assertThat(deleted.getRowKind()).isEqualTo(RowKind.DELETE);
        assertThat(deleted.getBinary(0)).containsExactly(after);
    }

    @Test
    void testBinaryMarkersDoNotLeakAcrossRecords() throws Exception {
        PostgresRecordParser parser = newParser();
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80, 65};
        ObjectNode value = (ObjectNode) jsonValue(ByteBuffer.wrap(bytes));
        RichCdcMultiplexRecord binary = collectRecord(parser, value.toString());
        ObjectNode payloadField =
                (ObjectNode) value.get("schema").get("fields").get(0).get("fields").get(0);
        payloadField.put("type", "string");
        RichCdcMultiplexRecord text = collectRecord(parser, value.toString());

        assertThat(text.toRichCdcRecord().toCdcRecord().base64EncodedFields()).isEmpty();
        assertThat(toRow(text).getString(0).toString()).isEqualTo("AP+AQQ==");
        assertThat(binary.toRichCdcRecord().toCdcRecord().base64EncodedFields())
                .containsExactly("payload");
        assertThat(toRow(binary).getBinary(0)).containsExactly(bytes);
    }

    @Test
    void testBinaryAlongsideLogicalTypes() throws Exception {
        byte[] bytes = {0, (byte) 0xff, (byte) 0x80, 65};
        ObjectNode value = (ObjectNode) jsonValue(ByteBuffer.wrap(bytes));
        ArrayNode fields = (ArrayNode) value.get("schema").get("fields").get(0).get("fields");
        ObjectNode bits =
                fields.addObject()
                        .put("field", "bits")
                        .put("type", "bytes")
                        .put("name", "io.debezium.data.Bits")
                        .put("optional", true);
        bits.putObject("parameters").put("length", "16");
        ObjectNode price =
                fields.addObject()
                        .put("field", "price")
                        .put("type", "bytes")
                        .put("name", "org.apache.kafka.connect.data.Decimal")
                        .put("optional", true);
        price.putObject("parameters").put("connect.decimal.precision", "10").put("scale", "2");
        ObjectNode after = (ObjectNode) value.get("payload").get("after");
        after.put("bits", "Af8=");
        after.put("price", "-123.45");
        RichCdcMultiplexRecord rich = collectRecord(newParser(), value.toString());

        assertThat(rich.toRichCdcRecord().toCdcRecord().base64EncodedFields())
                .containsExactly("payload");
        GenericRow row = toRow(rich);
        RowType rowType = rich.buildSchema().rowType();
        assertThat(row.getBinary(rowType.getFieldIndex("payload"))).containsExactly(bytes);
        assertThat(row.getBinary(rowType.getFieldIndex("bits")))
                .containsExactly((byte) 0xff, (byte) 1);
        assertThat(row.getDecimal(rowType.getFieldIndex("price"), 10, 2).toBigDecimal())
                .isEqualByComparingTo("-123.45");
    }

    @Test
    void testBinaryToExistingTextColumn() throws Exception {
        byte[] bytes = "plain text".getBytes(StandardCharsets.UTF_8);
        CdcRecord record =
                parse(ByteBuffer.wrap(bytes), Collections.emptyList())
                        .toRichCdcRecord()
                        .toCdcRecord();
        GenericRow row =
                CdcRecordUtils.toGenericRow(
                                record,
                                Arrays.asList(
                                        new DataField(0, "payload", DataTypes.STRING()),
                                        new DataField(1, "id", DataTypes.INT())),
                                true)
                        .get();
        assertThat(row.getString(0).toString()).isEqualTo("plain text");
    }

    private static GenericRow toRow(RichCdcMultiplexRecord rich) {
        return toRow(rich, rich.buildSchema().fields());
    }

    private static GenericRow toRow(RichCdcMultiplexRecord rich, List<DataField> fields) {
        return CdcRecordUtils.toGenericRow(rich.toRichCdcRecord().toCdcRecord(), fields, true)
                .get();
    }

    private static PostgresRecordParser newParser() {
        return new PostgresRecordParser(
                new Configuration(),
                Collections.emptyList(),
                TypeMapping.defaultMapping(),
                new CdcMetadataConverter[0]);
    }

    private static RichCdcMultiplexRecord parse(ByteBuffer bytes, List<ComputedColumn> computed)
            throws Exception {
        return collectRecord(
                new PostgresRecordParser(
                        new Configuration(),
                        computed,
                        TypeMapping.defaultMapping(),
                        new CdcMetadataConverter[0]),
                jsonValue(bytes).toString());
    }
}
