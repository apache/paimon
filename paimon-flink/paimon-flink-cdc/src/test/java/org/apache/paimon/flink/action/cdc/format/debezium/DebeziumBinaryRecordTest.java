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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.Expression;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.MySqlRecordParser;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcRecordUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Binary values must survive Debezium parsing, key projection and table writes. */
class DebeziumBinaryRecordTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"json", "mysql"})
    void testBinaryValues(String format) throws Exception {
        assertBinaryValues((bytes, computed) -> parse(format, bytes, computed));
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "mysql"})
    void testWriteAndReadBinaryValues(String format) throws Exception {
        assertWriteAndReadBinaryValues(
                (bytes, computed) -> parse(format, bytes, computed), tempDir.resolve(format));
    }

    @ParameterizedTest
    @ValueSource(strings = {"json", "mysql"})
    void testComputedColumnsKeepTextSemantics(String format) throws Exception {
        assertComputedColumns((bytes, computed) -> parse(format, bytes, computed));
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
                        .put("name", "io.debezium.data.Bits");
        bits.putObject("parameters").put("length", "16");
        ObjectNode price =
                fields.addObject()
                        .put("field", "price")
                        .put("type", "bytes")
                        .put("name", "org.apache.kafka.connect.data.Decimal");
        price.putObject("parameters").put("connect.decimal.precision", "10").put("scale", "2");
        fields.addObject()
                .put("field", "location")
                .put("type", "struct")
                .put("name", "io.debezium.data.geometry.Point");
        ObjectNode after = (ObjectNode) value.get("payload").get("after");
        after.put("bits", "Af8=");
        after.put("price", "123.45");
        byte[] wkb =
                ByteBuffer.allocate(21)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .put((byte) 1)
                        .putInt(1)
                        .putDouble(1)
                        .putDouble(2)
                        .array();
        after.putObject("location")
                .put("wkb", Base64.getEncoder().encodeToString(wkb))
                .put("srid", 0);
        DebeziumJsonRecordParser parser =
                new DebeziumJsonRecordParser(TypeMapping.defaultMapping(), Collections.emptyList());
        RichCdcMultiplexRecord rich = collectRecord(parser, value);
        CdcRecord record = rich.toRichCdcRecord().toCdcRecord();
        assertThat(record.base64EncodedFields()).containsExactly("payload");
        GenericRow row =
                CdcRecordUtils.toGenericRow(record, rich.buildSchema().fields(), true).get();
        RowType rowType = rich.buildSchema().rowType();
        assertThat(row.getBinary(rowType.getFieldIndex("payload"))).containsExactly(bytes);
        assertThat(row.getBinary(rowType.getFieldIndex("bits")))
                .containsExactly((byte) 0xff, (byte) 1);
        assertThat(row.getDecimal(rowType.getFieldIndex("price"), 10, 2).toBigDecimal())
                .isEqualByComparingTo("123.45");
        JsonNode geometry =
                MAPPER.readTree(row.getString(rowType.getFieldIndex("location")).toString());
        assertThat(geometry.get("type").asText()).isEqualTo("Point");
        assertThat(geometry.get("coordinates").get(0).asDouble()).isEqualTo(1);
        assertThat(geometry.get("coordinates").get(1).asDouble()).isEqualTo(2);
    }

    static void assertBinaryValues(RecordParser parser) throws Exception {
        for (byte[] bytes : binaryValues()) {
            RichCdcMultiplexRecord rich =
                    parser.parse(
                            bytes == null ? null : ByteBuffer.wrap(bytes), Collections.emptyList());
            CdcRecord record = rich.toRichCdcRecord().toCdcRecord();
            List<DataField> fields =
                    Arrays.asList(
                            new DataField(0, "payload", DataTypes.BYTES()),
                            new DataField(1, "id", DataTypes.INT()));
            GenericRow row = CdcRecordUtils.toGenericRow(record, fields, true).get();
            GenericRow key = CdcRecordUtils.projectAsInsert(record, fields);
            if (bytes == null) {
                assertThat(row.isNullAt(0)).isTrue();
                assertThat(key.isNullAt(0)).isTrue();
            } else {
                assertThat(row.getBinary(0)).containsExactly(bytes);
                assertThat(key.getBinary(0)).containsExactly(bytes);
            }
        }
    }

    static void assertWriteAndReadBinaryValues(RecordParser parser, java.nio.file.Path tableDir)
            throws Exception {
        Path path = new Path(tableDir.toUri());
        Schema schema =
                Schema.newBuilder()
                        .column("payload", DataTypes.BYTES())
                        .column("id", DataTypes.INT())
                        .option("bucket", "-1")
                        .build();
        LocalFileIO fileIO = LocalFileIO.create();
        new FileSystemSchemaManager(fileIO, path).createTable(schema);
        FileStoreTable table = FileStoreTableFactory.create(fileIO, path);
        List<byte[]> values = binaryValues();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (byte[] bytes : values) {
                CdcRecord record =
                        parser.parse(
                                        bytes == null ? null : ByteBuffer.wrap(bytes),
                                        Collections.emptyList())
                                .toRichCdcRecord()
                                .toCdcRecord();
                write.write(CdcRecordUtils.toGenericRow(record, schema.fields(), true).get());
            }
            commit.commit(write.prepareCommit());
        }
        List<String> expected = new ArrayList<>();
        for (byte[] bytes : values) {
            expected.add(bytes == null ? null : Base64.getEncoder().encodeToString(bytes));
        }
        List<String> actual = new ArrayList<>();
        ReadBuilder read = table.newReadBuilder();
        try (CloseableIterator<InternalRow> rows =
                read.newRead().createReader(read.newScan().plan()).toCloseableIterator()) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                actual.add(
                        row.isNullAt(0)
                                ? null
                                : Base64.getEncoder().encodeToString(row.getBinary(0)));
            }
        }
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    static void assertComputedColumns(RecordParser parser) throws Exception {
        byte[] bytes = "plain text".getBytes(StandardCharsets.UTF_8);
        List<ComputedColumn> computed =
                Arrays.asList(
                        new ComputedColumn(
                                "prefix", Expression.substring("PAYLOAD", "0", "5"), false),
                        new ComputedColumn(
                                "short_prefix", Expression.substring("prefix", "0", "2"), true));
        CdcRecord record =
                parser.parse(ByteBuffer.wrap(bytes), computed).toRichCdcRecord().toCdcRecord();
        assertThat(record.data())
                .containsEntry("prefix", "plain")
                .containsEntry("short_prefix", "pl");
        assertThat(
                        CdcRecordUtils.projectAsInsert(
                                        record,
                                        Collections.singletonList(
                                                new DataField(0, "payload", DataTypes.BYTES())))
                                .getBinary(0))
                .containsExactly(bytes);
    }

    private static List<byte[]> binaryValues() {
        byte[] allBytes = new byte[256];
        for (int i = 0; i < allBytes.length; i++) {
            allBytes[i] = (byte) i;
        }
        return Arrays.asList(
                null,
                new byte[0],
                "plain text".getBytes(StandardCharsets.UTF_8),
                "中文😀".getBytes(StandardCharsets.UTF_8),
                new byte[] {0, (byte) 0xff, (byte) 0x80, 65},
                allBytes);
    }

    private static RichCdcMultiplexRecord parse(
            String format, ByteBuffer bytes, List<ComputedColumn> computed) throws Exception {
        if (format.equals("mysql")) {
            return collectRecord(
                    new MySqlRecordParser(
                            new Configuration(),
                            computed,
                            TypeMapping.defaultMapping(),
                            new CdcMetadataConverter[0]),
                    jsonValue(bytes).toString());
        }
        return collectRecord(
                new DebeziumJsonRecordParser(TypeMapping.defaultMapping(), computed),
                jsonValue(bytes));
    }

    @FunctionalInterface
    interface RecordParser {
        RichCdcMultiplexRecord parse(ByteBuffer bytes, List<ComputedColumn> computed)
                throws Exception;
    }

    static RichCdcMultiplexRecord collectRecord(
            FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> parser, Object value)
            throws Exception {
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
        assertThat(records).hasSize(1);
        return records.get(0);
    }

    static JsonNode jsonValue(ByteBuffer buffer) throws Exception {
        ObjectNode value =
                (ObjectNode)
                        MAPPER.readTree(
                                "{\"schema\":{\"fields\":[{\"field\":\"after\",\"fields\":[{\"field\":\"payload\",\"type\":\"bytes\",\"optional\":true},{\"field\":\"id\",\"type\":\"int32\",\"optional\":false}]}]},\"payload\":{\"before\":null,\"after\":{},\"source\":{\"db\":\"test\",\"table\":\"binary_table\"},\"op\":\"c\"}}");
        ObjectNode after = (ObjectNode) value.get("payload").get("after");
        after.put("id", 1);
        if (buffer == null) {
            after.putNull("payload");
        } else {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            after.put("payload", Base64.getEncoder().encodeToString(bytes));
        }
        return value;
    }
}
