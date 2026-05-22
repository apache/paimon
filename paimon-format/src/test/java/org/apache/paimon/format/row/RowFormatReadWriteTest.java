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

package org.apache.paimon.format.row;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the row-store file format. */
public class RowFormatReadWriteTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testBasicReadWrite() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(100)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "test.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, BinaryString.fromString("hello")));
        expected.add(GenericRow.of(2, BinaryString.fromString("world")));
        expected.add(GenericRow.of(3, BinaryString.fromString("paimon")));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(expected.get(i).getInt(0));
            assertThat(result.get(i).getString(1)).isEqualTo(expected.get(i).getString(1));
        }
    }

    @Test
    public void testAllPrimitiveTypes() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f_boolean", new BooleanType()),
                                new DataField(1, "f_tinyint", new TinyIntType()),
                                new DataField(2, "f_smallint", new SmallIntType()),
                                new DataField(3, "f_int", new IntType()),
                                new DataField(4, "f_bigint", new BigIntType()),
                                new DataField(5, "f_float", new FloatType()),
                                new DataField(6, "f_double", new DoubleType()),
                                new DataField(7, "f_string", new VarCharType(200)),
                                new DataField(8, "f_binary", new VarBinaryType(200)),
                                new DataField(9, "f_date", new DateType()),
                                new DataField(10, "f_decimal_compact", new DecimalType(10, 2)),
                                new DataField(11, "f_decimal_large", new DecimalType(30, 10)),
                                new DataField(12, "f_timestamp_compact", new TimestampType(3)),
                                new DataField(13, "f_timestamp_full", new TimestampType(9))));

        Path path = new Path(tempDir.toUri().toString(), "all_types.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(
                GenericRow.of(
                        true,
                        (byte) 127,
                        (short) 32000,
                        Integer.MAX_VALUE,
                        Long.MAX_VALUE,
                        3.14f,
                        2.718281828,
                        BinaryString.fromString("hello world"),
                        new byte[] {1, 2, 3, 4, 5},
                        18000,
                        Decimal.fromBigDecimal(new BigDecimal("12345678.99"), 10, 2),
                        Decimal.fromBigDecimal(
                                new BigDecimal("12345678901234567890.1234567890"), 30, 10),
                        Timestamp.fromEpochMillis(1700000000000L),
                        Timestamp.fromEpochMillis(1700000000000L, 123456)));

        expected.add(
                GenericRow.of(
                        false,
                        (byte) -128,
                        (short) -32000,
                        Integer.MIN_VALUE,
                        Long.MIN_VALUE,
                        -0.0f,
                        Double.MAX_VALUE,
                        BinaryString.fromString(""),
                        new byte[0],
                        0,
                        Decimal.fromBigDecimal(new BigDecimal("-99999999.99"), 10, 2),
                        Decimal.fromBigDecimal(
                                new BigDecimal("-12345678901234567890.1234567890"), 30, 10),
                        Timestamp.fromEpochMillis(0L),
                        Timestamp.fromEpochMillis(0L, 0)));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(2);
        for (int rowIdx = 0; rowIdx < 2; rowIdx++) {
            InternalRow actual = result.get(rowIdx);
            InternalRow exp = expected.get(rowIdx);
            assertThat(actual.getBoolean(0)).isEqualTo(exp.getBoolean(0));
            assertThat(actual.getByte(1)).isEqualTo(exp.getByte(1));
            assertThat(actual.getShort(2)).isEqualTo(exp.getShort(2));
            assertThat(actual.getInt(3)).isEqualTo(exp.getInt(3));
            assertThat(actual.getLong(4)).isEqualTo(exp.getLong(4));
            assertThat(actual.getFloat(5)).isEqualTo(exp.getFloat(5));
            assertThat(actual.getDouble(6)).isEqualTo(exp.getDouble(6));
            assertThat(actual.getString(7)).isEqualTo(exp.getString(7));
            assertThat(actual.getBinary(8)).isEqualTo(exp.getBinary(8));
            assertThat(actual.getInt(9)).isEqualTo(exp.getInt(9));
            assertThat(actual.getDecimal(10, 10, 2)).isEqualTo(exp.getDecimal(10, 10, 2));
            assertThat(actual.getDecimal(11, 30, 10)).isEqualTo(exp.getDecimal(11, 30, 10));
            assertThat(actual.getTimestamp(12, 3)).isEqualTo(exp.getTimestamp(12, 3));
            assertThat(actual.getTimestamp(13, 9)).isEqualTo(exp.getTimestamp(13, 9));
        }
    }

    @Test
    public void testNullValues() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f_int", new IntType(true)),
                                new DataField(1, "f_string", new VarCharType(true, 100)),
                                new DataField(2, "f_double", new DoubleType(true)),
                                new DataField(3, "f_binary", new VarBinaryType(true, 100)),
                                new DataField(4, "f_decimal", new DecimalType(true, 10, 2)),
                                new DataField(5, "f_timestamp", new TimestampType(true, 9))));

        Path path = new Path(tempDir.toUri().toString(), "nulls.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(null, null, null, null, null, null));
        expected.add(
                GenericRow.of(
                        42,
                        BinaryString.fromString("not null"),
                        3.14,
                        new byte[] {1},
                        Decimal.fromBigDecimal(new BigDecimal("1.23"), 10, 2),
                        Timestamp.fromEpochMillis(1000L, 999)));
        expected.add(
                GenericRow.of(null, BinaryString.fromString("partial"), null, null, null, null));
        expected.add(
                GenericRow.of(100, null, 2.0, null, null, Timestamp.fromEpochMillis(2000L, 0)));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(4);

        assertThat(result.get(0).isNullAt(0)).isTrue();
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(0).isNullAt(2)).isTrue();
        assertThat(result.get(0).isNullAt(3)).isTrue();
        assertThat(result.get(0).isNullAt(4)).isTrue();
        assertThat(result.get(0).isNullAt(5)).isTrue();

        assertThat(result.get(1).getInt(0)).isEqualTo(42);
        assertThat(result.get(1).getString(1)).isEqualTo(BinaryString.fromString("not null"));
        assertThat(result.get(1).getDouble(2)).isEqualTo(3.14);
        assertThat(result.get(1).getBinary(3)).isEqualTo(new byte[] {1});
        assertThat(result.get(1).getDecimal(4, 10, 2))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("1.23"), 10, 2));
        assertThat(result.get(1).getTimestamp(5, 9))
                .isEqualTo(Timestamp.fromEpochMillis(1000L, 999));

        assertThat(result.get(2).isNullAt(0)).isTrue();
        assertThat(result.get(2).getString(1)).isEqualTo(BinaryString.fromString("partial"));
        assertThat(result.get(2).isNullAt(2)).isTrue();

        assertThat(result.get(3).getInt(0)).isEqualTo(100);
        assertThat(result.get(3).isNullAt(1)).isTrue();
        assertThat(result.get(3).getDouble(2)).isEqualTo(2.0);
    }

    @Test
    public void testArrayType() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f_int_array", new ArrayType(new IntType())),
                                new DataField(
                                        1, "f_string_array", new ArrayType(new VarCharType(100))),
                                new DataField(
                                        2,
                                        "f_nullable_array",
                                        new ArrayType(true, new IntType(true)))));

        Path path = new Path(tempDir.toUri().toString(), "arrays.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(
                GenericRow.of(
                        new GenericArray(new Object[] {1, 2, 3}),
                        new GenericArray(
                                new Object[] {
                                    BinaryString.fromString("a"), BinaryString.fromString("b")
                                }),
                        new GenericArray(new Object[] {1, null, 3, null, 5})));
        expected.add(
                GenericRow.of(
                        new GenericArray(new Object[0]),
                        new GenericArray(new Object[] {BinaryString.fromString("")}),
                        null));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getArray(0).size()).isEqualTo(3);
        assertThat(result.get(0).getArray(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getArray(0).getInt(1)).isEqualTo(2);
        assertThat(result.get(0).getArray(0).getInt(2)).isEqualTo(3);

        assertThat(result.get(0).getArray(1).size()).isEqualTo(2);
        assertThat(result.get(0).getArray(1).getString(0)).isEqualTo(BinaryString.fromString("a"));
        assertThat(result.get(0).getArray(1).getString(1)).isEqualTo(BinaryString.fromString("b"));

        assertThat(result.get(0).getArray(2).size()).isEqualTo(5);
        assertThat(result.get(0).getArray(2).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getArray(2).isNullAt(1)).isTrue();
        assertThat(result.get(0).getArray(2).getInt(2)).isEqualTo(3);
        assertThat(result.get(0).getArray(2).isNullAt(3)).isTrue();
        assertThat(result.get(0).getArray(2).getInt(4)).isEqualTo(5);

        assertThat(result.get(1).getArray(0).size()).isEqualTo(0);
        assertThat(result.get(1).getArray(1).size()).isEqualTo(1);
        assertThat(result.get(1).isNullAt(2)).isTrue();
    }

    @Test
    public void testMapType() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "f_map",
                                        new MapType(new VarCharType(100), new IntType(true)))));

        Path path = new Path(tempDir.toUri().toString(), "maps.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        Map<Object, Object> map1 = new HashMap<>();
        map1.put(BinaryString.fromString("a"), 1);
        map1.put(BinaryString.fromString("b"), 2);
        map1.put(BinaryString.fromString("c"), null);

        Map<Object, Object> map2 = new HashMap<>();

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(new GenericMap(map1)));
        expected.add(GenericRow.of(new GenericMap(map2)));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getMap(0).size()).isEqualTo(3);
        assertThat(result.get(1).getMap(0).size()).isEqualTo(0);
    }

    @Test
    public void testMapWithNullKeys() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0,
                                        "f_map",
                                        new MapType(
                                                new VarCharType(true, 100), new IntType(true)))));

        Path path = new Path(tempDir.toUri().toString(), "null_key_maps.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        Map<Object, Object> map = new HashMap<>();
        map.put(null, 100);
        map.put(BinaryString.fromString("key"), null);

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(new GenericMap(map)));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.get(0).getMap(0).size()).isEqualTo(2);
    }

    @Test
    public void testNestedRow() throws IOException {
        RowType innerType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "x", new IntType()),
                                new DataField(1, "y", new VarCharType(100))));

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "nested", innerType),
                                new DataField(2, "nullable_nested", innerType)));

        Path path = new Path(tempDir.toUri().toString(), "nested.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, GenericRow.of(10, BinaryString.fromString("inner")), null));
        expected.add(
                GenericRow.of(
                        2,
                        GenericRow.of(20, null),
                        GenericRow.of(30, BinaryString.fromString("deep"))));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        InternalRow nested0 = result.get(0).getRow(1, 2);
        assertThat(nested0.getInt(0)).isEqualTo(10);
        assertThat(nested0.getString(1)).isEqualTo(BinaryString.fromString("inner"));
        assertThat(result.get(0).isNullAt(2)).isTrue();

        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        InternalRow nested1 = result.get(1).getRow(1, 2);
        assertThat(nested1.getInt(0)).isEqualTo(20);
        assertThat(nested1.isNullAt(1)).isTrue();
        InternalRow nested2 = result.get(1).getRow(2, 2);
        assertThat(nested2.getInt(0)).isEqualTo(30);
        assertThat(nested2.getString(1)).isEqualTo(BinaryString.fromString("deep"));
    }

    @Test
    public void testDeeplyNestedTypes() throws IOException {
        RowType innerRowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "v", new IntType()),
                                new DataField(1, "arr", new ArrayType(new IntType()))));

        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(
                                        1,
                                        "nested_array",
                                        new ArrayType(new ArrayType(new IntType()))),
                                new DataField(
                                        2,
                                        "map_of_arrays",
                                        new MapType(
                                                new VarCharType(50), new ArrayType(new IntType()))),
                                new DataField(3, "array_of_rows", new ArrayType(innerRowType))));

        Path path = new Path(tempDir.toUri().toString(), "deeply_nested.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        GenericArray innerArr1 = new GenericArray(new Object[] {1, 2, 3});
        GenericArray innerArr2 = new GenericArray(new Object[] {4, 5});
        GenericArray nestedArray = new GenericArray(new Object[] {innerArr1, innerArr2});

        Map<Object, Object> mapOfArrays = new HashMap<>();
        mapOfArrays.put(BinaryString.fromString("x"), new GenericArray(new Object[] {10, 20}));
        mapOfArrays.put(BinaryString.fromString("y"), new GenericArray(new Object[] {30}));

        GenericRow innerRow1 = GenericRow.of(100, new GenericArray(new Object[] {1, 2}));
        GenericRow innerRow2 = GenericRow.of(200, new GenericArray(new Object[] {3}));
        GenericArray arrayOfRows = new GenericArray(new Object[] {innerRow1, innerRow2});

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, nestedArray, new GenericMap(mapOfArrays), arrayOfRows));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(1);
        InternalRow row = result.get(0);
        assertThat(row.getInt(0)).isEqualTo(1);

        assertThat(row.getArray(1).size()).isEqualTo(2);
        assertThat(row.getArray(1).getArray(0).getInt(0)).isEqualTo(1);
        assertThat(row.getArray(1).getArray(0).getInt(2)).isEqualTo(3);
        assertThat(row.getArray(1).getArray(1).getInt(0)).isEqualTo(4);

        assertThat(row.getMap(2).size()).isEqualTo(2);

        assertThat(row.getArray(3).size()).isEqualTo(2);
        InternalRow readInner0 = row.getArray(3).getRow(0, 2);
        assertThat(readInner0.getInt(0)).isEqualTo(100);
        assertThat(readInner0.getArray(1).getInt(0)).isEqualTo(1);
        assertThat(readInner0.getArray(1).getInt(1)).isEqualTo(2);
        InternalRow readInner1 = row.getArray(3).getRow(1, 2);
        assertThat(readInner1.getInt(0)).isEqualTo(200);
    }

    @Test
    public void testVariantType() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "v", new VariantType())));

        Path path = new Path(tempDir.toUri().toString(), "variant.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        GenericVariant v1 = GenericVariant.fromJson("{\"key\": 123}");
        GenericVariant v2 = GenericVariant.fromJson("[1, 2, 3]");

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, v1));
        expected.add(GenericRow.of(2, v2));
        expected.add(GenericRow.of(3, null));

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getVariant(1).value()).isEqualTo(v1.value());
        assertThat(result.get(0).getVariant(1).metadata()).isEqualTo(v1.metadata());
        assertThat(result.get(1).getVariant(1).value()).isEqualTo(v2.value());
        assertThat(result.get(1).getVariant(1).metadata()).isEqualTo(v2.metadata());
        assertThat(result.get(2).isNullAt(1)).isTrue();
    }

    @Test
    public void testMultipleBlocks() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "multi_block.row");
        Options options = new Options();
        options.setString("file.block-size", "1kb");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> expected = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            expected.add(GenericRow.of(i));
        }

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
        }
    }

    @Test
    public void testRowPositionTracking() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "positions.row");
        Options options = new Options();
        options.setString("file.block-size", "512b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> expected = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            expected.add(GenericRow.of(i));
        }

        writeRows(format, rowType, path, expected);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        long expectedPosition = 0;
        FileRecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                assertThat(batch.returnedPosition()).isEqualTo(expectedPosition);
                assertThat(row.getInt(0)).isEqualTo((int) expectedPosition);
                expectedPosition++;
            }
            batch.releaseBatch();
        }
        assertThat(expectedPosition).isEqualTo(1000);
        reader.close();
    }

    @Test
    public void testProjection() throws IOException {
        RowType fullType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new VarCharType(100)),
                                new DataField(2, "c", new IntType())));

        RowType projectedType = new RowType(Arrays.asList(new DataField(2, "c", new IntType())));

        Path path = new Path(tempDir.toUri().toString(), "projection.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, BinaryString.fromString("a"), 100));
        rows.add(GenericRow.of(2, BinaryString.fromString("b"), 200));
        rows.add(GenericRow.of(3, BinaryString.fromString("c"), 300));

        writeRows(format, fullType, path, rows);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(fullType, projectedType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(GenericRow.of(row.getInt(0))));
        reader.close();

        assertThat(result.size()).isEqualTo(3);
        assertThat(result.get(0).getInt(0)).isEqualTo(100);
        assertThat(result.get(1).getInt(0)).isEqualTo(200);
        assertThat(result.get(2).getInt(0)).isEqualTo(300);
    }

    @Test
    public void testProjectionMultipleColumns() throws IOException {
        RowType fullType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new VarCharType(100)),
                                new DataField(2, "c", new DoubleType()),
                                new DataField(3, "d", new BigIntType())));

        RowType projectedType =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "c", new DoubleType()),
                                new DataField(0, "a", new IntType())));

        Path path = new Path(tempDir.toUri().toString(), "projection_multi.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, BinaryString.fromString("x"), 1.1, 100L));
        rows.add(GenericRow.of(2, BinaryString.fromString("y"), 2.2, 200L));

        writeRows(format, fullType, path, rows);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(fullType, projectedType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(GenericRow.of(row.getDouble(0), row.getInt(1))));
        reader.close();

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getDouble(0)).isEqualTo(1.1);
        assertThat(result.get(0).getInt(1)).isEqualTo(1);
        assertThat(result.get(1).getDouble(0)).isEqualTo(2.2);
        assertThat(result.get(1).getInt(1)).isEqualTo(2);
    }

    @Test
    public void testSelection() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "selection.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> expected = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            expected.add(GenericRow.of(i));
        }

        writeRows(format, rowType, path, expected);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(10);
        selection.add(100);
        selection.add(499);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).containsExactly(0, 10, 100, 499);
    }

    @Test
    public void testSelectionSkipsEntireBlocks() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(100)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "selection_skip.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("val" + i)));
        }

        writeRows(format, rowType, path, rows);

        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(999);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).containsExactly(0, 999);
    }

    @Test
    public void testLargeVariableLengthData() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(10000)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "large_strings.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        Random random = new Random(42);
        List<InternalRow> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            StringBuilder sb = new StringBuilder();
            int len = random.nextInt(5000) + 100;
            for (int j = 0; j < len; j++) {
                sb.append((char) ('a' + random.nextInt(26)));
            }
            expected.add(GenericRow.of(i, BinaryString.fromString(sb.toString())));
        }

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(expected.get(i).getInt(0));
            assertThat(result.get(i).getString(1)).isEqualTo(expected.get(i).getString(1));
        }
    }

    @Test
    public void testEmptyFile() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "empty.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        writeRows(format, rowType, path, new ArrayList<>());
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result).isEmpty();
    }

    @Test
    public void testManyColumns() throws IOException {
        int numCols = 100;
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < numCols; i++) {
            fields.add(new DataField(i, "f" + i, new IntType(true)));
        }
        RowType rowType = new RowType(fields);

        Path path = new Path(tempDir.toUri().toString(), "many_cols.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        for (int r = 0; r < 50; r++) {
            Object[] values = new Object[numCols];
            for (int c = 0; c < numCols; c++) {
                values[c] = (r + c) % 3 == 0 ? null : r * numCols + c;
            }
            expected.add(GenericRow.of(values));
        }

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(50);
        for (int r = 0; r < 50; r++) {
            for (int c = 0; c < numCols; c++) {
                if ((r + c) % 3 == 0) {
                    assertThat(result.get(r).isNullAt(c)).isTrue();
                } else {
                    assertThat(result.get(r).getInt(c)).isEqualTo(r * numCols + c);
                }
            }
        }
    }

    @Test
    public void testRandomizedRoundTrip() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "f_int", new IntType(true)),
                                new DataField(1, "f_long", new BigIntType(true)),
                                new DataField(2, "f_str", new VarCharType(true, 500)),
                                new DataField(3, "f_double", new DoubleType(true)),
                                new DataField(4, "f_bool", new BooleanType(true)),
                                new DataField(5, "f_bytes", new VarBinaryType(true, 500))));

        Path path = new Path(tempDir.toUri().toString(), "random.row");
        Options options = new Options();
        options.setString("file.block-size", "2kb");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        Random random = new Random(12345);
        int numRows = 5000;
        List<InternalRow> expected = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            Object[] values = new Object[6];
            values[0] = random.nextBoolean() ? null : random.nextInt();
            values[1] = random.nextBoolean() ? null : random.nextLong();
            values[2] =
                    random.nextBoolean()
                            ? null
                            : BinaryString.fromString(randomString(random, random.nextInt(200)));
            values[3] = random.nextBoolean() ? null : random.nextDouble();
            values[4] = random.nextBoolean() ? null : random.nextBoolean();
            values[5] = random.nextBoolean() ? null : randomBytes(random, random.nextInt(100));
            expected.add(GenericRow.of(values));
        }

        writeRows(format, rowType, path, expected);
        List<InternalRow> result = readAllRows(format, rowType, path);

        assertThat(result.size()).isEqualTo(numRows);
        for (int i = 0; i < numRows; i++) {
            InternalRow exp = expected.get(i);
            InternalRow act = result.get(i);
            for (int c = 0; c < 6; c++) {
                assertThat(act.isNullAt(c))
                        .as("row %d col %d null mismatch", i, c)
                        .isEqualTo(exp.isNullAt(c));
            }
            if (!act.isNullAt(0)) {
                assertThat(act.getInt(0)).isEqualTo(exp.getInt(0));
            }
            if (!act.isNullAt(1)) {
                assertThat(act.getLong(1)).isEqualTo(exp.getLong(1));
            }
            if (!act.isNullAt(2)) {
                assertThat(act.getString(2)).isEqualTo(exp.getString(2));
            }
            if (!act.isNullAt(3)) {
                assertThat(act.getDouble(3)).isEqualTo(exp.getDouble(3));
            }
            if (!act.isNullAt(4)) {
                assertThat(act.getBoolean(4)).isEqualTo(exp.getBoolean(4));
            }
            if (!act.isNullAt(5)) {
                assertThat(act.getBinary(5)).isEqualTo(exp.getBinary(5));
            }
        }
    }

    @Test
    public void testBlobAndVectorTypes() throws IOException {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "data", new org.apache.paimon.types.BlobType()),
                                new DataField(
                                        2,
                                        "embedding",
                                        new org.apache.paimon.types.VectorType(
                                                4, new FloatType()))));

        Path path = new Path(tempDir.toUri().toString(), "blob_vector.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        LocalFileIO fileIO = new LocalFileIO();
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(
                GenericRow.of(
                        1,
                        org.apache.paimon.data.Blob.fromData(new byte[] {10, 20, 30}),
                        org.apache.paimon.data.BinaryVector.fromPrimitiveArray(
                                new float[] {1.0f, 2.0f, 3.0f, 4.0f})));
        writer.addElement(GenericRow.of(2, null, null));
        writer.close();

        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    GenericRow copy = new GenericRow(3);
                    copy.setField(0, row.getInt(0));
                    copy.setField(1, row.isNullAt(1) ? null : row.getBlob(1));
                    copy.setField(2, row.isNullAt(2) ? null : row.getVector(2));
                    result.add(copy);
                });
        reader.close();

        assertThat(result.size()).isEqualTo(2);
        InternalRow row1 = result.get(0);
        assertThat(row1.getInt(0)).isEqualTo(1);
        assertThat(row1.getBlob(1).toData()).isEqualTo(new byte[] {10, 20, 30});
        org.apache.paimon.data.InternalVector vec = row1.getVector(2);
        assertThat(vec.size()).isEqualTo(4);
        assertThat(vec.getFloat(0)).isEqualTo(1.0f);
        assertThat(vec.getFloat(1)).isEqualTo(2.0f);
        assertThat(vec.getFloat(2)).isEqualTo(3.0f);
        assertThat(vec.getFloat(3)).isEqualTo(4.0f);

        InternalRow row2 = result.get(1);
        assertThat(row2.getInt(0)).isEqualTo(2);
        assertThat(row2.isNullAt(1)).isTrue();
        assertThat(row2.isNullAt(2)).isTrue();
    }

    @Test
    public void testNestedRowProjection() throws IOException {
        RowType innerType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", innerType)));

        Path path = new Path(tempDir.toUri().toString(), "nested_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> expected = new ArrayList<>();
        expected.add(GenericRow.of(1, GenericRow.of(10, 100)));
        expected.add(GenericRow.of(2, GenericRow.of(20, 200)));
        writeRows(format, dataSchema, path, expected);

        // Read with projected type: only top-level 'r', nested only 'b'
        RowType projectedInner = new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "r", projectedInner)));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    // projectedSchema is ROW<r ROW<b>>
                    // row.getRow(0, 1) should return the projected nested row with only 'b'
                    InternalRow nested = row.getRow(0, 1);
                    result.add(GenericRow.of(nested.getInt(0)));
                });
        reader.close();

        // nested.getInt(0) should be 'b' value (100, 200), not 'a' value (10, 20)
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(100);
        assertThat(result.get(1).getInt(0)).isEqualTo(200);
    }

    @Test
    public void testDeeplyNestedProjection() throws IOException {
        // data: ROW<id INT, l1 ROW<x INT, l2 ROW<a INT, b INT, c INT>>>
        RowType level2 =
                new RowType(
                        Arrays.asList(
                                new DataField(20, "a", new IntType()),
                                new DataField(21, "b", new IntType()),
                                new DataField(22, "c", new IntType())));
        RowType level1 =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "x", new IntType()),
                                new DataField(11, "l2", level2)));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "l1", level1)));

        Path path = new Path(tempDir.toUri().toString(), "deep_nested_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, GenericRow.of(10, GenericRow.of(100, 200, 300))));
        rows.add(GenericRow.of(2, GenericRow.of(20, GenericRow.of(400, 500, 600))));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<l1 ROW<l2 ROW<c INT>>>
        RowType projLevel2 = new RowType(Arrays.asList(new DataField(22, "c", new IntType())));
        RowType projLevel1 = new RowType(Arrays.asList(new DataField(11, "l2", projLevel2)));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "l1", projLevel1)));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<Integer> results = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    InternalRow l1 = row.getRow(0, 1);
                    InternalRow l2 = l1.getRow(0, 1);
                    results.add(l2.getInt(0));
                });
        reader.close();

        assertThat(results).containsExactly(300, 600);
    }

    @Test
    public void testNestedProjectionWithNullRows() throws IOException {
        // data: ROW<id INT, r ROW<a INT, b INT>>
        RowType innerType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", innerType)));

        Path path = new Path(tempDir.toUri().toString(), "nested_null_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, GenericRow.of(10, 100)));
        rows.add(GenericRow.of(2, null));
        rows.add(GenericRow.of(3, GenericRow.of(30, 300)));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<r ROW<b INT>>
        RowType projectedInner = new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "r", projectedInner)));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<boolean[]> nullFlags = new ArrayList<>();
        List<Integer> values = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    boolean isNull = row.isNullAt(0);
                    nullFlags.add(new boolean[] {isNull});
                    if (!isNull) {
                        values.add(row.getRow(0, 1).getInt(0));
                    }
                });
        reader.close();

        assertThat(nullFlags.size()).isEqualTo(3);
        assertThat(nullFlags.get(0)[0]).isFalse();
        assertThat(nullFlags.get(1)[0]).isTrue();
        assertThat(nullFlags.get(2)[0]).isFalse();
        assertThat(values).containsExactly(100, 300);
    }

    @Test
    public void testMultipleNestedRowsProjection() throws IOException {
        // data: ROW<r1 ROW<a INT, b INT>, r2 ROW<x INT, y INT>, id INT>
        RowType nested1 =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType nested2 =
                new RowType(
                        Arrays.asList(
                                new DataField(20, "x", new IntType()),
                                new DataField(21, "y", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "r1", nested1),
                                new DataField(1, "r2", nested2),
                                new DataField(2, "id", new IntType())));

        Path path = new Path(tempDir.toUri().toString(), "multi_nested_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(GenericRow.of(1, 2), GenericRow.of(3, 4), 100));
        rows.add(GenericRow.of(GenericRow.of(5, 6), GenericRow.of(7, 8), 200));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<r1 ROW<b INT>, r2 ROW<x INT>>
        RowType projNested1 = new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projNested2 = new RowType(Arrays.asList(new DataField(20, "x", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "r1", projNested1),
                                new DataField(1, "r2", projNested2)));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<int[]> results = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    int b = row.getRow(0, 1).getInt(0);
                    int x = row.getRow(1, 1).getInt(0);
                    results.add(new int[] {b, x});
                });
        reader.close();

        assertThat(results.size()).isEqualTo(2);
        assertThat(results.get(0)).isEqualTo(new int[] {2, 3});
        assertThat(results.get(1)).isEqualTo(new int[] {6, 7});
    }

    @Test
    public void testNestedProjectionWithFieldReordering() throws IOException {
        // data: ROW<id INT, r ROW<a INT, b INT, c INT>>
        RowType innerType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType()),
                                new DataField(12, "c", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "id", new IntType()),
                                new DataField(1, "r", innerType)));

        Path path = new Path(tempDir.toUri().toString(), "nested_reorder_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(1, GenericRow.of(10, 20, 30)));
        rows.add(GenericRow.of(2, GenericRow.of(40, 50, 60)));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<r ROW<c INT, a INT>> (reversed order, skip 'b')
        RowType projectedInner =
                new RowType(
                        Arrays.asList(
                                new DataField(12, "c", new IntType()),
                                new DataField(10, "a", new IntType())));
        RowType projectedSchema = new RowType(Arrays.asList(new DataField(1, "r", projectedInner)));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<int[]> results = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    InternalRow nested = row.getRow(0, 2);
                    results.add(new int[] {nested.getInt(0), nested.getInt(1)});
                });
        reader.close();

        // c, a order
        assertThat(results.size()).isEqualTo(2);
        assertThat(results.get(0)).isEqualTo(new int[] {30, 10});
        assertThat(results.get(1)).isEqualTo(new int[] {60, 40});
    }

    @Test
    public void testArrayElementProjection() throws IOException {
        // data: ROW<arr ARRAY<ROW<a INT, b INT>>>
        RowType elementType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(Arrays.asList(new DataField(0, "arr", new ArrayType(elementType))));

        Path path = new Path(tempDir.toUri().toString(), "array_elem_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        rows.add(
                GenericRow.of(
                        new GenericArray(
                                new Object[] {GenericRow.of(1, 100), GenericRow.of(2, 200)})));
        rows.add(GenericRow.of(new GenericArray(new Object[] {GenericRow.of(3, 300)})));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<arr ARRAY<ROW<b INT>>>
        RowType projectedElementType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "arr", new ArrayType(projectedElementType))));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<Integer> results = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    InternalRow.FieldGetter arrayGetter =
                            InternalRow.createFieldGetter(new ArrayType(projectedElementType), 0);
                    org.apache.paimon.data.InternalArray arr = row.getArray(0);
                    for (int i = 0; i < arr.size(); i++) {
                        results.add(arr.getRow(i, 1).getInt(0));
                    }
                });
        reader.close();

        // Should get 'b' values (100, 200, 300), not 'a' values (1, 2, 3)
        assertThat(results).containsExactly(100, 200, 300);
    }

    @Test
    public void testMapValueProjection() throws IOException {
        // data: ROW<m MAP<INT, ROW<a INT, b INT>>>
        RowType valueType =
                new RowType(
                        Arrays.asList(
                                new DataField(10, "a", new IntType()),
                                new DataField(11, "b", new IntType())));
        RowType dataSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "m", new MapType(new IntType(), valueType))));

        Path path = new Path(tempDir.toUri().toString(), "map_value_proj.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        Map<Object, Object> mapData = new java.util.HashMap<>();
        mapData.put(1, GenericRow.of(10, 100));
        mapData.put(2, GenericRow.of(20, 200));
        List<InternalRow> rows = new ArrayList<>();
        rows.add(GenericRow.of(new GenericMap(mapData)));
        writeRows(format, dataSchema, path, rows);

        // projected: ROW<m MAP<INT, ROW<b INT>>>
        RowType projectedValueType =
                new RowType(Arrays.asList(new DataField(11, "b", new IntType())));
        RowType projectedSchema =
                new RowType(
                        Arrays.asList(
                                new DataField(
                                        0, "m", new MapType(new IntType(), projectedValueType))));

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataSchema, projectedSchema, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        List<Integer> results = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    org.apache.paimon.data.InternalMap m = row.getMap(0);
                    org.apache.paimon.data.InternalArray keys = m.keyArray();
                    org.apache.paimon.data.InternalArray values = m.valueArray();
                    for (int i = 0; i < m.size(); i++) {
                        results.add(values.getRow(i, 1).getInt(0));
                    }
                });
        reader.close();

        // Should get 'b' values (100, 200), not 'a' values (10, 20)
        assertThat(results).containsExactlyInAnyOrder(100, 200);
    }

    // ======================== Helpers ========================

    private void writeRows(FileFormat format, RowType rowType, Path path, List<InternalRow> rows)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
    }

    private List<InternalRow> readAllRows(FileFormat format, RowType rowType, Path path)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(copyRow(row, rowType)));
        reader.close();
        return result;
    }

    private GenericRow copyRow(InternalRow row, RowType rowType) {
        int arity = rowType.getFieldCount();
        GenericRow copy = new GenericRow(arity);
        for (int i = 0; i < arity; i++) {
            if (row.isNullAt(i)) {
                copy.setField(i, null);
            } else {
                copy.setField(i, copyField(row, i, rowType.getTypeAt(i)));
            }
        }
        return copy;
    }

    private Object copyField(InternalRow row, int i, org.apache.paimon.types.DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(i);
            case TINYINT:
                return row.getByte(i);
            case SMALLINT:
                return row.getShort(i);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(i);
            case BIGINT:
                return row.getLong(i);
            case FLOAT:
                return row.getFloat(i);
            case DOUBLE:
                return row.getDouble(i);
            case CHAR:
            case VARCHAR:
                return row.getString(i);
            case BINARY:
            case VARBINARY:
                return row.getBinary(i);
            case DECIMAL:
                {
                    int p = ((org.apache.paimon.types.DecimalType) type).getPrecision();
                    int s = ((org.apache.paimon.types.DecimalType) type).getScale();
                    return row.getDecimal(i, p, s);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    int p = ((org.apache.paimon.types.TimestampType) type).getPrecision();
                    return row.getTimestamp(i, p);
                }
            case VARIANT:
                return row.getVariant(i);
            case ARRAY:
                return row.getArray(i);
            case MAP:
                return row.getMap(i);
            case MULTISET:
                return row.getMap(i);
            case ROW:
                return row.getRow(i, ((RowType) type).getFieldCount());
            default:
                throw new UnsupportedOperationException("Unsupported: " + type);
        }
    }

    private static String randomString(Random random, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    private static byte[] randomBytes(Random random, int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
