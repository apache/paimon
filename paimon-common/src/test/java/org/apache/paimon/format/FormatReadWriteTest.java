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

package org.apache.paimon.format;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test Base class for Format. */
public abstract class FormatReadWriteTest {

    @TempDir java.nio.file.Path tempPath;

    private final String formatType;

    private FileIO fileIO;
    private Path file;

    protected FormatReadWriteTest(String formatType) {
        this.formatType = formatType;
    }

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID().toString());
    }

    protected abstract FileFormat fileFormat();

    @Test
    public void testSimpleTypes() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = (RowType) rowType.notNull();
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        FileFormat format = fileFormat();

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(GenericRow.of(1, 1L));
        writer.addElement(GenericRow.of(2, 2L));
        writer.addElement(GenericRow.of(3, null));
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result)
                .containsExactly(
                        GenericRow.of(1, 1L), GenericRow.of(2, 2L), GenericRow.of(3, null));
    }

    @Test
    public void testFullTypes() throws IOException {
        RowType rowType = rowTypeForFullTypesTest();
        InternalRow expected = expectedRowForFullTypesTest();
        FileFormat format = fileFormat();

        PositionOutputStream out = fileIO.newOutputStream(file, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        writer.addElement(expected);
        writer.close();
        out.close();

        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(result::add);
        assertThat(result.size()).isEqualTo(1);

        validateFullTypesResult(result.get(0), expected);
    }

    @Test
    public void testNestedReadPruning() throws Exception {
        FileFormat format = fileFormat();

        RowType writeType =
                DataTypes.ROW(
                        DataTypes.FIELD(0, "f0", DataTypes.INT()),
                        DataTypes.FIELD(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(3, "f1", DataTypes.INT()),
                                        DataTypes.FIELD(4, "f2", DataTypes.INT()))));

        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = format.createWriterFactory(writeType).create(out, "zstd")) {
            writer.addElement(GenericRow.of(0, GenericRow.of(10, 11, 12)));
        }

        // skip read f0, f1.f1
        RowType readType =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                1,
                                "f1",
                                DataTypes.ROW(
                                        DataTypes.FIELD(2, "f0", DataTypes.INT()),
                                        DataTypes.FIELD(4, "f2", DataTypes.INT()))));

        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(readType)
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(readType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }

        assertThat(result).containsExactly(GenericRow.of(GenericRow.of(10, 12)));
    }

    private RowType rowTypeForFullTypesTest() {
        RowType.Builder builder =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("name", DataTypes.STRING()) /* optional by default */
                        .field("salary", DataTypes.DOUBLE().notNull())
                        .field(
                                "locations",
                                DataTypes.MAP(DataTypes.STRING().notNull(), getMapValueType()))
                        .field(
                                "nonStrKeyMap",
                                DataTypes.MAP(DataTypes.INT().notNull(), getMapValueType()))
                        .field("strArray", DataTypes.ARRAY(DataTypes.STRING()).nullable())
                        .field("intArray", DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .field("boolean", DataTypes.BOOLEAN().nullable())
                        .field("tinyint", DataTypes.TINYINT())
                        .field("smallint", DataTypes.SMALLINT())
                        .field("bigint", DataTypes.BIGINT())
                        .field("bytes", DataTypes.BYTES())
                        .field("timestamp", DataTypes.TIMESTAMP())
                        .field("timestamp_3", DataTypes.TIMESTAMP(3))
                        .field("timestamp_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                        .field("timestamp_ltz_3", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .field("date", DataTypes.DATE())
                        .field("decimal", DataTypes.DECIMAL(2, 2))
                        .field("decimal2", DataTypes.DECIMAL(38, 2))
                        .field("decimal3", DataTypes.DECIMAL(10, 1))
                        .field(
                                "rowArray",
                                DataTypes.ARRAY(
                                        DataTypes.ROW(
                                                DataTypes.FIELD(
                                                        0,
                                                        "int0",
                                                        DataTypes.INT().notNull(),
                                                        "nested row int field 0"),
                                                DataTypes.FIELD(
                                                        1,
                                                        "double1",
                                                        DataTypes.DOUBLE().notNull(),
                                                        "nested row double field 1"))));

        RowType rowType = builder.build();

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = (RowType) rowType.notNull();
        }

        return rowType;
    }

    private GenericRow expectedRowForFullTypesTest() {
        Object[] mapValueData = getMapValueData();
        List<Object> values =
                Arrays.asList(
                        1,
                        fromString("name"),
                        5.26D,
                        new GenericMap(
                                new HashMap<Object, Object>() {
                                    {
                                        this.put(fromString("key1"), mapValueData[0]);
                                        this.put(fromString("key2"), mapValueData[1]);
                                    }
                                }),
                        new GenericMap(
                                new HashMap<Object, Object>() {
                                    {
                                        this.put(1, mapValueData[0]);
                                        this.put(2, mapValueData[1]);
                                    }
                                }),
                        new GenericArray(new Object[] {fromString("123"), fromString("456")}),
                        new GenericArray(new Object[] {123, 456}),
                        true,
                        (byte) 3,
                        (short) 6,
                        12304L,
                        new byte[] {1, 5, 2},
                        Timestamp.fromMicros(123123123),
                        Timestamp.fromEpochMillis(123123123),
                        Timestamp.fromMicros(123123123),
                        Timestamp.fromEpochMillis(123123123),
                        2456,
                        Decimal.fromBigDecimal(new BigDecimal("0.22"), 2, 2),
                        Decimal.fromBigDecimal(new BigDecimal("12312455.22"), 38, 2),
                        Decimal.fromBigDecimal(new BigDecimal("12455.1"), 10, 1),
                        new GenericArray(
                                new Object[] {GenericRow.of(1, 0.1D), GenericRow.of(2, 0.2D)}));
        return GenericRow.of(values.toArray());
    }

    private DataType getMapValueType() {
        if (formatType.equals("avro") || formatType.equals("orc")) {
            return DataTypes.ROW(
                    DataTypes.FIELD(0, "posX", DataTypes.DOUBLE().notNull(), "X field"),
                    DataTypes.FIELD(1, "posY", DataTypes.DOUBLE().notNull(), "Y field"));
        } else {
            return DataTypes.DOUBLE();
        }
    }

    private Object[] getMapValueData() {
        if (formatType.equals("avro") || formatType.equals("orc")) {
            // allow nested row in array
            return new Object[] {GenericRow.of(5.2D, 6.2D), GenericRow.of(6.2D, 2.2D)};
        } else {
            return new Object[] {5.2D, 6.2D};
        }
    }

    private void validateFullTypesResult(InternalRow actual, InternalRow expected) {
        if (formatType.equals("avro")) {
            assertThat(actual).isEqualTo(expected);
        } else {
            RowType rowType = rowTypeForFullTypesTest();
            InternalRow.FieldGetter[] fieldGetters = rowType.fieldGetters();
            for (int i = 0; i < fieldGetters.length; i++) {
                String name = rowType.getFieldNames().get(i);
                Object actualField = fieldGetters[i].getFieldOrNull(actual);
                Object expectedField = fieldGetters[i].getFieldOrNull(expected);
                switch (name) {
                    case "locations":
                        validateInternalMap(
                                (InternalMap) actualField,
                                (InternalMap) expectedField,
                                DataTypes.STRING());
                        break;
                    case "nonStrKeyMap":
                        validateInternalMap(
                                (InternalMap) actualField,
                                (InternalMap) expectedField,
                                DataTypes.INT());
                        break;
                    case "strArray":
                        validateInternalArray(
                                (InternalArray) actualField,
                                (InternalArray) expectedField,
                                DataTypes.STRING());
                        break;
                    case "intArray":
                        validateInternalArray(
                                (InternalArray) actualField,
                                (InternalArray) expectedField,
                                DataTypes.INT());
                        break;
                    case "rowArray":
                        validateInternalArray(
                                (InternalArray) actualField,
                                (InternalArray) expectedField,
                                ((ArrayType)
                                                rowType.getFields().stream()
                                                        .filter(f -> f.name().equals("rowArray"))
                                                        .findAny()
                                                        .get()
                                                        .type())
                                        .getElementType());
                        break;
                    default:
                        assertThat(actualField).isEqualTo(expectedField);
                        break;
                }
            }
        }
    }

    private void validateInternalMap(
            InternalMap actualMap, InternalMap expectedMap, DataType keyType) {
        validateInternalArray(actualMap.keyArray(), expectedMap.keyArray(), keyType);
        validateInternalArray(actualMap.valueArray(), expectedMap.valueArray(), getMapValueType());
    }

    private void validateInternalArray(
            InternalArray actualArray, InternalArray expectedArray, DataType elementType) {
        assertThat(actualArray.size()).isEqualTo(expectedArray.size());
        switch (elementType.getTypeRoot()) {
            case VARCHAR:
                for (int i = 0; i < actualArray.size(); i++) {
                    assertThat(actualArray.getString(i)).isEqualTo(expectedArray.getString(i));
                }
                break;
            case DOUBLE:
                assertThat(actualArray.toDoubleArray()).isEqualTo(expectedArray.toDoubleArray());
                break;
            case INTEGER:
                assertThat(actualArray.toIntArray()).isEqualTo(expectedArray.toIntArray());
                break;
            case ROW:
                InternalArray.ElementGetter getter = InternalArray.createElementGetter(elementType);
                RowType rowType = (RowType) elementType;
                for (int i = 0; i < expectedArray.size(); i++) {
                    InternalRow actual = (InternalRow) getter.getElementOrNull(actualArray, i);
                    InternalRow expected = (InternalRow) getter.getElementOrNull(expectedArray, i);
                    assertThat(actual.getFieldCount()).isEqualTo(expected.getFieldCount());
                    for (int j = 0; j < actual.getFieldCount(); j++) {
                        InternalRow.FieldGetter fieldGetter =
                                InternalRow.createFieldGetter(rowType.getTypeAt(j), j);
                        assertThat(fieldGetter.getFieldOrNull(expected))
                                .isEqualTo(fieldGetter.getFieldOrNull(actual));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Haven't implemented array comparing for type "
                                + elementType.getTypeRoot());
        }
    }
}
