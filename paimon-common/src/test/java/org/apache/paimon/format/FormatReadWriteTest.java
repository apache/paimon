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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VariantType;

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
import java.util.stream.IntStream;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test Base class for Format. */
public abstract class FormatReadWriteTest {

    @TempDir java.nio.file.Path tempPath;

    protected final String formatType;

    protected FileIO fileIO;
    protected Path file;
    protected Path parent;

    protected FormatReadWriteTest(String formatType) {
        this.formatType = formatType;
    }

    @BeforeEach
    public void beforeEach() {
        this.fileIO = LocalFileIO.create();
        this.parent = new Path(tempPath.toUri());
        this.file = new Path(new Path(tempPath.toUri()), UUID.randomUUID() + "." + formatType);
    }

    protected abstract FileFormat fileFormat();

    @Test
    public void testSimpleTypes() throws IOException {
        FileFormat format = fileFormat();
        testSimpleTypesUtil(format, file);
    }

    protected void testSimpleTypesUtil(FileFormat format, Path file) throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.BIGINT());

        if (ThreadLocalRandom.current().nextBoolean()) {
            rowType = rowType.notNull();
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        FormatWriterFactory factory = format.createWriterFactory(rowType);
        write(factory, file, GenericRow.of(1, 1L), GenericRow.of(2, 2L), GenericRow.of(3, null));
        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));

        assertThat(result.get(0)).isEqualTo(GenericRow.of(1, 1L));
        assertThat(result.get(1)).isEqualTo(GenericRow.of(2, 2L));
        assertThat(result.get(2).getInt(0)).isEqualTo(3);
        assertThat(result.get(2).isNullAt(1)).isTrue();
    }

    @Test
    public void testFullTypes() throws IOException {
        FileFormat format = fileFormat();
        testFullTypesUtil(format, file);
    }

    protected void testFullTypesUtil(FileFormat format, Path file) throws IOException {
        RowType rowType = rowTypeForFullTypesTest();
        InternalRow expected = expectedRowForFullTypesTest();

        FormatWriterFactory factory = format.createWriterFactory(rowType);
        InternalRow toWrite = expected;
        for (int i = 0; i < 2; i++) {
            write(factory, file, toWrite);
            RecordReader<InternalRow> reader =
                    format.createReaderFactory(rowType, rowType, new ArrayList<>())
                            .createReader(
                                    new FormatReaderContext(
                                            fileIO, file, fileIO.getFileSize(file)));
            InternalRowSerializer internalRowSerializer = new InternalRowSerializer(rowType);
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(row -> result.add(internalRowSerializer.copy(row)));
            assertThat(result.size()).isEqualTo(1);
            validateFullTypesResult(result.get(0), expected);
            toWrite = result.get(0);
            fileIO.deleteQuietly(file);
        }
    }

    public boolean supportNestedReadPruning() {
        return true;
    }

    public String compression() {
        return "zstd";
    }

    @Test
    public void testNestedReadPruning() throws Exception {
        if (!supportNestedReadPruning()) {
            return;
        }
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

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        write(factory, file, GenericRow.of(0, GenericRow.of(10, 11, 12)));

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
                format.createReaderFactory(readType, readType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(readType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }

        assertThat(result.get(0).getRow(0, 2).getInt(0)).isEqualTo(10);
        assertThat(result.get(0).getRow(0, 2).getInt(1)).isEqualTo(12);
    }

    @Test
    public void testReadWriteVariant() throws IOException {
        FileFormat format = fileFormat();
        // todo: support other format types
        if (!format.getFormatIdentifier().equals("parquet")) {
            return;
        }

        RowType writeType = DataTypes.ROW(DataTypes.FIELD(0, "v", DataTypes.VARIANT()));

        FormatWriterFactory factory = format.createWriterFactory(writeType);
        write(
                factory,
                file,
                GenericRow.of(GenericVariant.fromJson("{\"age\":35,\"city\":\"Chicago\"}")));
        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(writeType, writeType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(writeType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }

        assertThat(result.get(0).getVariant(0).toJson())
                .isEqualTo("{\"age\":35,\"city\":\"Chicago\"}");
    }

    @Test
    public void testReadWriteVariantList() throws IOException {
        FileFormat format = fileFormat();
        // todo: support other format types
        if (!format.getFormatIdentifier().equals("parquet")) {
            return;
        }

        RowType writeType = DataTypes.ROW(new ArrayType(true, new VariantType()));
        write(
                format.createWriterFactory(writeType),
                file,
                GenericRow.of(
                        new GenericArray(
                                new Object[] {
                                    GenericVariant.fromJson("{\"age\":35,\"city\":\"Chicago\"}"),
                                    GenericVariant.fromJson("{\"age\":45,\"city\":\"Beijing\"}")
                                })));

        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(writeType, writeType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(fileIO, file, fileIO.getFileSize(file)))) {
            InternalRowSerializer serializer = new InternalRowSerializer(writeType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }
        InternalRow internalRow = result.get(0);
        BinaryArray array = (BinaryArray) internalRow.getArray(0);
        assertThat(array.getVariant(0).toJson()).isEqualTo("{\"age\":35,\"city\":\"Chicago\"}");
        assertThat(array.getVariant(1).toJson()).isEqualTo("{\"age\":45,\"city\":\"Beijing\"}");
    }

    @Test
    public void testWriteNullToNonNullField() {
        FileFormat format = fileFormat();
        String identifier = format.getFormatIdentifier();
        // no agg for these formats now
        assumeTrue(!identifier.equals("csv") && !identifier.equals("json"));

        FormatWriterFactory factory =
                format.createWriterFactory(
                        RowType.builder().field("f0", DataTypes.INT().notNull()).build());

        assertThatThrownBy(() -> write(factory, file, GenericRow.of((Object) null)))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "Field 'f0' expected not null but found null value. A possible cause is "
                                        + "that the table used partial-update or aggregation merge-engine and the aggregate "
                                        + "function produced null value when retracting."));
    }

    protected void write(FormatWriterFactory factory, Path file, InternalRow... rows)
            throws IOException {
        FormatWriter writer;
        PositionOutputStream out = null;
        if (factory instanceof SupportsDirectWrite) {
            writer = ((SupportsDirectWrite) factory).create(fileIO, file, this.compression());
        } else {
            out = fileIO.newOutputStream(file, false);
            writer = factory.create(out, this.compression());
        }
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
        if (out != null) {
            out.close();
        }
    }

    protected RowType rowTypeForFullTypesTest() {
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
                        .field(
                                "allStrMap",
                                DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING()))
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
            rowType = rowType.notNull();
        }
        return rowType;
    }

    protected GenericRow expectedRowForFullTypesTest() {
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
                        new GenericMap(
                                new HashMap<Object, Object>() {
                                    {
                                        this.put(fromString("mykey1"), fromString("v1"));
                                        this.put(fromString("mykey2"), null);
                                        this.put(
                                                fromString("mykey3"),
                                                fromString("a_very_very_long_string"));
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

    public boolean supportDataFileWithoutExtension() {
        return false;
    }

    @Test
    public void testWriteAndReadFileWithoutExtension() throws IOException {
        if (!supportDataFileWithoutExtension()) {
            return;
        }
        RowType rowType =
                RowType.of(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.BOOLEAN());

        // Create test data
        List<InternalRow> testData = new ArrayList<>();
        testData.add(GenericRow.of(1, BinaryString.fromString("Alice"), true));
        testData.add(GenericRow.of(2, BinaryString.fromString("Bob"), false));
        testData.add(GenericRow.of(3, BinaryString.fromString("Charlie"), true));

        // Create file format
        FileFormat jsonFormat = fileFormat();

        // Write data
        Path filePath = new Path(parent, UUID.randomUUID().toString());
        FormatWriterFactory writerFactory = jsonFormat.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(filePath, false), compression())) {
            for (InternalRow row : testData) {
                writer.addElement(row);
            }
        }

        // Read data
        FormatReaderFactory readerFactory = jsonFormat.createReaderFactory(rowType, rowType, null);
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderFactory.Context() {
                            @Override
                            public FileIO fileIO() {
                                return fileIO;
                            }

                            @Override
                            public Path filePath() {
                                return filePath;
                            }

                            @Override
                            public long fileSize() {
                                try {
                                    return fileIO.getFileSize(filePath);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public org.apache.paimon.utils.RoaringBitmap32 selection() {
                                return null;
                            }
                        });

        List<InternalRow> readData = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        while (iterator != null) {
            InternalRow row;
            while ((row = iterator.next()) != null) {
                readData.add(GenericRow.of(row.getInt(0), row.getString(1), row.getBoolean(2)));
            }
            iterator.releaseBatch();
            iterator = reader.readBatch();
        }
        reader.close();

        // Verify data
        assertThat(readData).hasSize(3);
        assertThat(readData.get(0).getInt(0)).isEqualTo(1);
        assertThat(readData.get(0).getString(1).toString()).isEqualTo("Alice");
        assertThat(readData.get(0).getBoolean(2)).isTrue();

        assertThat(readData.get(1).getInt(0)).isEqualTo(2);
        assertThat(readData.get(1).getString(1).toString()).isEqualTo("Bob");
        assertThat(readData.get(1).getBoolean(2)).isFalse();

        assertThat(readData.get(2).getInt(0)).isEqualTo(3);
        assertThat(readData.get(2).getString(1).toString()).isEqualTo("Charlie");
        assertThat(readData.get(2).getBoolean(2)).isTrue();
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

    protected void validateFullTypesResult(InternalRow actual, InternalRow expected) {
        RowType rowType = rowTypeForFullTypesTest();
        InternalRow.FieldGetter[] fieldGetters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(i -> InternalRow.createFieldGetter(rowType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
        for (int i = 0; i < fieldGetters.length; i++) {
            String name = rowType.getFieldNames().get(i);
            Object actualField = fieldGetters[i].getFieldOrNull(actual);
            Object expectedField = fieldGetters[i].getFieldOrNull(expected);
            switch (name) {
                case "locations":
                    validateInternalMap(
                            (InternalMap) actualField,
                            (InternalMap) expectedField,
                            DataTypes.STRING(),
                            getMapValueType());
                    break;
                case "nonStrKeyMap":
                    validateInternalMap(
                            (InternalMap) actualField,
                            (InternalMap) expectedField,
                            DataTypes.INT(),
                            getMapValueType());
                    break;
                case "allStrMap":
                    validateInternalMap(
                            (InternalMap) actualField,
                            (InternalMap) expectedField,
                            DataTypes.STRING(),
                            DataTypes.STRING());
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

    private void validateInternalMap(
            InternalMap actualMap, InternalMap expectedMap, DataType keyType, DataType valueType) {
        validateInternalArray(actualMap.keyArray(), expectedMap.keyArray(), keyType);
        validateInternalArray(actualMap.valueArray(), expectedMap.valueArray(), valueType);
    }

    private void validateInternalArray(
            InternalArray actualArray, InternalArray expectedArray, DataType elementType) {
        assertThat(actualArray.size()).isEqualTo(expectedArray.size());
        switch (elementType.getTypeRoot()) {
            case VARCHAR:
                for (int i = 0; i < actualArray.size(); i++) {
                    if (expectedArray.isNullAt(i)) {
                        assertThat(actualArray.isNullAt(i)).isTrue();
                    } else {
                        assertThat(actualArray.getString(i)).isEqualTo(expectedArray.getString(i));
                    }
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
