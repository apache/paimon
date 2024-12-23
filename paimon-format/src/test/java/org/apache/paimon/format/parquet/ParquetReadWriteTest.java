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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ParquetReaderFactory}. */
public class ParquetReadWriteTest {

    private static final LocalDateTime BASE_TIME = LocalDateTime.now();

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .fields(
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new BooleanType(),
                            new TinyIntType(),
                            new SmallIntType(),
                            new IntType(),
                            new BigIntType(),
                            new FloatType(),
                            new DoubleType(),
                            new TimestampType(3),
                            new TimestampType(6),
                            new TimestampType(9),
                            new DecimalType(5, 0),
                            new DecimalType(15, 2),
                            new DecimalType(20, 0),
                            new DecimalType(5, 0),
                            new DecimalType(15, 0),
                            new DecimalType(20, 0),
                            new ArrayType(new VarCharType(VarCharType.MAX_LENGTH)),
                            new ArrayType(new BooleanType()),
                            new ArrayType(new TinyIntType()),
                            new ArrayType(new SmallIntType()),
                            new ArrayType(new IntType()),
                            new ArrayType(new BigIntType()),
                            new ArrayType(new FloatType()),
                            new ArrayType(new DoubleType()),
                            new ArrayType(new TimestampType(9)),
                            new ArrayType(new DecimalType(5, 0)),
                            new ArrayType(new DecimalType(15, 0)),
                            new ArrayType(new DecimalType(20, 0)),
                            new ArrayType(new DecimalType(5, 0)),
                            new ArrayType(new DecimalType(15, 0)),
                            new ArrayType(new DecimalType(20, 0)),
                            new MapType(
                                    new VarCharType(VarCharType.MAX_LENGTH),
                                    new VarCharType(VarCharType.MAX_LENGTH)),
                            new MapType(new IntType(), new BooleanType()),
                            new MultisetType(new VarCharType(VarCharType.MAX_LENGTH)),
                            RowType.builder()
                                    .fields(new VarCharType(VarCharType.MAX_LENGTH), new IntType())
                                    .build(),
                            new MapType(
                                    new TimestampType(6), new VarCharType(VarCharType.MAX_LENGTH)))
                    .build();

    private static final RowType NESTED_ARRAY_MAP_TYPE =
            RowType.of(
                    new IntType(),
                    new ArrayType(true, new IntType()),
                    new ArrayType(true, new ArrayType(true, new IntType())),
                    new ArrayType(
                            true,
                            new MapType(
                                    true,
                                    new VarCharType(VarCharType.MAX_LENGTH),
                                    new VarCharType(VarCharType.MAX_LENGTH))),
                    new ArrayType(true, RowType.builder().field("a", new IntType()).build()),
                    RowType.of(
                            new IntType(),
                            new ArrayType(
                                    true,
                                    RowType.builder()
                                            .field(
                                                    "b",
                                                    new ArrayType(
                                                            true,
                                                            new ArrayType(true, new IntType())))
                                            .field("c", new IntType())
                                            .build())),
                    RowType.of(
                            new ArrayType(RowType.of(new VarCharType(255))),
                            RowType.of(new IntType()),
                            new VarCharType(255)));

    @TempDir public File folder;

    public static Collection<Integer> parameters() {
        return Arrays.asList(10, 1000);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testTypesReadWithSplits(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt(number / 2);
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDictionary(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            int v = intValues[random.nextInt(10)];
            values.add(v == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPartialDictionary(int rowGroupSize) throws IOException {
        // prepare parquet file
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        int[] intValues = new int[10];
        // test large values in dictionary
        for (int i = 0; i < intValues.length; i++) {
            intValues[i] = random.nextInt();
        }
        for (int i = 0; i < number; i++) {
            int v = i < 5000 ? intValues[random.nextInt(10)] : i;
            values.add(v == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testContinuousRepetition(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int v = random.nextInt(10);
            for (int j = 0; j < 100; j++) {
                values.add(v == 0 ? null : v);
            }
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testLargeValue(int rowGroupSize) throws IOException {
        int number = 10000;
        List<Integer> values = new ArrayList<>(number);
        Random random = new Random();
        for (int i = 0; i < number; i++) {
            int v = random.nextInt();
            values.add(v % 10 == 0 ? null : v);
        }

        innerTestTypes(folder, values, rowGroupSize);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProjection(int rowGroupSize) throws IOException {
        int number = 1000;
        List<InternalRow> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFileByPaimon(folder, records, rowGroupSize, ROW_TYPE);
        // test reader
        DataType[] fieldTypes = new DataType[] {new DoubleType(), new TinyIntType(), new IntType()};
        ParquetReaderFactory format =
                new ParquetReaderFactory(
                        new Options(),
                        RowType.builder()
                                .fields(fieldTypes, new String[] {"f7", "f2", "f4"})
                                .build(),
                        500,
                        FilterCompat.NOOP);

        AtomicInteger cnt = new AtomicInteger(0);
        RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(),
                                testPath,
                                new LocalFileIO().getFileSize(testPath)));
        reader.forEachRemaining(
                row -> {
                    int i = cnt.get();
                    assertThat(row.getDouble(0)).isEqualTo(i);
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    cnt.incrementAndGet();
                });
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testProjectionReadUnknownField(int rowGroupSize) throws IOException {
        int number = 1000;
        List<InternalRow> records = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFileByPaimon(folder, records, rowGroupSize, ROW_TYPE);

        // test reader
        DataType[] fieldTypes =
                new DataType[] {
                    new DoubleType(), new TinyIntType(), new IntType(), new VarCharType()
                };
        ParquetReaderFactory format =
                new ParquetReaderFactory(
                        new Options(),
                        // f99 not exist in parquet file.
                        RowType.builder()
                                .fields(fieldTypes, new String[] {"f7", "f2", "f4", "f99"})
                                .build(),
                        500,
                        FilterCompat.NOOP);

        AtomicInteger cnt = new AtomicInteger(0);
        RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(),
                                testPath,
                                new LocalFileIO().getFileSize(testPath)));
        reader.forEachRemaining(
                row -> {
                    int i = cnt.get();
                    assertThat(row.getDouble(0)).isEqualTo(i);
                    assertThat(row.getByte(1)).isEqualTo((byte) i);
                    assertThat(row.getInt(2)).isEqualTo(i);
                    assertThat(row.isNullAt(3)).isTrue();
                    cnt.incrementAndGet();
                });
    }

    @RepeatedTest(10)
    void testReadRowPosition() throws IOException {
        int recordNumber = new Random().nextInt(10000) + 1;
        int batchSize = new Random().nextInt(1000) + 1;
        int rowGroupSize = new Random().nextInt(1000) + 1;
        List<InternalRow> records = new ArrayList<>(recordNumber);
        for (int i = 0; i < recordNumber; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFileByPaimon(folder, records, rowGroupSize, ROW_TYPE);

        DataType[] fieldTypes = new DataType[] {new DoubleType()};
        ParquetReaderFactory format =
                new ParquetReaderFactory(
                        new Options(),
                        RowType.builder().fields(fieldTypes, new String[] {"f7"}).build(),
                        batchSize,
                        FilterCompat.NOOP);

        AtomicInteger cnt = new AtomicInteger(0);
        try (RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(),
                                testPath,
                                new LocalFileIO().getFileSize(testPath)))) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> {
                        assertThat(row.getDouble(0)).isEqualTo(cnt.get());
                        // check row position
                        assertThat(rowPosition).isEqualTo(cnt.get());
                        cnt.incrementAndGet();
                    });
        }
    }

    @RepeatedTest(10)
    void testReadRowPositionWithRandomFilter() throws IOException {
        int recordNumber = new Random().nextInt(10000) + 1;
        int batchSize = new Random().nextInt(1000) + 1;
        // make row group size = 1, then the row count in one row group will be
        // `parquet.page.size.row.check.min`, which default value is 100
        int rowGroupSize = 1;
        int rowGroupCount = 100;
        int randomStart = new Random().nextInt(10000) + 1;
        List<InternalRow> records = new ArrayList<>(recordNumber);
        for (int i = 0; i < recordNumber; i++) {
            Integer v = i;
            records.add(newRow(v));
        }

        Path testPath = createTempParquetFileByPaimon(folder, records, rowGroupSize, ROW_TYPE);

        DataType[] fieldTypes = new DataType[] {new IntType()};
        // Build filter: f4 > randomStart
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(new DataField(0, "f4", new IntType()))));
        FilterCompat.Filter filter =
                ParquetFilters.convert(
                        PredicateBuilder.splitAnd(builder.greaterThan(0, randomStart)));
        ParquetReaderFactory format =
                new ParquetReaderFactory(
                        new Options(),
                        RowType.builder().fields(fieldTypes, new String[] {"f4"}).build(),
                        batchSize,
                        filter);

        AtomicBoolean isFirst = new AtomicBoolean(true);
        try (RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(),
                                testPath,
                                new LocalFileIO().getFileSize(testPath)))) {
            reader.forEachRemainingWithPosition(
                    (rowPosition, row) -> {
                        // check filter
                        // Note: the minimum unit of filter is row group
                        if (isFirst.get()) {
                            assertThat(randomStart - row.getInt(0)).isLessThan(rowGroupCount);
                            isFirst.set(false);
                        }
                        // check row position
                        // Note: in the written file, field f4's value is equaled to row position,
                        // so we can use it to check row position
                        assertThat(rowPosition).isEqualTo(row.getInt(0));
                    });
        }
    }

    @ParameterizedTest
    @CsvSource({"10, paimon", "1000, paimon", "10, origin", "1000, origin"})
    public void testNestedRead(int rowGroupSize, String writerType) throws Exception {
        List<InternalRow> rows = prepareNestedData(1283);
        Path path;
        if ("paimon".equals(writerType)) {
            path = createTempParquetFileByPaimon(folder, rows, rowGroupSize, NESTED_ARRAY_MAP_TYPE);
        } else if ("origin".equals(writerType)) {
            path = createNestedDataByOriginWriter(1283, folder, rowGroupSize);
        } else {
            throw new RuntimeException("Unknown writer type.");
        }
        ParquetReaderFactory format =
                new ParquetReaderFactory(
                        new Options(), NESTED_ARRAY_MAP_TYPE, 500, FilterCompat.NOOP);
        RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(), path, new LocalFileIO().getFileSize(path)));
        List<InternalRow> results = new ArrayList<>(1283);
        reader.forEachRemaining(results::add);
        compareNestedRow(rows, results);
    }

    @Test
    public void testNestedNullMapKey() {
        List<InternalRow> rows = prepareNestedData(1283, true);
        assertThatThrownBy(
                        () ->
                                createTempParquetFileByPaimon(
                                        folder, rows, 10, NESTED_ARRAY_MAP_TYPE),
                        "Parquet does not support null keys in a map. "
                                + "See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps for more details.")
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testConvertToParquetTypeWithId() {
        List<DataField> nestedFields =
                Arrays.asList(
                        new DataField(3, "v1", DataTypes.INT()),
                        new DataField(4, "v2", DataTypes.STRING()));
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "a", DataTypes.INT()),
                        new DataField(1, "b", DataTypes.ARRAY(DataTypes.STRING())),
                        new DataField(
                                2,
                                "c",
                                DataTypes.MAP(
                                        DataTypes.INT(),
                                        DataTypes.MAP(
                                                DataTypes.BIGINT(), new RowType(nestedFields)))));
        RowType rowType = new RowType(fields);

        int baseId = 536870911;
        int depthLimit = 1 << 10;
        Type innerMapValueType =
                new GroupType(
                                Type.Repetition.OPTIONAL,
                                "value",
                                Types.primitive(INT32, Type.Repetition.OPTIONAL)
                                        .named("v1")
                                        .withId(3),
                                Types.primitive(
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                Type.Repetition.OPTIONAL)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .named("v2")
                                        .withId(4))
                        .withId(baseId + depthLimit * 2 + 2);
        Type outerMapValueType =
                ConversionPatterns.mapType(
                                Type.Repetition.OPTIONAL,
                                "value",
                                "key_value",
                                Types.primitive(INT64, Type.Repetition.REQUIRED)
                                        .named("key")
                                        .withId(baseId - depthLimit * 2 - 2),
                                innerMapValueType)
                        .withId(baseId + depthLimit * 2 + 1);
        Type expected =
                new MessageType(
                        "table",
                        Types.primitive(INT32, Type.Repetition.OPTIONAL).named("a").withId(0),
                        ConversionPatterns.listOfElements(
                                        Type.Repetition.OPTIONAL,
                                        "b",
                                        Types.primitive(
                                                        PrimitiveType.PrimitiveTypeName.BINARY,
                                                        Type.Repetition.OPTIONAL)
                                                .as(LogicalTypeAnnotation.stringType())
                                                .named("element")
                                                .withId(baseId + depthLimit + 1))
                                .withId(1),
                        ConversionPatterns.mapType(
                                        Type.Repetition.OPTIONAL,
                                        "c",
                                        "key_value",
                                        Types.primitive(INT32, Type.Repetition.REQUIRED)
                                                .named("key")
                                                .withId(baseId - depthLimit * 2 - 1),
                                        outerMapValueType)
                                .withId(2));
        Type actual = ParquetSchemaConverter.convertToParquetMessageType("table", rowType);
        assertThat(actual).isEqualTo(expected);
    }

    private void innerTestTypes(File folder, List<Integer> records, int rowGroupSize)
            throws IOException {
        List<InternalRow> rows = records.stream().map(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFileByPaimon(folder, rows, rowGroupSize, ROW_TYPE);
        int len = testReadingFile(subList(records, 0), testPath);
        assertThat(len).isEqualTo(records.size());
    }

    private Path createTempParquetFileByPaimon(
            File folder, List<InternalRow> rows, int rowGroupSize, RowType rowType)
            throws IOException {
        // write data
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Options conf = new Options();
        conf.setInteger("parquet.block.size", rowGroupSize);
        if (ThreadLocalRandom.current().nextBoolean()) {
            conf.set("parquet.enable.dictionary", "false");
        }
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(rowType, conf));
        String[] candidates = new String[] {"snappy", "zstd", "gzip"};
        String compress = candidates[new Random().nextInt(3)];
        FormatWriter writer =
                factory.create(new LocalFileIO().newOutputStream(path, false), compress);
        for (InternalRow row : rows) {
            writer.addElement(row);
        }

        writer.close();
        return path;
    }

    private int testReadingFile(List<Integer> expected, Path path) throws IOException {
        ParquetReaderFactory format =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        RecordReader<InternalRow> reader =
                format.createReader(
                        new FormatReaderContext(
                                new LocalFileIO(), path, new LocalFileIO().getFileSize(path)));

        AtomicInteger cnt = new AtomicInteger(0);
        final AtomicReference<InternalRow> previousRow = new AtomicReference<>();
        reader.forEachRemaining(
                row -> {
                    if (previousRow.get() == null) {
                        previousRow.set(row);
                    } else {
                        // ParquetColumnarRowInputFormat should only have one row instance.
                        assertThat(row).isSameAs(previousRow.get());
                    }
                    Integer v = expected.get(cnt.get());
                    if (v == null) {
                        for (int i = 0; i < 35; i++) {
                            assertThat(row.isNullAt(i)).isTrue();
                        }
                    } else {
                        assertThat(row.getString(0)).hasToString("" + v);
                        assertThat(row.getBoolean(1)).isEqualTo(v % 2 == 0);
                        assertThat(row.getByte(2)).isEqualTo(v.byteValue());
                        assertThat(row.getShort(3)).isEqualTo(v.shortValue());
                        assertThat(row.getInt(4)).isEqualTo(v.intValue());
                        assertThat(row.getLong(5)).isEqualTo(v.longValue());
                        assertThat(row.getFloat(6)).isEqualTo(v.floatValue());
                        assertThat(row.getDouble(7)).isEqualTo(v.doubleValue());
                        assertThat(row.getTimestamp(8, 3)).isEqualTo(toMills(v));
                        assertThat(row.getTimestamp(9, 6)).isEqualTo(toMicros(v));
                        assertThat(row.getTimestamp(10, 9)).isEqualTo(toNanos(v));
                        if (Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null) {
                            assertThat(row.isNullAt(11)).isTrue();
                            assertThat(row.isNullAt(14)).isTrue();
                            assertThat(row.isNullAt(26)).isTrue();
                            assertThat(row.isNullAt(29)).isTrue();
                        } else {
                            assertThat(row.getDecimal(11, 5, 0))
                                    .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getDecimal(14, 5, 0))
                                    .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getArray(26).getDecimal(0, 5, 0))
                                    .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                            assertThat(row.getArray(29).getDecimal(0, 5, 0))
                                    .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0));
                        }
                        assertThat(row.getDecimal(12, 15, 2))
                                .isEqualTo(Decimal.fromUnscaledLong(v.longValue(), 15, 2));
                        assertThat(row.getDecimal(13, 20, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
                        assertThat(row.getDecimal(15, 15, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
                        assertThat(row.getDecimal(16, 20, 0).toBigDecimal())
                                .isEqualTo(BigDecimal.valueOf(v));
                        assertThat(row.getArray(17).getString(0)).hasToString("" + v);
                        assertThat(row.getArray(18).getBoolean(0)).isEqualTo(v % 2 == 0);
                        assertThat(row.getArray(19).getByte(0)).isEqualTo(v.byteValue());
                        assertThat(row.getArray(20).getShort(0)).isEqualTo(v.shortValue());
                        assertThat(row.getArray(21).getInt(0)).isEqualTo(v.intValue());
                        assertThat(row.getArray(22).getLong(0)).isEqualTo(v.longValue());
                        assertThat(row.getArray(23).getFloat(0)).isEqualTo(v.floatValue());
                        assertThat(row.getArray(24).getDouble(0)).isEqualTo(v.doubleValue());
                        assertThat(row.getArray(25).getTimestamp(0, 9).toLocalDateTime())
                                .isEqualTo(toDateTime(v));

                        assertThat(row.getArray(27).getDecimal(0, 15, 0))
                                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                        assertThat(row.getArray(28).getDecimal(0, 20, 0))
                                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                        assertThat(row.getArray(30).getDecimal(0, 15, 0))
                                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 15, 0));
                        assertThat(row.getArray(31).getDecimal(0, 20, 0))
                                .isEqualTo(Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0));
                        assertThat(row.getMap(32).valueArray().getString(0)).hasToString("" + v);
                        assertThat(row.getMap(33).valueArray().getBoolean(0)).isEqualTo(v % 2 == 0);
                        assertThat(row.getMap(34).keyArray().getString(0)).hasToString("" + v);
                        assertThat(row.getRow(35, 2).getString(0)).hasToString("" + v);
                        assertThat(row.getRow(35, 2).getInt(1)).isEqualTo(v.intValue());
                    }
                    cnt.incrementAndGet();
                });

        return cnt.get();
    }

    private InternalRow newRow(Integer v) {
        if (v == null) {
            return new GenericRow(ROW_TYPE.getFieldCount());
        }

        BinaryString str = BinaryString.fromString("" + v);

        Map<BinaryString, BinaryString> f30 = new HashMap<>();
        f30.put(str, str);

        Map<Integer, Boolean> f31 = new HashMap<>();
        f31.put(v, v % 2 == 0);

        Map<BinaryString, Integer> f32 = new HashMap<>();
        f32.put(str, v);

        Map<Timestamp, BinaryString> f34 = new HashMap<>();
        f34.put(toMicros(v), str);

        return GenericRow.of(
                str,
                v % 2 == 0,
                v.byteValue(),
                v.shortValue(),
                v,
                v.longValue(),
                v.floatValue(),
                v.doubleValue(),
                toMills(v),
                toMicros(v),
                toNanos(v),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                Decimal.fromUnscaledLong(v.longValue(), 15, 2),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 15, 0),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0),
                new GenericArray(new Object[] {str, null}),
                new GenericArray(new Object[] {v % 2 == 0, null}),
                new GenericArray(new Object[] {v.byteValue(), null}),
                new GenericArray(new Object[] {v.shortValue(), null}),
                new GenericArray(new Object[] {v, null}),
                new GenericArray(new Object[] {v.longValue(), null}),
                new GenericArray(new Object[] {v.floatValue(), null}),
                new GenericArray(new Object[] {v.doubleValue(), null}),
                new GenericArray(new Object[] {toNanos(v), null}),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArray(
                                new Object[] {
                                    Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArray(
                        new Object[] {Decimal.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null}),
                new GenericArray(
                        new Object[] {Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null}),
                Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0) == null
                        ? null
                        : new GenericArray(
                                new Object[] {
                                    Decimal.fromBigDecimal(BigDecimal.valueOf(v), 5, 0), null
                                }),
                new GenericArray(
                        new Object[] {Decimal.fromBigDecimal(BigDecimal.valueOf(v), 15, 0), null}),
                new GenericArray(
                        new Object[] {Decimal.fromBigDecimal(BigDecimal.valueOf(v), 20, 0), null}),
                new GenericMap(f30),
                new GenericMap(f31),
                new GenericMap(f32),
                GenericRow.of(str, v),
                new GenericMap(f34));
    }

    private Timestamp toMills(Integer v) {
        return Timestamp.fromEpochMillis(
                Timestamp.fromLocalDateTime(toDateTime(v)).getMillisecond());
    }

    private Timestamp toMicros(Integer v) {
        return Timestamp.fromMicros(Timestamp.fromLocalDateTime(toDateTime(v)).toMicros());
    }

    private Timestamp toNanos(Integer v) {
        return Timestamp.fromLocalDateTime(toDateTime(v));
    }

    private LocalDateTime toDateTime(Integer v) {
        v = (v > 0 ? v : -v) % 10000;
        return BASE_TIME.plusNanos(v).plusSeconds(v);
    }

    private static <T> List<T> subList(List<T> list, int i) {
        return list.subList(i, list.size());
    }

    private List<InternalRow> prepareNestedData(int rowNum) {
        return prepareNestedData(rowNum, false);
    }

    private List<InternalRow> prepareNestedData(int rowNum, boolean nullMapKey) {
        List<InternalRow> rows = new ArrayList<>(rowNum);

        for (int i = 0; i < rowNum; i++) {
            Integer v = i;
            Map<BinaryString, BinaryString> mp1 = new HashMap<>();
            mp1.put(
                    nullMapKey ? null : BinaryString.fromString("key_" + i),
                    BinaryString.fromString("val_" + i));
            Map<BinaryString, BinaryString> mp2 = new HashMap<>();
            mp2.put(BinaryString.fromString("key_" + i), null);
            mp2.put(BinaryString.fromString("key@" + i), BinaryString.fromString("val@" + i));

            rows.add(
                    GenericRow.of(
                            v,
                            new GenericArray(new Object[] {v, v + 1}),
                            new GenericArray(
                                    new Object[] {
                                        new GenericArray(new Object[] {i, i + 1, null}),
                                        new GenericArray(new Object[] {i, i + 2, null}),
                                        new GenericArray(new Object[] {}),
                                        null
                                    }),
                            new GenericArray(
                                    new GenericMap[] {
                                        null, new GenericMap(mp1), new GenericMap(mp2)
                                    }),
                            new GenericArray(
                                    new GenericRow[] {GenericRow.of(i), GenericRow.of(i + 1)}),
                            GenericRow.of(
                                    i,
                                    new GenericArray(
                                            new GenericRow[] {
                                                GenericRow.of(
                                                        new GenericArray(
                                                                new Object[] {
                                                                    new GenericArray(
                                                                            new Object[] {
                                                                                i, i + 1, null
                                                                            }),
                                                                    new GenericArray(
                                                                            new Object[] {
                                                                                i, i + 2, null
                                                                            }),
                                                                    new GenericArray(
                                                                            new Object[] {}),
                                                                    null
                                                                }),
                                                        i)
                                            })),
                            null));
        }
        return rows;
    }

    private Path createNestedDataByOriginWriter(int rowNum, File tmpDir, int rowGroupSize) {
        Path path = new Path(tmpDir.getPath(), UUID.randomUUID().toString());
        Configuration conf = new Configuration();
        conf.setInt("parquet.block.size", rowGroupSize);
        MessageType schema =
                ParquetSchemaConverter.convertToParquetMessageType(
                        "paimon-parquet", NESTED_ARRAY_MAP_TYPE);
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(
                                HadoopOutputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(path.toString()), conf))
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .withConf(new Configuration())
                        .withType(schema)
                        .build()) {
            SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
            for (int i = 0; i < rowNum; i++) {
                Group row = simpleGroupFactory.newGroup();
                // add int
                row.append("f0", i);

                // add array<int>
                Group f1 = row.addGroup("f1");
                createParquetArrayGroup(f1, i, i + 1);

                // add array<array<int>>
                Group f2 = row.addGroup("f2");
                createParquetDoubleNestedArray(f2, i);

                // add array<map>
                Group f3 = row.addGroup("f3");
                f3.addGroup(0);
                Group mapList = f3.addGroup(0);
                Group map1 = mapList.addGroup(0);
                createParquetMapGroup(map1, "key_" + i, "val_" + i);
                Group map2 = mapList.addGroup(0);
                createParquetMapGroup(map2, "key_" + i, null);
                createParquetMapGroup(map2, "key@" + i, "val@" + i);

                // add array<row>
                Group f4 = row.addGroup("f4");
                Group rowList = f4.addGroup(0);
                Group row1 = rowList.addGroup(0);
                row1.add(0, i);
                Group row2 = rowList.addGroup(0);
                row2.add(0, i + 1);
                f4.addGroup(0);

                // add ROW<`f0` INT , `f1` INTARRAY<ROW<`b` ARRAY<ARRAY<INT>>, `c` INT>>>>
                Group f5 = row.addGroup("f5");
                f5.add(0, i);
                Group arrayRow = f5.addGroup(1);
                Group insideRow = arrayRow.addGroup(0).addGroup(0);
                Group insideArray = insideRow.addGroup(0);
                createParquetDoubleNestedArray(insideArray, i);
                insideRow.add(1, i);
                arrayRow.addGroup(0);
                writer.write(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Create nested data by parquet origin writer failed.");
        }
        return path;
    }

    private void createParquetDoubleNestedArray(Group group, int i) {
        Group outside = group.addGroup(0);
        Group inside = outside.addGroup(0);
        createParquetArrayGroup(inside, i, i + 1);
        Group inside2 = outside.addGroup(0);
        createParquetArrayGroup(inside2, i, i + 2);
        // create empty array []
        outside.addGroup(0);
        // create null
        group.addGroup(0);
    }

    private void createParquetArrayGroup(Group group, int i, int j) {
        Group element = group.addGroup(0);
        element.add(0, i);
        element = group.addGroup(0);
        element.add(0, j);
        group.addGroup(0);
    }

    private void createParquetMapGroup(Group map, String key, String value) {
        Group entry = map.addGroup(0);
        entry.append("key", key);
        if (value != null) {
            entry.append("value", value);
        }
    }

    private void compareNestedRow(List<InternalRow> rows, List<InternalRow> results) {
        Assertions.assertEquals(rows.size(), results.size());

        for (InternalRow result : results) {
            int index = result.getInt(0);
            InternalRow origin = rows.get(index);
            Assertions.assertEquals(origin.getInt(0), result.getInt(0));

            // int[]
            Assertions.assertEquals(origin.getArray(1).getInt(0), result.getArray(1).getInt(0));
            Assertions.assertEquals(origin.getArray(1).getInt(1), result.getArray(1).getInt(1));

            // int[][]
            Assertions.assertEquals(
                    origin.getArray(2).getArray(0).getInt(0),
                    result.getArray(2).getArray(0).getInt(0));
            Assertions.assertEquals(
                    origin.getArray(2).getArray(0).getInt(1),
                    result.getArray(2).getArray(0).getInt(1));
            Assertions.assertTrue(result.getArray(2).getArray(0).isNullAt(2));

            Assertions.assertEquals(
                    origin.getArray(2).getArray(1).getInt(0),
                    result.getArray(2).getArray(1).getInt(0));
            Assertions.assertEquals(
                    origin.getArray(2).getArray(1).getInt(1),
                    result.getArray(2).getArray(1).getInt(1));
            Assertions.assertTrue(result.getArray(2).getArray(1).isNullAt(2));

            Assertions.assertEquals(0, result.getArray(2).getArray(2).size());
            Assertions.assertTrue(result.getArray(2).isNullAt(3));

            // map[]
            Assertions.assertTrue(result.getArray(3).isNullAt(0));
            Assertions.assertFalse(result.getArray(3).getMap(1).keyArray().isNullAt(0));

            Assertions.assertEquals(
                    origin.getArray(3).getMap(1).valueArray().getString(0),
                    result.getArray(3).getMap(1).valueArray().getString(0));

            Map<String, String> originMap = new HashMap<>();
            Map<String, String> resultMap = new HashMap<>();
            fillWithMap(originMap, origin.getArray(3).getMap(2), 0);
            fillWithMap(originMap, origin.getArray(3).getMap(2), 1);
            fillWithMap(resultMap, result.getArray(3).getMap(2), 0);
            fillWithMap(resultMap, result.getArray(3).getMap(2), 1);
            Assertions.assertEquals(originMap, resultMap);

            // row<int>[]
            Assertions.assertEquals(
                    origin.getArray(4).getRow(0, 1).getInt(0),
                    result.getArray(4).getRow(0, 1).getInt(0));
            Assertions.assertEquals(
                    origin.getArray(4).getRow(1, 1).getInt(0),
                    result.getArray(4).getRow(1, 1).getInt(0));

            Assertions.assertEquals(origin.getRow(5, 2).getInt(0), result.getRow(5, 2).getInt(0));
            Assertions.assertEquals(
                    origin.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(0).getInt(0),
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(0).getInt(0));
            Assertions.assertEquals(
                    origin.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(0).getInt(1),
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(0).getInt(1));
            Assertions.assertTrue(
                    result.getRow(5, 2)
                            .getArray(1)
                            .getRow(0, 2)
                            .getArray(0)
                            .getArray(0)
                            .isNullAt(2));

            Assertions.assertEquals(
                    origin.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(1).getInt(0),
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(1).getInt(0));
            Assertions.assertEquals(
                    origin.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(1).getInt(1),
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(1).getInt(1));
            Assertions.assertTrue(
                    result.getRow(5, 2)
                            .getArray(1)
                            .getRow(0, 2)
                            .getArray(0)
                            .getArray(1)
                            .isNullAt(2));

            Assertions.assertEquals(
                    0, result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).getArray(2).size());
            Assertions.assertTrue(
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getArray(0).isNullAt(3));

            Assertions.assertEquals(
                    origin.getRow(5, 2).getArray(1).getRow(0, 2).getInt(1),
                    result.getRow(5, 2).getArray(1).getRow(0, 2).getInt(1));
            Assertions.assertTrue(result.isNullAt(6));
            Assertions.assertTrue(result.getRow(6, 2).isNullAt(0));
            Assertions.assertTrue(result.getRow(6, 2).isNullAt(1));
            Assertions.assertTrue(result.getRow(6, 2).isNullAt(2));
        }
    }

    private void fillWithMap(Map<String, String> map, InternalMap internalMap, int index) {
        map.put(
                internalMap.keyArray().isNullAt(index)
                        ? null
                        : internalMap.keyArray().getString(index).toString(),
                internalMap.valueArray().isNullAt(index)
                        ? null
                        : internalMap.valueArray().getString(index).toString());
    }
}
