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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
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
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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
                                    .build())
                    .build();

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

        Path testPath = createTempParquetFile(folder, records, rowGroupSize);
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

        Path testPath = createTempParquetFile(folder, records, rowGroupSize);

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

        Path testPath = createTempParquetFile(folder, records, rowGroupSize);

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

    private void innerTestTypes(File folder, List<Integer> records, int rowGroupSize)
            throws IOException {
        List<InternalRow> rows = records.stream().map(this::newRow).collect(Collectors.toList());
        Path testPath = createTempParquetFile(folder, rows, rowGroupSize);
        int len = testReadingFile(subList(records, 0), testPath);
        assertThat(len).isEqualTo(records.size());
    }

    private Path createTempParquetFile(File folder, List<InternalRow> rows, int rowGroupSize)
            throws IOException {
        // write data
        Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
        Options conf = new Options();
        conf.setInteger("parquet.block.size", rowGroupSize);
        ParquetWriterFactory factory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, conf));
        String[] candidates = new String[] {"snappy", "zstd", "gzip"};
        String compress = candidates[new Random().nextInt(3)];
        FormatWriter writer =
                factory.create(new LocalFileIO().newOutputStream(path, false), compress);
        for (InternalRow row : rows) {
            writer.addElement(row);
        }

        writer.flush();
        writer.finish();
        return path;
    }

    private int testReadingFile(List<Integer> expected, Path path) throws IOException {
        ParquetReaderFactory format =
                new ParquetReaderFactory(new Options(), ROW_TYPE, 500, FilterCompat.NOOP);

        // validate java serialization
        try {
            InstantiationUtil.clone(format);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }

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
                        assertThat(row.isNullAt(0)).isTrue();
                        assertThat(row.isNullAt(1)).isTrue();
                        assertThat(row.isNullAt(2)).isTrue();
                        assertThat(row.isNullAt(3)).isTrue();
                        assertThat(row.isNullAt(4)).isTrue();
                        assertThat(row.isNullAt(5)).isTrue();
                        assertThat(row.isNullAt(6)).isTrue();
                        assertThat(row.isNullAt(7)).isTrue();
                        assertThat(row.isNullAt(8)).isTrue();
                        assertThat(row.isNullAt(9)).isTrue();
                        assertThat(row.isNullAt(10)).isTrue();
                        assertThat(row.isNullAt(11)).isTrue();
                        assertThat(row.isNullAt(12)).isTrue();
                        assertThat(row.isNullAt(13)).isTrue();
                        assertThat(row.isNullAt(14)).isTrue();
                        assertThat(row.isNullAt(15)).isTrue();
                        assertThat(row.isNullAt(16)).isTrue();
                        assertThat(row.isNullAt(17)).isTrue();
                        assertThat(row.isNullAt(18)).isTrue();
                        assertThat(row.isNullAt(19)).isTrue();
                        assertThat(row.isNullAt(20)).isTrue();
                        assertThat(row.isNullAt(21)).isTrue();
                        assertThat(row.isNullAt(22)).isTrue();
                        assertThat(row.isNullAt(23)).isTrue();
                        assertThat(row.isNullAt(24)).isTrue();
                        assertThat(row.isNullAt(25)).isTrue();
                        assertThat(row.isNullAt(26)).isTrue();
                        assertThat(row.isNullAt(27)).isTrue();
                        assertThat(row.isNullAt(28)).isTrue();
                        assertThat(row.isNullAt(29)).isTrue();
                        assertThat(row.isNullAt(30)).isTrue();
                        assertThat(row.isNullAt(31)).isTrue();
                        assertThat(row.isNullAt(32)).isTrue();
                        assertThat(row.isNullAt(33)).isTrue();
                        assertThat(row.isNullAt(34)).isTrue();
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

        Map<BinaryString, BinaryString> f30 = new HashMap<>();
        f30.put(BinaryString.fromString("" + v), BinaryString.fromString("" + v));

        Map<Integer, Boolean> f31 = new HashMap<>();
        f31.put(v, v % 2 == 0);

        Map<BinaryString, Integer> f32 = new HashMap<>();
        f32.put(BinaryString.fromString("" + v), v);

        return GenericRow.of(
                BinaryString.fromString("" + v),
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
                new GenericArray(new Object[] {BinaryString.fromString("" + v), null}),
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
                GenericRow.of(BinaryString.fromString("" + v), v));
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
}
