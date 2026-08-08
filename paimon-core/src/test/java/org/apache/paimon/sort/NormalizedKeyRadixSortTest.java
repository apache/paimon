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

package org.apache.paimon.sort;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.MutableObjectIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NormalizedKeyRadixSort}. */
class NormalizedKeyRadixSortTest {

    private static final int RECORD_COUNT = 4_096;

    @TempDir Path tempDir;

    @ParameterizedTest(name = "{0}")
    @MethodSource("typeCases")
    void testMatchesComparisonSort(TypeCase typeCase) throws Exception {
        List<GenericRow> rows = rows(typeCase, RECORD_COUNT);
        assertThat(sortExternal(typeCase, rows, 32L << 20))
                .containsExactlyElementsOf(sortWithComparator(typeCase, rows));
    }

    @Test
    void testSpilledRunsMatchComparisonSort() throws Exception {
        TypeCase typeCase = stringCase();
        List<GenericRow> rows = rows(typeCase, 20_000);
        assertThat(sortExternal(typeCase, rows, 256L << 10))
                .containsExactlyElementsOf(sortWithComparator(typeCase, rows));
    }

    private static List<Integer> sortWithComparator(TypeCase typeCase, List<GenericRow> rows) {
        RowType rowType = RowType.of(typeCase.dataType, new IntType());
        List<GenericRow> expected = new ArrayList<>(rows);
        expected.sort(newRecordComparator(rowType.getFieldTypes(), new int[] {0, 1})::compare);
        List<Integer> result = new ArrayList<>(expected.size());
        expected.forEach(row -> result.add(row.getInt(1)));
        return result;
    }

    private List<Integer> sortExternal(TypeCase typeCase, List<GenericRow> rows, long memorySize)
            throws Exception {
        java.nio.file.Path ioPath = tempDir.resolve(typeCase.name + '-' + memorySize);
        java.nio.file.Files.createDirectories(ioPath);
        IOManager ioManager = IOManager.create(ioPath.toString());
        BinaryExternalSortBuffer sorter =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        RowType.of(typeCase.dataType, new IntType()),
                        new int[] {0, 1},
                        memorySize,
                        MemorySegmentPool.DEFAULT_PAGE_SIZE,
                        128,
                        CompressOptions.defaultOptions(),
                        MemorySize.MAX_VALUE);
        try {
            for (GenericRow row : rows) {
                sorter.write(row);
            }
            return collectIds(sorter.sortedIterator(), rows.size());
        } finally {
            sorter.clear();
            ioManager.close();
        }
    }

    private static List<Integer> collectIds(
            MutableObjectIterator<BinaryRow> iterator, int expectedSize) throws Exception {
        List<Integer> result = new ArrayList<>(expectedSize);
        BinaryRow reuse = new BinaryRow(2);
        while ((reuse = iterator.next(reuse)) != null) {
            result.add(reuse.getInt(1));
        }
        return result;
    }

    private static List<GenericRow> rows(TypeCase typeCase, int count) {
        List<Integer> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(i);
        }
        Collections.shuffle(ids, new Random(42));

        List<GenericRow> rows = new ArrayList<>(count);
        for (int id : ids) {
            rows.add(GenericRow.of(typeCase.value(id), id));
        }
        return rows;
    }

    private static Stream<TypeCase> typeCases() {
        return Stream.of(
                new TypeCase("boolean", new BooleanType(), i -> (i & 1) == 0),
                new TypeCase("tinyint", new TinyIntType(), i -> (byte) (i * 31)),
                new TypeCase("smallint", new SmallIntType(), i -> (short) (i * 257)),
                new TypeCase("int", new IntType(), i -> (i - 2_048) * 104_729),
                new TypeCase("bigint", new BigIntType(), i -> (i - 2_048L) * 10_000_019L),
                new TypeCase("float", new FloatType(), NormalizedKeyRadixSortTest::floatValue),
                new TypeCase("double", new DoubleType(), NormalizedKeyRadixSortTest::doubleValue),
                new TypeCase(
                        "decimal",
                        new DecimalType(18, 4),
                        i -> Decimal.fromUnscaledLong((i - 2_048L) * 100_003L, 18, 4)),
                new TypeCase("date", new DateType(), i -> i - 2_048),
                new TypeCase("time", new TimeType(3), i -> (i * 104_729) % 86_400_000),
                new TypeCase(
                        "timestamp-compact",
                        new TimestampType(3),
                        i -> Timestamp.fromEpochMillis((i - 2_048L) * 100_003L)),
                new TypeCase(
                        "timestamp",
                        new TimestampType(9),
                        i ->
                                Timestamp.fromEpochMillis(
                                        (i - 2_048L) * 100_003L, (i * 257) % 1_000_000)),
                new TypeCase(
                        "local-zoned-timestamp",
                        new LocalZonedTimestampType(9),
                        i ->
                                Timestamp.fromEpochMillis(
                                        (i - 2_048L) * 100_003L, (i * 509) % 1_000_000)),
                new TypeCase(
                        "char",
                        new CharType(32),
                        i -> BinaryString.fromString(String.format("char-prefix-%08d", i))),
                stringCase(),
                new TypeCase("binary", new BinaryType(8), NormalizedKeyRadixSortTest::binaryValue),
                new TypeCase(
                        "varbinary",
                        new VarBinaryType(16),
                        NormalizedKeyRadixSortTest::binaryValue));
    }

    private static Float floatValue(int i) {
        switch (i % 257) {
            case 0:
                return Float.NEGATIVE_INFINITY;
            case 1:
                return -0.0f;
            case 2:
                return 0.0f;
            case 3:
                return Float.POSITIVE_INFINITY;
            case 4:
                return Float.NaN;
            default:
                return (i - 2_048) / 7.0f;
        }
    }

    private static Double doubleValue(int i) {
        switch (i % 257) {
            case 0:
                return Double.NEGATIVE_INFINITY;
            case 1:
                return -0.0d;
            case 2:
                return 0.0d;
            case 3:
                return Double.POSITIVE_INFINITY;
            case 4:
                return Double.NaN;
            default:
                return (i - 2_048) / 11.0d;
        }
    }

    private static byte[] binaryValue(int i) {
        return new byte[] {
            (byte) (i * 193),
            (byte) (i >>> 1),
            (byte) 0x80,
            (byte) 0xff,
            (byte) (i >>> 24),
            (byte) (i >>> 16),
            (byte) (i >>> 8),
            (byte) i
        };
    }

    private static TypeCase stringCase() {
        return new TypeCase(
                "string",
                VarCharType.STRING_TYPE,
                i -> BinaryString.fromString(String.format("共同-prefix-%08d", i * 104_729)));
    }

    private static class TypeCase {
        private final String name;
        private final DataType dataType;
        private final IntFunction<Object> values;

        private TypeCase(String name, DataType dataType, IntFunction<Object> values) {
            this.name = name;
            this.dataType = dataType;
            this.values = values;
        }

        private Object value(int id) {
            return id % 31 == 0 ? null : values.apply(id);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
