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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import dev.vortex.api.Expression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VortexPredicateConverter}.
 *
 * <p>Each test verifies two things: (1) the converted expression has a valid native pointer, and
 * (2) the predicate produces correct results when used in a write-read round-trip through the
 * Vortex format.
 */
public class VortexPredicateConverterTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("f_int", DataTypes.INT())
                    .field("f_bigint", DataTypes.BIGINT())
                    .field("f_string", DataTypes.STRING())
                    .build();

    private static final PredicateBuilder BUILDER = new PredicateBuilder(ROW_TYPE);

    private static void assertValidExpression(Expression expr) {
        assertNotNull(expr, "Expression should not be null");
        assertTrue(expr.nativePointer() != 0, "Expression native pointer should be non-zero");
    }

    // -- Predicate conversion + native pointer validity tests --

    @Test
    public void testEqual() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.equal(0, 42)));
        assertValidExpression(result);
    }

    @Test
    public void testNotEqual() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.notEqual(1, 100L)));
        assertValidExpression(result);
    }

    @Test
    public void testGreaterThan() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.greaterThan(0, 10)));
        assertValidExpression(result);
    }

    @Test
    public void testGreaterOrEqual() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.greaterOrEqual(0, 10)));
        assertValidExpression(result);
    }

    @Test
    public void testLessThan() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.lessThan(1, 50L)));
        assertValidExpression(result);
    }

    @Test
    public void testLessOrEqual() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.lessOrEqual(1, 50L)));
        assertValidExpression(result);
    }

    @Test
    public void testIsNull() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.isNull(0)));
        assertValidExpression(result);
    }

    @Test
    public void testIsNotNull() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(BUILDER.isNotNull(0)));
        assertValidExpression(result);
    }

    @Test
    public void testAnd() {
        Predicate and = PredicateBuilder.and(BUILDER.greaterThan(0, 10), BUILDER.lessThan(0, 100));
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(and));
        assertValidExpression(result);
    }

    @Test
    public void testOr() {
        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 1), BUILDER.equal(0, 2));
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(or));
        assertValidExpression(result);
    }

    @Test
    public void testMultiplePredicatesAsAnd() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Arrays.asList(BUILDER.greaterThan(0, 5), BUILDER.lessThan(1, 200L)));
        assertValidExpression(result);
    }

    @Test
    public void testNullPredicates() {
        assertNull(VortexPredicateConverter.toVortexExpression(null));
    }

    @Test
    public void testEmptyPredicates() {
        assertNull(VortexPredicateConverter.toVortexExpression(Collections.emptyList()));
    }

    @Test
    public void testStringLiteral() {
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                BUILDER.equal(2, BinaryString.fromString("hello"))));
        assertValidExpression(result);
    }

    @Test
    public void testDecimalLiteral() {
        RowType decRowType = RowType.builder().field("f_dec", DataTypes.DECIMAL(10, 2)).build();
        PredicateBuilder decBuilder = new PredicateBuilder(decRowType);
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                decBuilder.equal(
                                        0,
                                        Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2))));
        assertValidExpression(result);
    }

    @Test
    public void testTimestampMillisPrecision() {
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(3)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                tsBuilder.equal(0, Timestamp.fromEpochMillis(123456789L))));
        assertValidExpression(result);
    }

    @Test
    public void testTimestampMicrosPrecision() {
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(6)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                tsBuilder.equal(0, Timestamp.fromMicros(123456789123L))));
        assertValidExpression(result);
    }

    @Test
    public void testTimestampNanosPrecision() {
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(9)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                tsBuilder.equal(0, Timestamp.fromEpochMillis(123456L, 789012))));
        assertValidExpression(result);
    }

    @Test
    public void testTimestampWithLocalTimeZone() {
        RowType tsRowType =
                RowType.builder()
                        .field("f_ts_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        Expression result =
                VortexPredicateConverter.toVortexExpression(
                        Collections.singletonList(
                                tsBuilder.equal(0, Timestamp.fromEpochMillis(123456789L))));
        assertValidExpression(result);
    }

    // -- Semantic round-trip tests: write data, read with predicate, verify filtering --

    @Test
    public void testEqualSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_int == 2 should return only the row with f_int=2
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("a")),
                            GenericRow.of(2, 20L, BinaryString.fromString("b")),
                            GenericRow.of(3, 30L, BinaryString.fromString("c"))
                        },
                        Collections.singletonList(BUILDER.equal(0, 2)));
        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).getInt(0));
    }

    @Test
    public void testGreaterThanSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_int > 2 should return rows with f_int=3,4
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("a")),
                            GenericRow.of(2, 20L, BinaryString.fromString("b")),
                            GenericRow.of(3, 30L, BinaryString.fromString("c")),
                            GenericRow.of(4, 40L, BinaryString.fromString("d"))
                        },
                        Collections.singletonList(BUILDER.greaterThan(0, 2)));
        assertEquals(2, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testGreaterOrEqualSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_int >= 3 should return rows with f_int=3,4
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("a")),
                            GenericRow.of(2, 20L, BinaryString.fromString("b")),
                            GenericRow.of(3, 30L, BinaryString.fromString("c")),
                            GenericRow.of(4, 40L, BinaryString.fromString("d"))
                        },
                        Collections.singletonList(BUILDER.greaterOrEqual(0, 3)));
        assertEquals(2, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testAndSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_int > 1 AND f_int < 4 should return rows 2,3
        Predicate and = PredicateBuilder.and(BUILDER.greaterThan(0, 1), BUILDER.lessThan(0, 4));
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("a")),
                            GenericRow.of(2, 20L, BinaryString.fromString("b")),
                            GenericRow.of(3, 30L, BinaryString.fromString("c")),
                            GenericRow.of(4, 40L, BinaryString.fromString("d"))
                        },
                        Collections.singletonList(and));
        assertEquals(2, rows.size());
        assertEquals(2, rows.get(0).getInt(0));
        assertEquals(3, rows.get(1).getInt(0));
    }

    @Test
    public void testOrSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_int == 1 OR f_int == 4 should return rows 1,4
        Predicate or = PredicateBuilder.or(BUILDER.equal(0, 1), BUILDER.equal(0, 4));
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("a")),
                            GenericRow.of(2, 20L, BinaryString.fromString("b")),
                            GenericRow.of(3, 30L, BinaryString.fromString("c")),
                            GenericRow.of(4, 40L, BinaryString.fromString("d"))
                        },
                        Collections.singletonList(or));
        assertEquals(2, rows.size());
        assertEquals(1, rows.get(0).getInt(0));
        assertEquals(4, rows.get(1).getInt(0));
    }

    @Test
    public void testStringEqualSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_string == "hello"
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        ROW_TYPE,
                        new GenericRow[] {
                            GenericRow.of(1, 10L, BinaryString.fromString("hello")),
                            GenericRow.of(2, 20L, BinaryString.fromString("world")),
                        },
                        Collections.singletonList(
                                BUILDER.equal(2, BinaryString.fromString("hello"))));
        assertEquals(1, rows.size());
        assertEquals(BinaryString.fromString("hello"), rows.get(0).getString(2));
    }

    @Test
    public void testDateSemantic(@TempDir java.nio.file.Path tempDir) throws Exception {
        // f_date >= 20 (epoch day) should return rows with f_date=20,30
        RowType dateRowType = RowType.builder().field("f_date", DataTypes.DATE()).build();
        PredicateBuilder dateBuilder = new PredicateBuilder(dateRowType);
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        dateRowType,
                        new GenericRow[] {GenericRow.of(10), GenericRow.of(20), GenericRow.of(30)},
                        Collections.singletonList(dateBuilder.greaterOrEqual(0, 20)));
        assertEquals(2, rows.size());
        assertEquals(20, rows.get(0).getInt(0));
        assertEquals(30, rows.get(1).getInt(0));
    }

    @Test
    public void testTimestampSecondsPrecisionSemantic(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // f_ts >= 2000s should return rows with f_ts=2000s,3000s
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(0)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        tsRowType,
                        new GenericRow[] {
                            GenericRow.of(Timestamp.fromEpochMillis(1_000_000L)),
                            GenericRow.of(Timestamp.fromEpochMillis(2_000_000L)),
                            GenericRow.of(Timestamp.fromEpochMillis(3_000_000L))
                        },
                        Collections.singletonList(
                                tsBuilder.greaterOrEqual(
                                        0, Timestamp.fromEpochMillis(2_000_000L))));
        assertEquals(2, rows.size());
        assertEquals(2_000_000L, rows.get(0).getTimestamp(0, 0).getMillisecond());
        assertEquals(3_000_000L, rows.get(1).getTimestamp(0, 0).getMillisecond());
    }

    @Test
    public void testTimestampLtzSecondsPrecisionSemantic(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // f_ts_ltz == 2000s should return only the row with f_ts_ltz=2000s
        RowType tsRowType =
                RowType.builder()
                        .field("f_ts_ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(0))
                        .build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        tsRowType,
                        new GenericRow[] {
                            GenericRow.of(Timestamp.fromEpochMillis(1_000_000L)),
                            GenericRow.of(Timestamp.fromEpochMillis(2_000_000L)),
                            GenericRow.of(Timestamp.fromEpochMillis(3_000_000L))
                        },
                        Collections.singletonList(
                                tsBuilder.equal(0, Timestamp.fromEpochMillis(2_000_000L))));
        assertEquals(1, rows.size());
        assertEquals(2_000_000L, rows.get(0).getTimestamp(0, 0).getMillisecond());
    }

    @Test
    public void testTimestampSecondsUnrepresentableLiteralNotPushed(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        // The literal carries millisecond precision while the column is stored in seconds. No
        // rounding direction answers every operator, so the leaf is not pushed and the reader
        // returns every row for the engine to filter. Rounding down to 1s used to drop the
        // 1000ms row from "< 1500ms" and the 1000ms row from "!= 1500ms".
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(0)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        GenericRow[] data = {
            GenericRow.of(Timestamp.fromEpochMillis(1_000L)),
            GenericRow.of(Timestamp.fromEpochMillis(2_000L)),
            GenericRow.of(Timestamp.fromEpochMillis(3_000L))
        };

        assertEquals(
                3,
                roundTrip(
                                tempDir,
                                tsRowType,
                                data,
                                Collections.singletonList(
                                        tsBuilder.lessThan(0, Timestamp.fromEpochMillis(1_500L))))
                        .size());
        assertEquals(
                3,
                roundTrip(
                                tempDir,
                                tsRowType,
                                data,
                                Collections.singletonList(
                                        tsBuilder.notEqual(0, Timestamp.fromEpochMillis(1_500L))))
                        .size());
    }

    @Test
    public void testTimestampSecondsPreEpochLiteralNotPushed(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // integer division truncates toward zero, so -500ms became -0s and dropped the epoch row
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(0)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        tsRowType,
                        new GenericRow[] {GenericRow.of(Timestamp.fromEpochMillis(0L))},
                        Collections.singletonList(
                                tsBuilder.greaterThan(0, Timestamp.fromEpochMillis(-500L))));
        assertEquals(1, rows.size());
        assertEquals(0L, rows.get(0).getTimestamp(0, 0).getMillisecond());
    }

    @Test
    public void testTimestampMillisSubMillisecondLiteralNotPushed(
            @TempDir java.nio.file.Path tempDir) throws Exception {
        // a TIMESTAMP(3) column is stored in milliseconds, so the sub millisecond part of the
        // literal was silently discarded and the 1500ms row was dropped
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(3)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        tsRowType,
                        new GenericRow[] {GenericRow.of(Timestamp.fromEpochMillis(1_500L))},
                        Collections.singletonList(
                                tsBuilder.lessThan(0, Timestamp.fromMicros(1_500_500L))));
        assertEquals(1, rows.size());
        assertEquals(1_500L, rows.get(0).getTimestamp(0, 0).getMillisecond());
    }

    @Test
    public void testTimestampNanosOverflowingLiteralNotPushed(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        // epoch millis times 1_000_000 overflows int64 nanoseconds beyond 2262-04-11, and the
        // wrapped bound landed before the epoch, so "< 9999-12-31" dropped the epoch row
        RowType tsRowType = RowType.builder().field("f_ts", DataTypes.TIMESTAMP(9)).build();
        PredicateBuilder tsBuilder = new PredicateBuilder(tsRowType);
        List<Predicate> predicates =
                Collections.singletonList(
                        tsBuilder.lessThan(0, Timestamp.fromEpochMillis(253402214400000L)));
        assertNull(VortexPredicateConverter.toVortexExpression(predicates));
        List<InternalRow> rows =
                roundTrip(
                        tempDir,
                        tsRowType,
                        new GenericRow[] {GenericRow.of(Timestamp.fromEpochMillis(0L))},
                        predicates);
        assertEquals(1, rows.size());
        assertEquals(0L, rows.get(0).getTimestamp(0, 0).getMillisecond());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFloatingPointValuePredicatesNotPushed(boolean isFloat) {
        PredicateBuilder builder = new PredicateBuilder(floatingPointRowType(isFloat));
        for (Predicate predicate : floatingPointPredicates(builder, isFloat)) {
            try (Expression expression =
                    VortexPredicateConverter.toVortexExpression(
                            Collections.singletonList(predicate))) {
                assertNull(expression, predicate.toString());
            }
        }

        // Null checks do not compare floating-point values and remain safe to push down.
        for (Predicate predicate : Arrays.asList(builder.isNull(1), builder.isNotNull(1))) {
            try (Expression expression =
                    VortexPredicateConverter.toVortexExpression(
                            Collections.singletonList(predicate))) {
                assertValidExpression(expression);
            }
        }

        Predicate integer = builder.greaterThan(0, 3);
        Predicate floating = builder.greaterThan(1, floatingPointNumber(isFloat, 0.0));
        for (List<Predicate> children :
                Arrays.asList(Arrays.asList(integer, floating), Arrays.asList(floating, integer))) {
            try (Expression and =
                            VortexPredicateConverter.toVortexExpression(
                                    Collections.singletonList(PredicateBuilder.and(children)));
                    Expression or =
                            VortexPredicateConverter.toVortexExpression(
                                    Collections.singletonList(PredicateBuilder.or(children)))) {
                assertValidExpression(and);
                assertNull(or);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFloatingPointPredicatesPreserveMatchingRows(
            boolean isFloat, @TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = floatingPointRowType(isFloat);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<Object> values =
                Arrays.asList(
                        floatingPointNumber(isFloat, Double.NEGATIVE_INFINITY),
                        floatingPointNumber(isFloat, -12.5),
                        floatingPointNumber(isFloat, -1.0),
                        floatingPointNumber(isFloat, -0.0),
                        floatingPointNumber(isFloat, 0.0),
                        floatingPointNumber(isFloat, 1.0),
                        floatingPointNumber(isFloat, 12.5),
                        floatingPointNumber(isFloat, Double.POSITIVE_INFINITY),
                        floatingPointNaN(isFloat, false, 0),
                        floatingPointNaN(isFloat, false, 1),
                        floatingPointNaN(isFloat, false, 0x1234),
                        floatingPointNaN(isFloat, true, 1),
                        floatingPointNaN(isFloat, true, 0x1234),
                        null);
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        FileIO fileIO = new LocalFileIO();
        Path file = new Path(tempDir.resolve("floating_point_predicates.vortex").toUri());
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < values.size(); i++) {
                writer.addElement(GenericRow.of(i, values.get(i)));
            }
        }

        List<InternalRow> allRows =
                readFloatingPointRows(format, rowType, fileIO, file, null, isFloat);
        assertEquals(values.size(), allRows.size());
        // Paimon considers all NaN signs and payloads equal, unlike Vortex comparisons.
        assertEquals(
                new TreeSet<>(Arrays.asList(8, 9, 10, 11, 12)),
                matchingIds(allRows, builder.equal(1, floatingPointNaN(isFloat, true, 1))));

        List<Predicate> predicates = floatingPointPredicates(builder, isFloat);
        predicates.add(builder.isNull(1));
        predicates.add(builder.isNotNull(1));
        Predicate integer = builder.lessThan(0, 12);
        Predicate floating = builder.greaterThan(1, floatingPointNumber(isFloat, 0.0));
        predicates.add(integer);
        for (List<Predicate> children :
                Arrays.asList(Arrays.asList(integer, floating), Arrays.asList(floating, integer))) {
            predicates.add(PredicateBuilder.and(children));
            predicates.add(PredicateBuilder.or(children));
        }
        for (Predicate predicate : predicates) {
            List<InternalRow> candidates =
                    readFloatingPointRows(format, rowType, fileIO, file, predicate, isFloat);
            Set<Integer> expected = matchingIds(allRows, predicate);
            // The direct format reader supplies candidates; the caller applies the residual.
            assertTrue(
                    matchingIds(candidates, PredicateBuilder.alwaysTrue()).containsAll(expected),
                    predicate.toString());
            assertEquals(expected, matchingIds(candidates, predicate), predicate.toString());
        }
    }

    private static RowType floatingPointRowType(boolean isFloat) {
        return RowType.builder()
                .field("id", DataTypes.INT())
                .field("value", isFloat ? DataTypes.FLOAT() : DataTypes.DOUBLE())
                .build();
    }

    private static List<Predicate> floatingPointPredicates(
            PredicateBuilder builder, boolean isFloat) {
        List<Predicate> predicates = new ArrayList<>();
        // A finite literal is also unsafe: the column can contain a negative NaN.
        for (Object literal :
                Arrays.asList(
                        floatingPointNumber(isFloat, 0.0),
                        floatingPointNaN(isFloat, false, 0x1234),
                        floatingPointNaN(isFloat, true, 1),
                        null)) {
            predicates.add(builder.equal(1, literal));
            predicates.add(builder.notEqual(1, literal));
            predicates.add(builder.greaterThan(1, literal));
            predicates.add(builder.greaterOrEqual(1, literal));
            predicates.add(builder.lessThan(1, literal));
            predicates.add(builder.lessOrEqual(1, literal));
        }
        // Small memberships expand to comparisons; larger ones remain membership leaves.
        for (int size : new int[] {1, 20, 21}) {
            List<Object> literals = new ArrayList<>();
            literals.add(floatingPointNaN(isFloat, true, 1));
            for (int i = 1; i < size; i++) {
                literals.add(floatingPointNumber(isFloat, i));
            }
            predicates.add(builder.in(1, literals));
            predicates.add(builder.notIn(1, literals));
        }
        return predicates;
    }

    private static Object floatingPointNumber(boolean isFloat, double value) {
        if (isFloat) {
            return (float) value;
        }
        return value;
    }

    private static Object floatingPointNaN(boolean isFloat, boolean negative, int payload) {
        if (isFloat) {
            return Float.intBitsToFloat((negative ? 0xffc00000 : 0x7fc00000) | payload);
        }
        return Double.longBitsToDouble(
                (negative ? 0xfff8000000000000L : 0x7ff8000000000000L) | payload);
    }

    private static Set<Integer> matchingIds(List<InternalRow> rows, Predicate predicate) {
        Set<Integer> ids = new TreeSet<>();
        for (InternalRow row : rows) {
            if (predicate.test(row)) {
                ids.add(row.getInt(0));
            }
        }
        return ids;
    }

    private static List<InternalRow> readFloatingPointRows(
            VortexFileFormat format,
            RowType rowType,
            FileIO fileIO,
            Path file,
            Predicate predicate,
            boolean isFloat)
            throws Exception {
        List<Predicate> predicates =
                predicate == null ? Collections.emptyList() : Collections.singletonList(predicate);
        RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, predicates)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null));
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                Object value = null;
                if (!row.isNullAt(1)) {
                    // Copy directly so a binary serializer cannot normalize NaN payloads.
                    if (isFloat) {
                        value = row.getFloat(1);
                    } else {
                        value = row.getDouble(1);
                    }
                }
                rows.add(GenericRow.of(row.getInt(0), value));
            }
        }
        return rows;
    }

    private List<InternalRow> roundTrip(
            java.nio.file.Path tempDir,
            RowType rowType,
            GenericRow[] data,
            List<Predicate> predicates)
            throws Exception {
        Options options = new Options();
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));
        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(new Path(tempDir.toUri()), "predicate_test_" + System.nanoTime());

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            for (GenericRow row : data) {
                writer.addElement(row);
            }
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, predicates);
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO,
                                        testFile,
                                        fileIO.getFileSize(testFile),
                                        null,
                                        null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            List<InternalRow> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(serializer.copy(iterator.next()));
            }
            return result;
        }
    }
}
