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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                                        fileIO, testFile, fileIO.getFileSize(testFile), null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            List<InternalRow> result = new ArrayList<>();
            while (iterator.hasNext()) {
                result.add(serializer.copy(iterator.next()));
            }
            return result;
        }
    }
}
