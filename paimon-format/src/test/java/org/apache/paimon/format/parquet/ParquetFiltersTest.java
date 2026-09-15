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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.NestedFieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.dictionarylevel.DictionaryFilter;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetFiltersTest {

    @Test
    public void testBoolean() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "flag", new BooleanType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(schema, builder.isNull(0), "eq(flag, null)", true);

        test(schema, builder.isNotNull(0), "noteq(flag, null)", true);

        test(schema, builder.equal(0, true), "eq(flag, true)", true);

        test(schema, builder.notEqual(0, false), "noteq(flag, false)", true);

        test(
                schema,
                builder.in(0, Arrays.asList(true, false)),
                "or(eq(flag, true), eq(flag, false))",
                true);
    }

    @Test
    public void testLong() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "long1", new BigIntType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(schema, builder.isNull(0), "eq(long1, null)", true);

        test(schema, builder.isNotNull(0), "noteq(long1, null)", true);

        test(schema, builder.lessThan(0, 5L), "lt(long1, 5)", true);

        test(schema, builder.greaterThan(0, 5L), "gt(long1, 5)", true);

        test(
                schema,
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                "or(eq(long1, 1), or(eq(long1, 2), eq(long1, 3)))",
                true);

        test(schema, builder.between(0, 1L, 3L), "and(gteq(long1, 1), lteq(long1, 3))", true);

        test(
                schema,
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                "and(and(noteq(long1, 1), noteq(long1, 2)), noteq(long1, 3))",
                true);
    }

    @Test
    public void testBigIntPredicateIsNotPushedToDecimalInt32() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "long1", new BigIntType())));
        MessageType schema =
                new MessageType(
                        "paimon_schema",
                        Collections.singletonList(
                                Types.optional(PrimitiveTypeName.INT32)
                                        .as(LogicalTypeAnnotation.decimalType(2, 9))
                                        .named("long1")));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(schema, builder.equal(0, 12345L), "", false);
    }

    @Test
    public void testBigIntPredicateIsNotPushedToIncompatibleInt64() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "long1", new BigIntType())));
        Predicate predicate = new PredicateBuilder(rowType).equal(0, 12345L);
        List<PrimitiveType> incompatibleTypes =
                Arrays.asList(
                        Types.optional(PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.decimalType(2, 18))
                                .named("long1"),
                        Types.optional(PrimitiveTypeName.INT64)
                                .as(
                                        LogicalTypeAnnotation.timeType(
                                                true, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("long1"),
                        Types.optional(PrimitiveTypeName.INT64)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                false, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("long1"),
                        Types.optional(PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.intType(64, false))
                                .named("long1"));

        for (PrimitiveType type : incompatibleTypes) {
            FilterCompat.Filter filter =
                    ParquetFilters.convert(
                            Collections.singletonList(predicate),
                            new MessageType("paimon_schema", type),
                            true);
            assertThat(filter)
                    .as("logical type %s", type.getLogicalTypeAnnotation())
                    .isEqualTo(FilterCompat.NOOP);
        }
    }

    /**
     * INTEGER(64,true) means the same as an unannotated INT64, so the predicate is still pushed.
     */
    @Test
    public void testBigIntPredicateIsPushedToSignedAnnotatedInt64() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "long1", new BigIntType())));
        MessageType schema =
                new MessageType(
                        "paimon_schema",
                        Types.optional(PrimitiveTypeName.INT64)
                                .as(LogicalTypeAnnotation.intType(64, true))
                                .named("long1"));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(schema, builder.equal(0, 12345L), "eq(long1, 12345)", true);
    }

    @Test
    public void testString() {
        RowType rowType =
                new RowType(
                        Collections.singletonList(new DataField(0, "string1", new VarCharType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        test(schema, builder.isNull(0), "eq(string1, null)", true);

        test(schema, builder.isNotNull(0), "noteq(string1, null)", true);

        test(
                schema,
                builder.in(0, Arrays.asList("1", "2", "3")),
                "or(eq(string1, Binary{\"1\"}), or(eq(string1, Binary{\"2\"}), eq(string1, Binary{\"3\"})))",
                true);
        test(
                schema,
                builder.notIn(0, Arrays.asList("1", "2", "3")),
                "and(and(noteq(string1, Binary{\"1\"}), noteq(string1, Binary{\"2\"})), noteq(string1, Binary{\"3\"}))",
                true);
    }

    @Test
    public void testStartsWithIsPushedToParquet() {
        RowType rowType =
                new RowType(
                        Collections.singletonList(new DataField(0, "string1", new VarCharType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        FilterCompat.Filter filter =
                ParquetFilters.convert(
                        Collections.singletonList(builder.startsWith(0, "abc")), schema, true);
        assertThat(filter).isInstanceOf(FilterPredicateCompat.class);
        FilterPredicate parquetPredicate = ((FilterPredicateCompat) filter).getFilterPredicate();
        assertThat(parquetPredicate)
                .isEqualTo(
                        FilterApi.and(
                                FilterApi.gtEq(
                                        FilterApi.binaryColumn("string1"),
                                        Binary.fromString("abc")),
                                FilterApi.lt(
                                        FilterApi.binaryColumn("string1"),
                                        Binary.fromString("abd"))));
    }

    @Test
    public void testEndsWithIsNotPushedToParquet() {
        RowType rowType =
                new RowType(
                        Collections.singletonList(new DataField(0, "string1", new VarCharType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        FilterCompat.Filter filter =
                ParquetFilters.convert(
                        Collections.singletonList(builder.endsWith(0, "abc")), schema, true);
        assertThat(filter).isEqualTo(FilterCompat.NOOP);
    }

    @Test
    public void testInFilterLong() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "col1", new BigIntType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        test(
                schema,
                builder.in(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                FilterApi.in(
                        FilterApi.longColumn("col1"),
                        LongStream.range(1L, 22L).boxed().collect(Collectors.toSet())),
                true);

        test(
                schema,
                builder.notIn(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                FilterApi.notIn(
                        FilterApi.longColumn("col1"),
                        LongStream.range(1L, 22L).boxed().collect(Collectors.toSet())),
                true);
    }

    @Test
    public void testInFilterDouble() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "col1", new DoubleType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        test(
                schema,
                builder.in(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Double::new)
                                .collect(Collectors.toList())),
                FilterApi.in(
                        FilterApi.doubleColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Double::new)
                                .collect(Collectors.toSet())),
                true);

        test(
                schema,
                builder.notIn(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Double::new)
                                .collect(Collectors.toList())),
                FilterApi.notIn(
                        FilterApi.doubleColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Double::new)
                                .collect(Collectors.toSet())),
                true);
    }

    @Test
    public void testInFilterString() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "col1", new VarCharType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        test(
                schema,
                builder.in(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(String::valueOf)
                                .collect(Collectors.toList())),
                FilterApi.in(
                        FilterApi.binaryColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(s -> Binary.fromString(String.valueOf(s)))
                                .collect(Collectors.toSet())),
                true);

        test(
                schema,
                builder.notIn(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(String::valueOf)
                                .collect(Collectors.toList())),
                FilterApi.notIn(
                        FilterApi.binaryColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(s -> Binary.fromString(String.valueOf(s)))
                                .collect(Collectors.toSet())),
                true);
    }

    @Test
    public void testIsNaNDouble() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "d1", new DoubleType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate = builder.isNaN(0);
        FilterCompat.Filter filter =
                ParquetFilters.convert(Collections.singletonList(predicate), schema, true);
        FilterPredicateCompat compat = (FilterPredicateCompat) filter;
        assertThat(compat.getFilterPredicate().toString())
                .contains(
                        "userdefinedbyinstance(d1, org.apache.parquet.filter2.predicate.ParquetFilters$IsNaNDoublePredicate");
    }

    @Test
    public void testIsNaNFloat() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "f1", new FloatType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Predicate predicate = builder.isNaN(0);
        FilterCompat.Filter filter =
                ParquetFilters.convert(Collections.singletonList(predicate), schema, true);
        FilterPredicateCompat compat = (FilterPredicateCompat) filter;
        assertThat(compat.getFilterPredicate().toString())
                .contains(
                        "userdefinedbyinstance(f1, org.apache.parquet.filter2.predicate.ParquetFilters$IsNaNFloatPredicate");
    }

    @Test
    public void testInFilterFloat() {
        RowType rowType =
                new RowType(Collections.singletonList(new DataField(0, "col1", new FloatType())));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(
                schema,
                builder.in(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Float::new)
                                .collect(Collectors.toList())),
                FilterApi.in(
                        FilterApi.floatColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Float::new)
                                .collect(Collectors.toSet())),
                true);

        test(
                schema,
                builder.notIn(
                        0,
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Float::new)
                                .collect(Collectors.toList())),
                FilterApi.notIn(
                        FilterApi.floatColumn("col1"),
                        LongStream.range(1L, 22L)
                                .boxed()
                                .map(Float::new)
                                .collect(Collectors.toSet())),
                true);
    }

    @Test
    public void testDecimal32Bit() {
        // precision <= 9 uses INT32
        int precision = 9;
        int scale = 2;
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.INT32, 0, precision, scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(precision, scale)))));

        Decimal value = Decimal.fromBigDecimal(new BigDecimal("123.45"), precision, scale);
        int expectedIntVal = (int) value.toUnscaledLong(); // 12345

        test(schema, builder.isNull(0), "eq(decimal1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(schema, builder.equal(0, value), "eq(decimal1, " + expectedIntVal + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(decimal1, " + expectedIntVal + ")", true);
        test(schema, builder.lessThan(0, value), "lt(decimal1, " + expectedIntVal + ")", true);
        test(schema, builder.lessOrEqual(0, value), "lteq(decimal1, " + expectedIntVal + ")", true);
        test(schema, builder.greaterThan(0, value), "gt(decimal1, " + expectedIntVal + ")", true);
        test(
                schema,
                builder.greaterOrEqual(0, value),
                "gteq(decimal1, " + expectedIntVal + ")",
                true);
    }

    @Test
    public void testDecimal64Bit() {
        // 9 < precision <= 18 uses INT64
        int precision = 18;
        int scale = 4;
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.INT64, 0, precision, scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(precision, scale)))));

        Decimal value =
                Decimal.fromBigDecimal(new BigDecimal("12345678901234.5678"), precision, scale);
        long expectedLongVal = value.toUnscaledLong();

        test(schema, builder.isNull(0), "eq(decimal1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(schema, builder.equal(0, value), "eq(decimal1, " + expectedLongVal + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(decimal1, " + expectedLongVal + ")", true);
        test(schema, builder.lessThan(0, value), "lt(decimal1, " + expectedLongVal + ")", true);
        test(
                schema,
                builder.lessOrEqual(0, value),
                "lteq(decimal1, " + expectedLongVal + ")",
                true);
        test(schema, builder.greaterThan(0, value), "gt(decimal1, " + expectedLongVal + ")", true);
        test(
                schema,
                builder.greaterOrEqual(0, value),
                "gteq(decimal1, " + expectedLongVal + ")",
                true);
    }

    @Test
    public void testDecimalBinary() {
        // precision > 18 uses Binary
        int fieldPrecision = 20;
        int literalPrecision = 8;
        int scale = 0;
        MessageType schema =
                decimalSchema(
                        "decimal1",
                        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        9,
                        fieldPrecision,
                        scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(fieldPrecision, scale)))));

        Decimal positive =
                Decimal.fromBigDecimal(new BigDecimal("10000939"), literalPrecision, scale);
        Binary expectedPositive =
                Binary.fromConstantByteArray(
                        new byte[] {0, 0, 0, 0, 0, 0, (byte) 0x98, (byte) 0x9A, 0x2B});
        Decimal negative =
                Decimal.fromBigDecimal(new BigDecimal("-10000939"), literalPrecision, scale);
        Binary expectedNegative =
                Binary.fromConstantByteArray(
                        new byte[] {
                            (byte) 0xFF,
                            (byte) 0xFF,
                            (byte) 0xFF,
                            (byte) 0xFF,
                            (byte) 0xFF,
                            (byte) 0xFF,
                            0x67,
                            0x65,
                            (byte) 0xD5
                        });

        test(schema, builder.isNull(0), "eq(decimal1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(
                schema,
                builder.equal(0, positive),
                FilterApi.eq(FilterApi.binaryColumn("decimal1"), expectedPositive),
                true);
        test(
                schema,
                builder.notEqual(0, positive),
                FilterApi.notEq(FilterApi.binaryColumn("decimal1"), expectedPositive),
                true);
        test(
                schema,
                builder.lessThan(0, positive),
                FilterApi.lt(FilterApi.binaryColumn("decimal1"), expectedPositive),
                true);
        test(
                schema,
                builder.greaterThan(0, positive),
                FilterApi.gt(FilterApi.binaryColumn("decimal1"), expectedPositive),
                true);
        test(
                schema,
                builder.equal(0, negative),
                FilterApi.eq(FilterApi.binaryColumn("decimal1"), expectedNegative),
                true);

        Decimal fullWidth =
                Decimal.fromBigDecimal(
                        new BigDecimal("99999999999999999999"), fieldPrecision, scale);
        assertThat(fullWidth.toUnscaledBytes()).hasSize(9);
        test(
                schema,
                builder.equal(0, fullWidth),
                FilterApi.eq(
                        FilterApi.binaryColumn("decimal1"),
                        Binary.fromConstantByteArray(fullWidth.toUnscaledBytes())),
                true);
    }

    @Test
    public void testDecimalBinaryMaxPrecision() {
        int precision = 38;
        int scale = 10;
        MessageType schema =
                decimalSchema(
                        "decimal1", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, precision, scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(precision, scale)))));
        Decimal value =
                Decimal.fromBigDecimal(
                        new BigDecimal("12345678901234567890.1234567890"), precision, scale);
        Binary expected =
                Binary.fromConstantByteArray(
                        new byte[] {
                            0,
                            0,
                            0,
                            1,
                            (byte) 0x8E,
                            (byte) 0xE9,
                            0x0F,
                            (byte) 0xF6,
                            (byte) 0xC3,
                            0x73,
                            (byte) 0xE0,
                            (byte) 0xEE,
                            0x4E,
                            0x3F,
                            0x0A,
                            (byte) 0xD2
                        });

        test(
                schema,
                builder.equal(0, value),
                FilterApi.eq(FilterApi.binaryColumn("decimal1"), expected),
                true);
    }

    @Test
    public void testInFilterDecimalBinary() {
        int fieldPrecision = 20;
        int scale = 0;
        MessageType schema =
                decimalSchema(
                        "decimal1",
                        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        9,
                        fieldPrecision,
                        scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(fieldPrecision, scale)))));

        List<Object> literals =
                IntStream.rangeClosed(1, 21)
                        .mapToObj(
                                value ->
                                        (Object)
                                                Decimal.fromBigDecimal(
                                                        BigDecimal.valueOf(value),
                                                        fieldPrecision,
                                                        scale))
                        .collect(Collectors.toList());
        Set<Binary> expected =
                IntStream.rangeClosed(1, 21)
                        .mapToObj(
                                value -> {
                                    byte[] bytes = new byte[9];
                                    bytes[8] = (byte) value;
                                    return Binary.fromConstantByteArray(bytes);
                                })
                        .collect(Collectors.toSet());

        test(
                schema,
                builder.in(0, literals),
                FilterApi.in(FilterApi.binaryColumn("decimal1"), expected),
                true);
        test(
                schema,
                builder.notIn(0, literals),
                FilterApi.notIn(FilterApi.binaryColumn("decimal1"), expected),
                true);
    }

    @Test
    public void testDecimalDictionaryFilter() {
        int fieldPrecision = 20;
        int scale = 0;
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(fieldPrecision, scale)))));
        Decimal positive =
                Decimal.fromBigDecimal(new BigDecimal("10000939"), fieldPrecision, scale);
        Decimal negative =
                Decimal.fromBigDecimal(new BigDecimal("-10000939"), fieldPrecision, scale);
        Decimal missing = Decimal.fromBigDecimal(new BigDecimal("10000940"), fieldPrecision, scale);

        byte[] positiveBytes = new byte[] {0, 0, 0, 0, 0, 0, (byte) 0x98, (byte) 0x9A, 0x2B};
        byte[] negativeBytes =
                new byte[] {
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    (byte) 0xFF,
                    0x67,
                    0x65,
                    (byte) 0xD5
                };
        DictionaryPage dictionaryPage =
                new DictionaryPage(
                        BytesInput.concat(
                                BytesInput.from(positiveBytes), BytesInput.from(negativeBytes)),
                        2,
                        Encoding.PLAIN);
        DictionaryPageReadStore dictionaries =
                new DictionaryPageReadStore() {
                    @Override
                    public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
                        return dictionaryPage;
                    }

                    @Override
                    public void close() {}
                };

        PrimitiveType primitiveType =
                Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(9)
                        .as(LogicalTypeAnnotation.decimalType(scale, fieldPrecision))
                        .named("decimal1");
        MessageType schema =
                new MessageType("paimon_schema", Collections.singletonList(primitiveType));
        EncodingStats encodingStats =
                new EncodingStats.Builder()
                        .addDictEncoding(Encoding.PLAIN)
                        .addDataEncoding(Encoding.RLE_DICTIONARY)
                        .build();
        Set<Encoding> encodings =
                new HashSet<>(Arrays.asList(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.RLE));
        ColumnChunkMetaData metadata =
                ColumnChunkMetaData.get(
                        ColumnPath.get("decimal1"),
                        primitiveType,
                        CompressionCodecName.UNCOMPRESSED,
                        encodingStats,
                        encodings,
                        null,
                        0,
                        0,
                        2,
                        0,
                        0);

        assertThat(
                        DictionaryFilter.canDrop(
                                convert(schema, builder.equal(0, positive)),
                                Collections.singletonList(metadata),
                                dictionaries))
                .isFalse();
        assertThat(
                        DictionaryFilter.canDrop(
                                convert(schema, builder.equal(0, negative)),
                                Collections.singletonList(metadata),
                                dictionaries))
                .isFalse();
        assertThat(
                        DictionaryFilter.canDrop(
                                convert(schema, builder.equal(0, missing)),
                                Collections.singletonList(metadata),
                                dictionaries))
                .isTrue();
    }

    @Test
    public void testDecimalLiteralOutsideFieldDomain() {
        int fieldPrecision = 9;
        int scale = 0;
        PredicateBuilder builder = decimalPredicateBuilder(fieldPrecision, scale);
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("4294967297"), 10, scale);
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.INT32, 0, fieldPrecision, scale);

        test(schema, builder.equal(0, value), "", false);
    }

    @Test
    public void testDecimalLiteralWiderThanPhysicalWidth() {
        int fieldPrecision = 20;
        int scale = 0;
        PredicateBuilder builder = decimalPredicateBuilder(fieldPrecision, scale);
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("3000000000"), 10, scale);
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4, 9, scale);

        test(schema, builder.equal(0, value), "", false);
    }

    @Test
    public void testDecimalLiteralWiderThanFieldDomain() {
        int fieldPrecision = 20;
        int scale = 0;
        PredicateBuilder builder = decimalPredicateBuilder(fieldPrecision, scale);
        Decimal value =
                Decimal.fromBigDecimal(
                        new BigDecimal("99999999999999999999999999999999999999"), 38, scale);
        MessageType schema =
                decimalSchema(
                        "decimal1",
                        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        9,
                        fieldPrecision,
                        scale);

        test(schema, builder.equal(0, value), "", false);
    }

    @Test
    public void testDecimalScaleNormalization() {
        int fieldPrecision = 20;
        int fieldScale = 2;
        PredicateBuilder builder = decimalPredicateBuilder(fieldPrecision, fieldScale);
        MessageType schema =
                decimalSchema(
                        "decimal1",
                        PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                        9,
                        fieldPrecision,
                        fieldScale);
        Decimal exact = Decimal.fromBigDecimal(new BigDecimal("100"), 3, 0);
        Decimal inexact = Decimal.fromBigDecimal(new BigDecimal("100.001"), 6, 3);
        Binary expected =
                Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 0, 0, 0x27, 0x10});

        test(
                schema,
                builder.equal(0, exact),
                FilterApi.eq(FilterApi.binaryColumn("decimal1"), expected),
                true);
        test(schema, builder.equal(0, inexact), "", false);
    }

    @Test
    public void testDecimalPhysicalTypes() {
        int precision = 9;
        int scale = 2;
        PredicateBuilder builder = decimalPredicateBuilder(precision, scale);
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("12.34"), precision, scale);

        test(
                decimalSchema("decimal1", PrimitiveTypeName.INT32, 0, precision, scale),
                builder.equal(0, value),
                FilterApi.eq(FilterApi.intColumn("decimal1"), 1234),
                true);
        test(
                decimalSchema("decimal1", PrimitiveTypeName.INT64, 0, precision, scale),
                builder.equal(0, value),
                FilterApi.eq(FilterApi.longColumn("decimal1"), 1234L),
                true);
        test(
                decimalSchema("decimal1", PrimitiveTypeName.BINARY, 0, precision, scale),
                builder.equal(0, value),
                FilterApi.eq(
                        FilterApi.binaryColumn("decimal1"),
                        Binary.fromConstantByteArray(new byte[] {0x04, (byte) 0xD2})),
                true);
        test(
                decimalSchema(
                        "decimal1", PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 4, precision, scale),
                builder.equal(0, value),
                FilterApi.eq(
                        FilterApi.binaryColumn("decimal1"),
                        Binary.fromConstantByteArray(new byte[] {0, 0, 0x04, (byte) 0xD2})),
                true);
    }

    @Test
    public void testInFilterDecimal32Bit() {
        int precision = 9;
        int scale = 2;
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.INT32, 0, precision, scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(precision, scale)))));

        Decimal v1 = Decimal.fromBigDecimal(new BigDecimal("100.00"), precision, scale);
        Decimal v2 = Decimal.fromBigDecimal(new BigDecimal("200.00"), precision, scale);
        Decimal v3 = Decimal.fromBigDecimal(new BigDecimal("300.00"), precision, scale);

        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(eq(decimal1, "
                        + (int) v1.toUnscaledLong()
                        + "), or(eq(decimal1, "
                        + (int) v2.toUnscaledLong()
                        + "), eq(decimal1, "
                        + (int) v3.toUnscaledLong()
                        + ")))",
                true);

        test(
                schema,
                builder.notIn(0, Arrays.asList(v1, v2, v3)),
                "and(and(noteq(decimal1, "
                        + (int) v1.toUnscaledLong()
                        + "), noteq(decimal1, "
                        + (int) v2.toUnscaledLong()
                        + ")), noteq(decimal1, "
                        + (int) v3.toUnscaledLong()
                        + "))",
                true);
    }

    @Test
    public void testInFilterDecimal64Bit() {
        int precision = 18;
        int scale = 4;
        MessageType schema =
                decimalSchema("decimal1", PrimitiveTypeName.INT64, 0, precision, scale);
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "decimal1",
                                                new DecimalType(precision, scale)))));

        Decimal v1 = Decimal.fromBigDecimal(new BigDecimal("10000000000.0000"), precision, scale);
        Decimal v2 = Decimal.fromBigDecimal(new BigDecimal("20000000000.0000"), precision, scale);
        Decimal v3 = Decimal.fromBigDecimal(new BigDecimal("30000000000.0000"), precision, scale);

        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(eq(decimal1, "
                        + v1.toUnscaledLong()
                        + "), or(eq(decimal1, "
                        + v2.toUnscaledLong()
                        + "), eq(decimal1, "
                        + v3.toUnscaledLong()
                        + ")))",
                true);

        test(
                schema,
                builder.notIn(0, Arrays.asList(v1, v2, v3)),
                "and(and(noteq(decimal1, "
                        + v1.toUnscaledLong()
                        + "), noteq(decimal1, "
                        + v2.toUnscaledLong()
                        + ")), noteq(decimal1, "
                        + v3.toUnscaledLong()
                        + "))",
                true);
    }

    @Test
    public void testTimestampMillis() {
        // precision <= 3 uses milliseconds (INT64)
        int precision = 3;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new TimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp value = Timestamp.fromEpochMillis(1704067200000L); // 2024-01-01 00:00:00
        long expectedMillis = value.getMillisecond();

        test(schema, builder.isNull(0), "eq(ts1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(ts1, null)", true);
        test(schema, builder.equal(0, value), "eq(ts1, " + expectedMillis + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(ts1, " + expectedMillis + ")", true);
        test(schema, builder.lessThan(0, value), "lt(ts1, " + expectedMillis + ")", true);
        test(schema, builder.lessOrEqual(0, value), "lteq(ts1, " + expectedMillis + ")", true);
        test(schema, builder.greaterThan(0, value), "gt(ts1, " + expectedMillis + ")", true);
        test(schema, builder.greaterOrEqual(0, value), "gteq(ts1, " + expectedMillis + ")", true);
    }

    @Test
    public void testTimestampMicros() {
        // 3 < precision <= 6 uses microseconds (INT64)
        int precision = 6;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new TimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp value = Timestamp.fromEpochMillis(1704067200123L, 456000); // with nanos
        long expectedMicros = value.getMillisecond() * 1000 + value.getNanoOfMillisecond() / 1000;

        test(schema, builder.isNull(0), "eq(ts1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(ts1, null)", true);
        test(schema, builder.equal(0, value), "eq(ts1, " + expectedMicros + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(ts1, " + expectedMicros + ")", true);
        test(schema, builder.lessThan(0, value), "lt(ts1, " + expectedMicros + ")", true);
        test(schema, builder.lessOrEqual(0, value), "lteq(ts1, " + expectedMicros + ")", true);
        test(schema, builder.greaterThan(0, value), "gt(ts1, " + expectedMicros + ")", true);
        test(schema, builder.greaterOrEqual(0, value), "gteq(ts1, " + expectedMicros + ")", true);
    }

    @Test
    public void testTimestampFileUnitMismatchCannotPushDown() {
        Timestamp value = Timestamp.fromEpochMillis(1704067200123L, 456000);

        RowType millisReadType =
                new RowType(
                        Collections.singletonList(new DataField(0, "ts1", new TimestampType(3))));
        MessageType microsFileSchema =
                new MessageType(
                        "paimon_schema",
                        Types.required(PrimitiveTypeName.INT64)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                false, LogicalTypeAnnotation.TimeUnit.MICROS))
                                .named("ts1"));
        test(microsFileSchema, new PredicateBuilder(millisReadType).equal(0, value), "", false);

        RowType microsReadType =
                new RowType(
                        Collections.singletonList(new DataField(0, "ts1", new TimestampType(6))));
        MessageType millisFileSchema =
                new MessageType(
                        "paimon_schema",
                        Types.required(PrimitiveTypeName.INT64)
                                .as(
                                        LogicalTypeAnnotation.timestampType(
                                                false, LogicalTypeAnnotation.TimeUnit.MILLIS))
                                .named("ts1"));
        test(millisFileSchema, new PredicateBuilder(microsReadType).equal(0, value), "", false);
    }

    @Test
    public void testLocalZonedTimestampMillis() {
        // precision <= 3 uses milliseconds (INT64)
        int precision = 3;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new LocalZonedTimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp value = Timestamp.fromEpochMillis(1704067200000L);
        long expectedMillis = value.getMillisecond();

        test(schema, builder.isNull(0), "eq(ts1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(ts1, null)", true);
        test(schema, builder.equal(0, value), "eq(ts1, " + expectedMillis + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(ts1, " + expectedMillis + ")", true);
        test(schema, builder.lessThan(0, value), "lt(ts1, " + expectedMillis + ")", true);
        test(schema, builder.greaterThan(0, value), "gt(ts1, " + expectedMillis + ")", true);
    }

    @Test
    public void testLocalZonedTimestampMicros() {
        // 3 < precision <= 6 uses microseconds (INT64)
        int precision = 6;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new LocalZonedTimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp value = Timestamp.fromEpochMillis(1704067200123L, 456000);
        long expectedMicros = value.getMillisecond() * 1000 + value.getNanoOfMillisecond() / 1000;

        test(schema, builder.isNull(0), "eq(ts1, null)", true);
        test(schema, builder.isNotNull(0), "noteq(ts1, null)", true);
        test(schema, builder.equal(0, value), "eq(ts1, " + expectedMicros + ")", true);
        test(schema, builder.notEqual(0, value), "noteq(ts1, " + expectedMicros + ")", true);
        test(schema, builder.lessThan(0, value), "lt(ts1, " + expectedMicros + ")", true);
        test(schema, builder.greaterThan(0, value), "gt(ts1, " + expectedMicros + ")", true);
    }

    @Test
    public void testInFilterTimestampMillis() {
        int precision = 3;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new TimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp v1 = Timestamp.fromEpochMillis(1704067200000L);
        Timestamp v2 = Timestamp.fromEpochMillis(1704153600000L);
        Timestamp v3 = Timestamp.fromEpochMillis(1704240000000L);

        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(eq(ts1, "
                        + v1.getMillisecond()
                        + "), or(eq(ts1, "
                        + v2.getMillisecond()
                        + "), eq(ts1, "
                        + v3.getMillisecond()
                        + ")))",
                true);

        test(
                schema,
                builder.notIn(0, Arrays.asList(v1, v2, v3)),
                "and(and(noteq(ts1, "
                        + v1.getMillisecond()
                        + "), noteq(ts1, "
                        + v2.getMillisecond()
                        + ")), noteq(ts1, "
                        + v3.getMillisecond()
                        + "))",
                true);
    }

    @Test
    public void testInFilterTimestampMicros() {
        int precision = 6;
        RowType rowType =
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "ts1", new TimestampType(precision))));
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        Timestamp v1 = Timestamp.fromEpochMillis(1704067200000L, 123000);
        Timestamp v2 = Timestamp.fromEpochMillis(1704153600000L, 456000);
        Timestamp v3 = Timestamp.fromEpochMillis(1704240000000L, 789000);

        long micros1 = v1.getMillisecond() * 1000 + v1.getNanoOfMillisecond() / 1000;
        long micros2 = v2.getMillisecond() * 1000 + v2.getNanoOfMillisecond() / 1000;
        long micros3 = v3.getMillisecond() * 1000 + v3.getNanoOfMillisecond() / 1000;

        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(eq(ts1, "
                        + micros1
                        + "), or(eq(ts1, "
                        + micros2
                        + "), eq(ts1, "
                        + micros3
                        + ")))",
                true);

        test(
                schema,
                builder.notIn(0, Arrays.asList(v1, v2, v3)),
                "and(and(noteq(ts1, "
                        + micros1
                        + "), noteq(ts1, "
                        + micros2
                        + ")), noteq(ts1, "
                        + micros3
                        + "))",
                true);
    }

    private void test(
            MessageType schema,
            Predicate predicate,
            FilterPredicate parquetPredicate,
            boolean canPushDown) {
        FilterCompat.Filter filter =
                ParquetFilters.convert(PredicateBuilder.splitAnd(predicate), schema, true);
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate()).isEqualTo(parquetPredicate);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
    }

    // ---------------------------------------------------------------------------------------
    // nested fields
    // ---------------------------------------------------------------------------------------

    private static final RowType ADDR_TYPE =
            RowType.of(
                    new DataType[] {new VarCharType(), new VarCharType()},
                    new String[] {"city", "zip"});

    private static RowType nestedRowType() {
        return RowType.of(
                new DataType[] {
                    new BigIntType(),
                    RowType.of(
                            new DataType[] {new BigIntType(), ADDR_TYPE},
                            new String[] {"id", "addr"}),
                    new ArrayType(ADDR_TYPE)
                },
                new String[] {"pk", "user", "addrs"});
    }

    private static Predicate nestedPredicate(RowType rowType, String column, String... path) {
        DataField field = rowType.getFields().get(rowType.getFieldIndex(column));
        FieldRef ref = new FieldRef(rowType.getFieldIndex(column), field.name(), field.type());
        return new PredicateBuilder(rowType)
                .equal(
                        new NestedFieldTransform(ref, Arrays.asList(path)),
                        BinaryString.fromString("Beijing"));
    }

    @Test
    public void testNestedField() {
        RowType rowType = nestedRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);

        // user.addr.city
        test(
                schema,
                nestedPredicate(rowType, "user", "addr", "city"),
                "eq(user.addr.city, Binary{\"Beijing\"})",
                true);
    }

    /** A nested field is dispatched through the same visitors as a top-level one. */
    @Test
    public void testNestedFieldSupportsEveryPushableFunction() {
        RowType rowType = nestedRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        // user.id, a BIGINT one level down
        FieldRef ref = new FieldRef(1, "user", rowType.getTypeAt(1));
        NestedFieldTransform id = new NestedFieldTransform(ref, Collections.singletonList("id"));
        PredicateBuilder builder = new PredicateBuilder(rowType);

        test(schema, builder.isNull(id), "eq(user.id, null)", true);
        test(schema, builder.isNotNull(id), "noteq(user.id, null)", true);
        test(schema, builder.equal(id, 5L), "eq(user.id, 5)", true);
        test(schema, builder.notEqual(id, 5L), "noteq(user.id, 5)", true);
        test(schema, builder.lessThan(id, 5L), "lt(user.id, 5)", true);
        test(schema, builder.lessOrEqual(id, 5L), "lteq(user.id, 5)", true);
        test(schema, builder.greaterThan(id, 5L), "gt(user.id, 5)", true);
        test(schema, builder.greaterOrEqual(id, 5L), "gteq(user.id, 5)", true);
        test(schema, builder.between(id, 1L, 3L), "and(gteq(user.id, 1), lteq(user.id, 3))", true);
        test(
                schema,
                builder.in(id, Arrays.asList(1L, 2L)),
                "or(eq(user.id, 1), eq(user.id, 2))",
                true);
        test(
                schema,
                builder.notIn(id, Arrays.asList(1L, 2L)),
                "and(noteq(user.id, 1), noteq(user.id, 2))",
                true);

        // AND/OR mixing a nested field with a top-level one
        test(
                schema,
                PredicateBuilder.and(builder.greaterThan(id, 5L), builder.lessThan(0, 100L)),
                "and(gt(user.id, 5), lt(pk, 100))",
                true);

        // string functions have no parquet equivalent, for nested and top-level alike
        test(schema, builder.startsWith(id, BinaryString.fromString("x")), (String) null, false);
    }

    /**
     * A field under a repeated group has no single value per row, and parquet-mr rejects a
     * predicate on one outright. A table declaring a struct over a file that repeats it - a format
     * table reading files someone else wrote - must give up rather than hand one over.
     */
    @Test
    public void testNestedFieldUnderRepeatedGroupIsNotPushedDown() {
        RowType rowType = nestedRowType();
        MessageType schema =
                new MessageType(
                        "paimon_schema",
                        Types.repeatedGroup()
                                .addField(Types.required(PrimitiveTypeName.INT64).named("id"))
                                .addField(
                                        Types.requiredGroup()
                                                .addField(
                                                        Types.required(PrimitiveTypeName.BINARY)
                                                                .as(
                                                                        LogicalTypeAnnotation
                                                                                .stringType())
                                                                .named("city"))
                                                .named("addr"))
                                .named("user"));

        test(schema, nestedPredicate(rowType, "user", "addr", "city"), (String) null, false);
    }

    /** A nested column the file does not hold still prunes: parquet-mr reads it as all-null. */
    @Test
    public void testNestedFieldMissingFromFile() {
        RowType rowType = nestedRowType();
        MessageType schema =
                ParquetSchemaConverter.convertToParquetMessageType(
                        RowType.of(new DataType[] {new BigIntType()}, new String[] {"pk"}));

        test(
                schema,
                nestedPredicate(rowType, "user", "addr", "city"),
                "eq(user.addr.city, Binary{\"Beijing\"})",
                true);
    }

    private static RowType payloadRowType() {
        return RowType.of(
                new DataType[] {
                    new BigIntType(),
                    RowType.of(
                            new DataType[] {
                                new DecimalType(10, 2),
                                new TimestampType(3),
                                new LocalZonedTimestampType(3),
                                new BigIntType()
                            },
                            new String[] {"amount", "ts", "ltz", "qty"}),
                    new DecimalType(10, 2)
                },
                new String[] {"pk", "payload", "amt_top"});
    }

    private static NestedFieldTransform payloadLeaf(RowType rowType, String leaf) {
        RowType payload = (RowType) rowType.getTypeAt(1);
        return new NestedFieldTransform(
                new FieldRef(1, "payload", payload), Collections.singletonList(leaf));
    }

    /** Control: the two shapes that already worked must keep working. */
    @Test
    public void testTopLevelDecimalAndNestedBigIntAreUnaffected() {
        RowType rowType = payloadRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Decimal amount = Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2);

        test(schema, builder.equal(2, amount), "eq(amt_top, 1234)", true);
        test(
                schema,
                builder.greaterThan(payloadLeaf(rowType, "qty"), 5L),
                "gt(payload.qty, 5)",
                true);
    }

    /**
     * A nested DECIMAL must be filtered on its full path. The physical type is resolved by walking
     * the path, but the column handed to parquet-mr used to be rebuilt from the leaf {@code
     * PrimitiveType}, which only knows its own name — parquet-mr then saw a missing top-level
     * column and could drop every row group.
     */
    @Test
    public void testNestedDecimalKeepsTheFullPath() {
        RowType rowType = payloadRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        NestedFieldTransform amount = payloadLeaf(rowType, "amount");
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2);

        test(schema, builder.equal(amount, value), "eq(payload.amount, 1234)", true);
        test(schema, builder.lessThan(amount, value), "lt(payload.amount, 1234)", true);
    }

    /** Same as {@link #testNestedDecimalKeepsTheFullPath()} for TIMESTAMP. */
    @Test
    public void testNestedTimestampKeepsTheFullPath() {
        RowType rowType = payloadRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        NestedFieldTransform ts = payloadLeaf(rowType, "ts");
        Timestamp value = Timestamp.fromEpochMillis(1704067200000L);
        long millis = value.getMillisecond();

        test(schema, builder.equal(ts, value), "eq(payload.ts, " + millis + ")", true);
        test(schema, builder.greaterThan(ts, value), "gt(payload.ts, " + millis + ")", true);
    }

    /** Same as {@link #testNestedDecimalKeepsTheFullPath()} for LOCAL ZONED TIMESTAMP. */
    @Test
    public void testNestedLocalZonedTimestampKeepsTheFullPath() {
        RowType rowType = payloadRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        NestedFieldTransform ltz = payloadLeaf(rowType, "ltz");
        Timestamp value = Timestamp.fromEpochMillis(1704067200000L);
        long millis = value.getMillisecond();

        test(schema, builder.equal(ltz, value), "eq(payload.ltz, " + millis + ")", true);
    }

    /**
     * A nested decimal schema with an explicit physical type, the way a Format Table may hold it.
     */
    private static MessageType nestedDecimalSchema(
            PrimitiveTypeName physicalType, int fixedLength, int precision, int scale) {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(physicalType);
        if (physicalType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            builder.length(fixedLength);
        }
        PrimitiveType amount =
                builder.as(LogicalTypeAnnotation.decimalType(scale, precision)).named("amount");
        return new MessageType(
                "paimon_schema",
                Arrays.asList(
                        Types.optional(PrimitiveTypeName.INT64).named("pk"),
                        Types.optionalGroup().addField(amount).named("payload")));
    }

    private void testNestedDecimalPhysicalType(
            PrimitiveTypeName physicalType, int fixedLength, int precision, String literal) {
        int scale = 2;
        RowType payload =
                RowType.of(
                        new DataType[] {new DecimalType(precision, scale)},
                        new String[] {"amount"});
        RowType rowType =
                RowType.of(
                        new DataType[] {new BigIntType(), payload}, new String[] {"pk", "payload"});
        MessageType schema = nestedDecimalSchema(physicalType, fixedLength, precision, scale);
        NestedFieldTransform amount =
                new NestedFieldTransform(
                        new FieldRef(1, "payload", payload), Collections.singletonList("amount"));
        Decimal value = Decimal.fromBigDecimal(new BigDecimal(literal), precision, scale);

        FilterPredicate filter =
                convert(schema, new PredicateBuilder(rowType).equal(amount, value));
        // whatever the physical type, the column must be the full path
        assertThat(filter.toString()).startsWith("eq(payload.amount, ");
    }

    /** The decimal visitor builds a column per physical type; every branch must keep the path. */
    @Test
    public void testNestedDecimalKeepsTheFullPathForEveryPhysicalType() {
        // precision <= 9 -> INT32
        testNestedDecimalPhysicalType(PrimitiveTypeName.INT32, 0, 8, "12.34");
        // precision <= 18 -> INT64
        testNestedDecimalPhysicalType(PrimitiveTypeName.INT64, 0, 15, "12.34");
        // larger -> FIXED_LEN_BYTE_ARRAY
        testNestedDecimalPhysicalType(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, 30, "12.34");
        // a Format Table may hold a decimal as BINARY
        testNestedDecimalPhysicalType(PrimitiveTypeName.BINARY, 0, 30, "12.34");
    }

    /** Micros-precision timestamps take a different literal path than millis. */
    @Test
    public void testNestedTimestampMicrosKeepsTheFullPath() {
        RowType payload =
                RowType.of(
                        new DataType[] {new TimestampType(6), new LocalZonedTimestampType(6)},
                        new String[] {"ts", "ltz"});
        RowType rowType =
                RowType.of(
                        new DataType[] {new BigIntType(), payload}, new String[] {"pk", "payload"});
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        FieldRef payloadRef = new FieldRef(1, "payload", payload);
        Timestamp value = Timestamp.fromEpochMillis(1704067200000L);
        long micros = value.toMicros();

        test(
                schema,
                builder.equal(
                        new NestedFieldTransform(payloadRef, Collections.singletonList("ts")),
                        value),
                "eq(payload.ts, " + micros + ")",
                true);
        test(
                schema,
                builder.greaterThan(
                        new NestedFieldTransform(payloadRef, Collections.singletonList("ltz")),
                        value),
                "gt(payload.ltz, " + micros + ")",
                true);
    }

    /** IN and NOT IN build the column through the same visitor; a nested decimal must keep it. */
    @Test
    public void testNestedDecimalInAndNotInKeepTheFullPath() {
        RowType rowType = payloadRowType();
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);
        NestedFieldTransform amount = payloadLeaf(rowType, "amount");
        Decimal one = Decimal.fromBigDecimal(new BigDecimal("1.00"), 10, 2);
        Decimal two = Decimal.fromBigDecimal(new BigDecimal("2.00"), 10, 2);

        test(
                schema,
                builder.in(amount, Arrays.asList(one, two)),
                "or(eq(payload.amount, 100), eq(payload.amount, 200))",
                true);
        test(
                schema,
                builder.notIn(amount, Arrays.asList(one, two)),
                "and(noteq(payload.amount, 100), noteq(payload.amount, 200))",
                true);
    }

    /** The path walk is recursive; three levels must resolve as well as two. */
    @Test
    public void testDeeplyNestedFieldKeepsTheFullPath() {
        RowType level3 = RowType.of(new DataType[] {new BigIntType()}, new String[] {"d"});
        RowType level2 = RowType.of(new DataType[] {level3}, new String[] {"c"});
        RowType level1 = RowType.of(new DataType[] {level2}, new String[] {"b"});
        RowType rowType =
                RowType.of(new DataType[] {new BigIntType(), level1}, new String[] {"pk", "a"});
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);

        NestedFieldTransform deep =
                new NestedFieldTransform(
                        new FieldRef(1, "a", level1), Arrays.asList("b", "c", "d"));
        test(schema, new PredicateBuilder(rowType).equal(deep, 7L), "eq(a.b.c.d, 7)", true);
    }

    /**
     * A nested component whose own name contains a dot cannot be expressed as a dot-joined path:
     * parquet-mr would split {@code s.a.b} into three components and miss the real two-component
     * column, treating it as all-null and pruning matching row groups. Refuse the pushdown.
     */
    @Test
    public void testNestedComponentContainingADotIsNotPushedDown() {
        RowType inner = RowType.of(new DataType[] {new BigIntType()}, new String[] {"a.b"});
        RowType rowType =
                RowType.of(new DataType[] {new BigIntType(), inner}, new String[] {"pk", "s"});
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);
        PredicateBuilder builder = new PredicateBuilder(rowType);

        // the file really holds s -> "a.b"; a dot-joined "s.a.b" does not address it
        assertThat(schema.getType("s").asGroupType().containsField("a.b")).isTrue();

        NestedFieldTransform dotted =
                new NestedFieldTransform(
                        new FieldRef(1, "s", inner), Collections.singletonList("a.b"));
        test(schema, builder.equal(dotted, 7L), (String) null, false);
    }

    /**
     * The dot may also sit in the top-level column's own name. The joined path then splits into
     * components the file does not have — and, worse, could collide with a genuinely nested column
     * of the same spelling. Refuse the pushdown here too.
     */
    @Test
    public void testNestedFieldUnderATopLevelNameContainingADotIsNotPushedDown() {
        RowType inner = RowType.of(new DataType[] {new VarCharType()}, new String[] {"city"});
        RowType rowType =
                RowType.of(new DataType[] {new BigIntType(), inner}, new String[] {"pk", "a.b"});
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);

        // the file holds ["a.b", "city"]; the joined name "a.b.city" splits into [a, b, city]
        assertThat(schema.containsField("a.b")).isTrue();
        assertThat(schema.containsField("a")).isFalse();

        NestedFieldTransform nested =
                new NestedFieldTransform(
                        new FieldRef(1, "a.b", inner), Collections.singletonList("city"));
        test(
                schema,
                new PredicateBuilder(rowType).equal(nested, BinaryString.fromString("Beijing")),
                (String) null,
                false);
    }

    /**
     * No component contains a dot here, but the joined path still equals the literal name of an
     * unrelated top-level sibling column. findFileColumn resolves a nested predicate's joined name
     * against the file by exact top-level match before walking components, so it would bind this
     * predicate to that unrelated column's physical type. Refuse the pushdown instead of risking
     * it.
     */
    @Test
    public void testNestedFieldCollidingWithADottedTopLevelSiblingIsNotPushedDown() {
        RowType inner = RowType.of(new DataType[] {new BigIntType()}, new String[] {"a"});
        RowType rowType =
                RowType.of(
                        new DataType[] {new BigIntType(), new IntType(), inner},
                        new String[] {"pk", "s.a", "s"});
        MessageType schema = ParquetSchemaConverter.convertToParquetMessageType(rowType);

        // the file holds both a top-level "s.a" and a nested s -> a; the joined name collides
        assertThat(schema.containsField("s.a")).isTrue();
        assertThat(schema.getType("s").asGroupType().containsField("a")).isTrue();

        NestedFieldTransform nested =
                new NestedFieldTransform(
                        new FieldRef(2, "s", inner), Collections.singletonList("a"));
        test(schema, new PredicateBuilder(rowType).equal(nested, 7L), (String) null, false);
    }

    private FilterPredicate convert(MessageType schema, Predicate predicate) {
        FilterCompat.Filter filter =
                ParquetFilters.convert(PredicateBuilder.splitAnd(predicate), schema, true);
        return ((FilterPredicateCompat) filter).getFilterPredicate();
    }

    private void test(
            MessageType schema, Predicate predicate, String expected, boolean canPushDown) {
        FilterCompat.Filter filter =
                ParquetFilters.convert(PredicateBuilder.splitAnd(predicate), schema, true);
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate().toString()).isEqualTo(expected);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
    }

    private static PredicateBuilder decimalPredicateBuilder(int precision, int scale) {
        return new PredicateBuilder(
                new RowType(
                        Collections.singletonList(
                                new DataField(0, "decimal1", new DecimalType(precision, scale)))));
    }

    private static MessageType decimalSchema(
            String fieldName,
            PrimitiveTypeName physicalType,
            int fixedLength,
            int precision,
            int scale) {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.required(physicalType);
        if (physicalType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            builder.length(fixedLength);
        }
        PrimitiveType type =
                builder.as(LogicalTypeAnnotation.decimalType(scale, precision)).named(fieldName);
        return new MessageType("paimon_schema", Collections.singletonList(type));
    }
}
