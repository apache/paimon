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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
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
    public void testLong() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));

        test(builder.isNull(0), "eq(long1, null)", true);

        test(builder.isNotNull(0), "noteq(long1, null)", true);

        test(builder.lessThan(0, 5L), "lt(long1, 5)", true);

        test(builder.greaterThan(0, 5L), "gt(long1, 5)", true);

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                "or(or(eq(long1, 1), eq(long1, 2)), eq(long1, 3))",
                true);

        test(builder.between(0, 1L, 3L), "and(gteq(long1, 1), lteq(long1, 3))", true);

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                "and(and(noteq(long1, 1), noteq(long1, 2)), noteq(long1, 3))",
                true);
    }

    @Test
    public void testString() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "string1", new VarCharType()))));
        test(builder.isNull(0), "eq(string1, null)", true);

        test(builder.isNotNull(0), "noteq(string1, null)", true);

        test(
                builder.in(0, Arrays.asList("1", "2", "3")),
                "or(or(eq(string1, Binary{\"1\"}), eq(string1, Binary{\"2\"})), eq(string1, Binary{\"3\"}))",
                true);
        test(
                builder.notIn(0, Arrays.asList("1", "2", "3")),
                "and(and(noteq(string1, Binary{\"1\"}), noteq(string1, Binary{\"2\"})), noteq(string1, Binary{\"3\"}))",
                true);
    }

    @Test
    public void testInFilterLong() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "col1", new BigIntType()))));
        test(
                builder.in(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                FilterApi.in(
                        FilterApi.longColumn("col1"),
                        LongStream.range(1L, 22L).boxed().collect(Collectors.toSet())),
                true);

        test(
                builder.notIn(0, LongStream.range(1L, 22L).boxed().collect(Collectors.toList())),
                FilterApi.notIn(
                        FilterApi.longColumn("col1"),
                        LongStream.range(1L, 22L).boxed().collect(Collectors.toSet())),
                true);
    }

    @Test
    public void testInFilterDouble() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "col1", new DoubleType()))));
        test(
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
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "col1", new VarCharType()))));
        test(
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
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "d1", new DoubleType()))));

        FilterCompat.Filter filter =
                ParquetFilters.convert(Collections.singletonList(builder.isNaN(0)));
        FilterPredicateCompat compat = (FilterPredicateCompat) filter;
        assertThat(compat.getFilterPredicate().toString())
                .contains(
                        "userdefinedbyinstance(d1, org.apache.parquet.filter2.predicate.ParquetFilters$IsNaNDoublePredicate");
    }

    @Test
    public void testIsNaNFloat() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "f1", new FloatType()))));

        FilterCompat.Filter filter =
                ParquetFilters.convert(Collections.singletonList(builder.isNaN(0)));
        FilterPredicateCompat compat = (FilterPredicateCompat) filter;
        assertThat(compat.getFilterPredicate().toString())
                .contains(
                        "userdefinedbyinstance(f1, org.apache.parquet.filter2.predicate.ParquetFilters$IsNaNFloatPredicate");
    }

    @Test
    public void testInFilterFloat() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "col1", new FloatType()))));

        test(
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
    public void testDecimalWithoutFileSchema() {
        int precision = 20;
        int scale = 0;
        PredicateBuilder builder = decimalPredicateBuilder(precision, scale);
        Decimal value = Decimal.fromBigDecimal(new BigDecimal("10000939"), precision, scale);

        test(builder.equal(0, value), "", false);
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

        // For less than 21 elements, it expands to or(eq, eq, eq)
        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(or(eq(decimal1, "
                        + (int) v1.toUnscaledLong()
                        + "), eq(decimal1, "
                        + (int) v2.toUnscaledLong()
                        + ")), eq(decimal1, "
                        + (int) v3.toUnscaledLong()
                        + "))",
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

        // For less than 21 elements, it expands to or(eq, eq, eq)
        test(
                schema,
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(or(eq(decimal1, "
                        + v1.toUnscaledLong()
                        + "), eq(decimal1, "
                        + v2.toUnscaledLong()
                        + ")), eq(decimal1, "
                        + v3.toUnscaledLong()
                        + "))",
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
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "ts1", new TimestampType(precision)))));

        Timestamp value = Timestamp.fromEpochMillis(1704067200000L); // 2024-01-01 00:00:00
        long expectedMillis = value.getMillisecond();

        test(builder.isNull(0), "eq(ts1, null)", true);
        test(builder.isNotNull(0), "noteq(ts1, null)", true);
        test(builder.equal(0, value), "eq(ts1, " + expectedMillis + ")", true);
        test(builder.notEqual(0, value), "noteq(ts1, " + expectedMillis + ")", true);
        test(builder.lessThan(0, value), "lt(ts1, " + expectedMillis + ")", true);
        test(builder.lessOrEqual(0, value), "lteq(ts1, " + expectedMillis + ")", true);
        test(builder.greaterThan(0, value), "gt(ts1, " + expectedMillis + ")", true);
        test(builder.greaterOrEqual(0, value), "gteq(ts1, " + expectedMillis + ")", true);
    }

    @Test
    public void testTimestampMicros() {
        // 3 < precision <= 6 uses microseconds (INT64)
        int precision = 6;
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "ts1", new TimestampType(precision)))));

        Timestamp value = Timestamp.fromEpochMillis(1704067200123L, 456000); // with nanos
        long expectedMicros = value.getMillisecond() * 1000 + value.getNanoOfMillisecond() / 1000;

        test(builder.isNull(0), "eq(ts1, null)", true);
        test(builder.isNotNull(0), "noteq(ts1, null)", true);
        test(builder.equal(0, value), "eq(ts1, " + expectedMicros + ")", true);
        test(builder.notEqual(0, value), "noteq(ts1, " + expectedMicros + ")", true);
        test(builder.lessThan(0, value), "lt(ts1, " + expectedMicros + ")", true);
        test(builder.lessOrEqual(0, value), "lteq(ts1, " + expectedMicros + ")", true);
        test(builder.greaterThan(0, value), "gt(ts1, " + expectedMicros + ")", true);
        test(builder.greaterOrEqual(0, value), "gteq(ts1, " + expectedMicros + ")", true);
    }

    @Test
    public void testLocalZonedTimestampMillis() {
        // precision <= 3 uses milliseconds (INT64)
        int precision = 3;
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "ts1",
                                                new LocalZonedTimestampType(precision)))));

        Timestamp value = Timestamp.fromEpochMillis(1704067200000L);
        long expectedMillis = value.getMillisecond();

        test(builder.isNull(0), "eq(ts1, null)", true);
        test(builder.isNotNull(0), "noteq(ts1, null)", true);
        test(builder.equal(0, value), "eq(ts1, " + expectedMillis + ")", true);
        test(builder.notEqual(0, value), "noteq(ts1, " + expectedMillis + ")", true);
        test(builder.lessThan(0, value), "lt(ts1, " + expectedMillis + ")", true);
        test(builder.greaterThan(0, value), "gt(ts1, " + expectedMillis + ")", true);
    }

    @Test
    public void testLocalZonedTimestampMicros() {
        // 3 < precision <= 6 uses microseconds (INT64)
        int precision = 6;
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(
                                                0,
                                                "ts1",
                                                new LocalZonedTimestampType(precision)))));

        Timestamp value = Timestamp.fromEpochMillis(1704067200123L, 456000);
        long expectedMicros = value.getMillisecond() * 1000 + value.getNanoOfMillisecond() / 1000;

        test(builder.isNull(0), "eq(ts1, null)", true);
        test(builder.isNotNull(0), "noteq(ts1, null)", true);
        test(builder.equal(0, value), "eq(ts1, " + expectedMicros + ")", true);
        test(builder.notEqual(0, value), "noteq(ts1, " + expectedMicros + ")", true);
        test(builder.lessThan(0, value), "lt(ts1, " + expectedMicros + ")", true);
        test(builder.greaterThan(0, value), "gt(ts1, " + expectedMicros + ")", true);
    }

    @Test
    public void testInFilterTimestampMillis() {
        int precision = 3;
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "ts1", new TimestampType(precision)))));

        Timestamp v1 = Timestamp.fromEpochMillis(1704067200000L);
        Timestamp v2 = Timestamp.fromEpochMillis(1704153600000L);
        Timestamp v3 = Timestamp.fromEpochMillis(1704240000000L);

        test(
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(or(eq(ts1, "
                        + v1.getMillisecond()
                        + "), eq(ts1, "
                        + v2.getMillisecond()
                        + ")), eq(ts1, "
                        + v3.getMillisecond()
                        + "))",
                true);

        test(
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
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "ts1", new TimestampType(precision)))));

        Timestamp v1 = Timestamp.fromEpochMillis(1704067200000L, 123000);
        Timestamp v2 = Timestamp.fromEpochMillis(1704153600000L, 456000);
        Timestamp v3 = Timestamp.fromEpochMillis(1704240000000L, 789000);

        long micros1 = v1.getMillisecond() * 1000 + v1.getNanoOfMillisecond() / 1000;
        long micros2 = v2.getMillisecond() * 1000 + v2.getNanoOfMillisecond() / 1000;
        long micros3 = v3.getMillisecond() * 1000 + v3.getNanoOfMillisecond() / 1000;

        test(
                builder.in(0, Arrays.asList(v1, v2, v3)),
                "or(or(eq(ts1, "
                        + micros1
                        + "), eq(ts1, "
                        + micros2
                        + ")), eq(ts1, "
                        + micros3
                        + "))",
                true);

        test(
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

    private void test(Predicate predicate, FilterPredicate parquetPredicate, boolean canPushDown) {
        FilterCompat.Filter filter = ParquetFilters.convert(PredicateBuilder.splitAnd(predicate));
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate()).isEqualTo(parquetPredicate);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
    }

    private FilterPredicate convert(Predicate predicate) {
        FilterCompat.Filter filter = ParquetFilters.convert(PredicateBuilder.splitAnd(predicate));
        return ((FilterPredicateCompat) filter).getFilterPredicate();
    }

    private void test(Predicate predicate, String expected, boolean canPushDown) {
        FilterCompat.Filter filter = ParquetFilters.convert(PredicateBuilder.splitAnd(predicate));
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate().toString()).isEqualTo(expected);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
    }
}
