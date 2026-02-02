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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
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

        test(builder.isNull(0), "eq(decimal1, null)", true);
        test(builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(builder.equal(0, value), "eq(decimal1, " + expectedIntVal + ")", true);
        test(builder.notEqual(0, value), "noteq(decimal1, " + expectedIntVal + ")", true);
        test(builder.lessThan(0, value), "lt(decimal1, " + expectedIntVal + ")", true);
        test(builder.lessOrEqual(0, value), "lteq(decimal1, " + expectedIntVal + ")", true);
        test(builder.greaterThan(0, value), "gt(decimal1, " + expectedIntVal + ")", true);
        test(builder.greaterOrEqual(0, value), "gteq(decimal1, " + expectedIntVal + ")", true);
    }

    @Test
    public void testDecimal64Bit() {
        // 9 < precision <= 18 uses INT64
        int precision = 18;
        int scale = 4;
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

        test(builder.isNull(0), "eq(decimal1, null)", true);
        test(builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(builder.equal(0, value), "eq(decimal1, " + expectedLongVal + ")", true);
        test(builder.notEqual(0, value), "noteq(decimal1, " + expectedLongVal + ")", true);
        test(builder.lessThan(0, value), "lt(decimal1, " + expectedLongVal + ")", true);
        test(builder.lessOrEqual(0, value), "lteq(decimal1, " + expectedLongVal + ")", true);
        test(builder.greaterThan(0, value), "gt(decimal1, " + expectedLongVal + ")", true);
        test(builder.greaterOrEqual(0, value), "gteq(decimal1, " + expectedLongVal + ")", true);
    }

    @Test
    public void testDecimalBinary() {
        // precision > 18 uses Binary
        int precision = 38;
        int scale = 10;
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
        Binary expectedBinary = Binary.fromConstantByteArray(value.toUnscaledBytes());

        test(builder.isNull(0), "eq(decimal1, null)", true);
        test(builder.isNotNull(0), "noteq(decimal1, null)", true);
        test(
                builder.equal(0, value),
                FilterApi.eq(FilterApi.binaryColumn("decimal1"), expectedBinary),
                true);
        test(
                builder.notEqual(0, value),
                FilterApi.notEq(FilterApi.binaryColumn("decimal1"), expectedBinary),
                true);
        test(
                builder.lessThan(0, value),
                FilterApi.lt(FilterApi.binaryColumn("decimal1"), expectedBinary),
                true);
        test(
                builder.greaterThan(0, value),
                FilterApi.gt(FilterApi.binaryColumn("decimal1"), expectedBinary),
                true);
    }

    @Test
    public void testInFilterDecimal32Bit() {
        int precision = 9;
        int scale = 2;
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

    private void test(Predicate predicate, FilterPredicate parquetPredicate, boolean canPushDown) {
        FilterCompat.Filter filter = ParquetFilters.convert(PredicateBuilder.splitAnd(predicate));
        if (canPushDown) {
            FilterPredicateCompat compat = (FilterPredicateCompat) filter;
            assertThat(compat.getFilterPredicate()).isEqualTo(parquetPredicate);
        } else {
            assertThat(filter).isEqualTo(FilterCompat.NOOP);
        }
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
