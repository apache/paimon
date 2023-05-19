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

package org.apache.paimon.format.parquet.filter;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.apache.parquet.io.api.Binary;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Tests for {@link ParquetPredicateFunctionVisitor}. */
public class ParquetFilterConverterTest {
    @Test
    public void testApplyCompoundPredicate() {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "fieldName", new BigIntType()))));

        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                new ParquetFilters.Or(
                        new ParquetFilters.Or(
                                new ParquetFilters.Equals("fieldName", new BigIntType(), 1),
                                new ParquetFilters.Equals("fieldName", new BigIntType(), 2)),
                        new ParquetFilters.Equals("fieldName", new BigIntType(), 3)),
                true);

        test(
                builder.between(0, 1L, 3L),
                new ParquetFilters.And(
                        new ParquetFilters.Not(
                                new ParquetFilters.LessThan("fieldName", new BigIntType(), 1)),
                        new ParquetFilters.LessThanEquals("fieldName", new BigIntType(), 3)),
                true);

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                new ParquetFilters.And(
                        new ParquetFilters.And(
                                new ParquetFilters.Not(
                                        new ParquetFilters.Equals(
                                                "fieldName", new BigIntType(), 1)),
                                new ParquetFilters.Not(
                                        new ParquetFilters.Equals(
                                                "fieldName", new BigIntType(), 2))),
                        new ParquetFilters.Not(
                                new ParquetFilters.Equals("fieldName", new BigIntType(), 3))),
                true);

        assertThat(
                        builder.in(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(ParquetPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();

        assertThat(
                        builder.notIn(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(ParquetPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("dataTypeProvider")
    public void testApplyPredicate(Tuple3 tuple3) {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "fieldName", tuple3.dataType))));
        test(
                builder.isNull(0),
                new ParquetFilters.IsNull("fieldName", tuple3.dataType),
                tuple3.canPushDown);
        test(
                builder.isNotNull(0),
                new ParquetFilters.Not(new ParquetFilters.IsNull("fieldName", tuple3.dataType)),
                tuple3.canPushDown);
        test(
                builder.equal(0, tuple3.value),
                new ParquetFilters.Equals("fieldName", tuple3.dataType, tuple3.value),
                tuple3.canPushDown);

        test(
                builder.notEqual(0, tuple3.value),
                new ParquetFilters.Not(
                        new ParquetFilters.Equals("fieldName", tuple3.dataType, tuple3.value)),
                tuple3.canPushDown);
        test(
                builder.lessThan(0, tuple3.value),
                new ParquetFilters.LessThan("fieldName", tuple3.dataType, tuple3.value),
                tuple3.canPushDown);
        test(
                builder.lessOrEqual(0, tuple3.value),
                new ParquetFilters.LessThanEquals("fieldName", tuple3.dataType, tuple3.value),
                tuple3.canPushDown);
        test(
                builder.greaterThan(0, tuple3.value),
                new ParquetFilters.Not(
                        new ParquetFilters.LessThanEquals(
                                "fieldName", tuple3.dataType, tuple3.value)),
                tuple3.canPushDown);
        test(
                builder.greaterOrEqual(0, tuple3.value),
                new ParquetFilters.Not(
                        new ParquetFilters.LessThan("fieldName", tuple3.dataType, tuple3.value)),
                tuple3.canPushDown);
    }

    static Stream<Tuple3> dataTypeProvider() {
        return Stream.of(
                Tuple3.of(new RowType(Lists.newArrayList()), null, false),
                Tuple3.of(new MultisetType(new TimeType()), null, false),
                Tuple3.of(new ArrayType(new TimeType()), null, false),
                Tuple3.of(new MapType(new BooleanType(), new BooleanType()), null, false),
                Tuple3.of(new BinaryType(), Binary.fromString("a"), true),
                Tuple3.of(new VarBinaryType(), Binary.fromString("b"), true),
                Tuple3.of(new CharType(), Binary.fromString("c"), true),
                Tuple3.of(new VarCharType(), Binary.fromString("paimon"), true),
                Tuple3.of(new TimeType(), LocalTime.now(), true),
                Tuple3.of(new DateType(), LocalDate.now(), true),
                Tuple3.of(new TimestampType(0), LocalDateTime.now(), true),
                Tuple3.of(new TimestampType(3), LocalDateTime.now(), true),
                Tuple3.of(new TimestampType(6), LocalDateTime.now(), true),
                Tuple3.of(new TimestampType(9), LocalDateTime.now(), false),
                Tuple3.of(new BooleanType(), true, true),
                Tuple3.of(
                        new DecimalType(),
                        Decimal.fromBigDecimal(new BigDecimal("1.23"), 10, 2),
                        false),
                Tuple3.of(new FloatType(), 1.2, true),
                Tuple3.of(new DoubleType(), 3.4, true),
                Tuple3.of(new BigIntType(), 10, true),
                Tuple3.of(new IntType(), 10, true));
    }

    private void test(
            Predicate predicate, ParquetFilters.Predicate parquetPredicate, boolean canPushDown) {
        Optional<ParquetFilters.Predicate> optionalPredicate =
                predicate.visit(ParquetPredicateFunctionVisitor.VISITOR);
        if (canPushDown) {
            // the converted predicate can not be empty.
            assertThat(optionalPredicate.isPresent()).isEqualTo(true);

            // asset the literal is same.
            optionalPredicate.ifPresent(
                    value -> assertThat(value).hasToString(parquetPredicate.toString()));
        } else {
            assertThat(optionalPredicate).isEqualTo(Optional.empty());
        }
    }

    static class Tuple3 {
        DataType dataType;
        Serializable value;
        boolean canPushDown;

        Tuple3(DataType dataType, Serializable value, boolean canPushDown) {
            this.dataType = dataType;
            this.value = value;
            this.canPushDown = canPushDown;
        }

        static Tuple3 of(DataType dataType, Serializable value, boolean canPushDown) {
            return new Tuple3(dataType, value, canPushDown);
        }
    }
}
