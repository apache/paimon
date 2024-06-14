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

package org.apache.paimon.format.orc.filter;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Tests for {@link org.apache.paimon.format.orc.filter.OrcPredicateFunctionVisitor}. */
public class OrcFilterConverterTest {

    @Test
    public void testApplyCompoundPredicate() {

        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "long1", new BigIntType()))));
        test(
                builder.in(0, Arrays.asList(1L, 2L, 3L)),
                new OrcFilters.Or(
                        new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 1),
                        new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 2),
                        new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 3)),
                true);

        test(
                builder.between(0, 1L, 3L),
                new OrcFilters.And(
                        new OrcFilters.Not(
                                new OrcFilters.LessThan("long1", PredicateLeaf.Type.LONG, 1)),
                        new OrcFilters.LessThanEquals("long1", PredicateLeaf.Type.LONG, 3)),
                true);

        test(
                builder.notIn(0, Arrays.asList(1L, 2L, 3L)),
                new OrcFilters.And(
                        new OrcFilters.Not(
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 1)),
                        new OrcFilters.Not(
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 2)),
                        new OrcFilters.Not(
                                new OrcFilters.Equals("long1", PredicateLeaf.Type.LONG, 3))),
                true);

        assertThat(
                        builder.in(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(OrcPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();

        assertThat(
                        builder.notIn(
                                        0,
                                        LongStream.range(1L, 22L)
                                                .boxed()
                                                .collect(Collectors.toList()))
                                .visit(OrcPredicateFunctionVisitor.VISITOR)
                                .isPresent())
                .isFalse();
    }

    @ParameterizedTest
    @MethodSource("dataTypeProvider")
    public void testApplyPredicate(Tuple4 tuple4) {
        PredicateBuilder builder =
                new PredicateBuilder(
                        new RowType(
                                Collections.singletonList(
                                        new DataField(0, "fieldName", tuple4.dataType))));

        test(
                builder.isNull(0),
                new OrcFilters.IsNull("fieldName", tuple4.type),
                tuple4.canPushDown);
        test(
                builder.isNotNull(0),
                new OrcFilters.Not(new OrcFilters.IsNull("fieldName", tuple4.type)),
                tuple4.canPushDown);
        test(
                builder.equal(0, tuple4.value),
                new OrcFilters.Equals("fieldName", tuple4.type, tuple4.value),
                tuple4.canPushDown);

        test(
                builder.notEqual(0, tuple4.value),
                new OrcFilters.Not(new OrcFilters.Equals("fieldName", tuple4.type, tuple4.value)),
                tuple4.canPushDown);
        test(
                builder.lessThan(0, tuple4.value),
                new OrcFilters.LessThan("fieldName", tuple4.type, tuple4.value),
                tuple4.canPushDown);
        test(
                builder.lessOrEqual(0, tuple4.value),
                new OrcFilters.LessThanEquals("fieldName", tuple4.type, tuple4.value),
                tuple4.canPushDown);
        test(
                builder.greaterThan(0, tuple4.value),
                new OrcFilters.Not(
                        new OrcFilters.LessThanEquals("fieldName", tuple4.type, tuple4.value)),
                tuple4.canPushDown);
        test(
                builder.greaterOrEqual(0, tuple4.value),
                new OrcFilters.Not(new OrcFilters.LessThan("fieldName", tuple4.type, tuple4.value)),
                tuple4.canPushDown);
    }

    private void test(Predicate predicate, OrcFilters.Predicate orcPredicate, boolean canPushDown) {
        Optional<OrcFilters.Predicate> optionalPredicate =
                predicate.visit(OrcPredicateFunctionVisitor.VISITOR);
        if (canPushDown) {

            // asset the literal is same.
            optionalPredicate.ifPresent(
                    value -> assertThat(value).hasToString(orcPredicate.toString()));

            // asset the type is same.
            if (predicate instanceof LeafPredicate
                    && orcPredicate instanceof OrcFilters.ColumnPredicate) {
                OrcFilters.ColumnPredicate columnPredicate =
                        (OrcFilters.ColumnPredicate) orcPredicate;
                assertThat(
                                OrcPredicateFunctionVisitor.toOrcType(
                                        ((LeafPredicate) predicate).type()))
                        .isEqualTo(columnPredicate.literalType);
            }
        } else {
            assertThat(optionalPredicate).isEqualTo(Optional.empty());
        }
    }

    static Stream<Tuple4> dataTypeProvider() {
        // TODO : add date and timestamp type
        return Stream.of(
                Tuple4.of(new RowType(Lists.newArrayList()), null, null, false),
                Tuple4.of(new MultisetType(new TimeType()), null, null, false),
                Tuple4.of(new ArrayType(new TimeType()), null, null, false),
                Tuple4.of(new MapType(new BooleanType(), new BooleanType()), null, null, false),
                Tuple4.of(new TimeType(), null, null, false),
                Tuple4.of(new BinaryType(), PredicateLeaf.Type.STRING, LocalDateTime.now(), false),
                Tuple4.of(
                        new VarBinaryType(), PredicateLeaf.Type.STRING, LocalDateTime.now(), false),
                Tuple4.of(new BooleanType(), PredicateLeaf.Type.BOOLEAN, true, true),
                Tuple4.of(new CharType(), PredicateLeaf.Type.STRING, "a", true),
                Tuple4.of(new VarCharType(), PredicateLeaf.Type.STRING, "paimon", true),
                Tuple4.of(
                        new DecimalType(),
                        PredicateLeaf.Type.DECIMAL,
                        Decimal.fromBigDecimal(new BigDecimal("1.23"), 10, 2),
                        true),
                Tuple4.of(new FloatType(), PredicateLeaf.Type.FLOAT, 1.2, true),
                Tuple4.of(new DoubleType(), PredicateLeaf.Type.FLOAT, 3.4, true),
                Tuple4.of(new TinyIntType(), PredicateLeaf.Type.LONG, 10, true),
                Tuple4.of(new SmallIntType(), PredicateLeaf.Type.LONG, 10, true),
                Tuple4.of(new BigIntType(), PredicateLeaf.Type.LONG, 10, true),
                Tuple4.of(new IntType(), PredicateLeaf.Type.LONG, 10, true));
    }

    static class Tuple4 {
        DataType dataType;
        PredicateLeaf.Type type;
        Serializable value;
        boolean canPushDown;

        Tuple4(
                DataType dataType,
                PredicateLeaf.Type type,
                Serializable value,
                boolean canPushDown) {
            this.dataType = dataType;
            this.type = type;
            this.value = value;
            this.canPushDown = canPushDown;
        }

        static Tuple4 of(
                DataType dataType,
                PredicateLeaf.Type type,
                Serializable value,
                boolean canPushDown) {
            return new Tuple4(dataType, type, value, canPushDown);
        }
    }
}
