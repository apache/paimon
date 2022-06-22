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

package org.apache.flink.table.store.file.predicate;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Predicate}s. */
public class PredicateTest {

    @Test
    public void testEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(BuiltInFunctionDefinitions.EQUALS, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.notEqual(0, 5));
    }

    @Test
    public void testNotEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(BuiltInFunctionDefinitions.NOT_EQUALS, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(5, 5, 0)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.equal(0, 5));
    }

    @Test
    public void testGreater() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessOrEqual(0, 5));
    }

    @Test
    public void testGreaterOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 4, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.lessThan(0, 5));
    }

    @Test
    public void testLess() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(BuiltInFunctionDefinitions.LESS_THAN, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterOrEqual(0, 5));
    }

    @Test
    public void testLessOrEqual() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.greaterThan(0, 5));
    }

    @Test
    public void testIsNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.IS_NULL,
                        field(0, DataTypes.INT()),
                        literal(null, DataTypes.INT()));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(true);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1)})).isEqualTo(true);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNotNull(0));
    }

    @Test
    public void testIsNotNull() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.IS_NOT_NULL,
                        field(0, DataTypes.INT()),
                        literal(null, DataTypes.INT()));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(null, null, 3)}))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null)).isEqualTo(builder.isNull(0));
    }

    @Test
    public void testAnd() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.AND,
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(0, DataTypes.INT()),
                                literal(3)),
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(1, DataTypes.INT()),
                                literal(5)));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4, 5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null, 5})).isEqualTo(false);

        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0), new FieldStats(4, 6, 0)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0), new FieldStats(6, 8, 0)
                                }))
                .isEqualTo(false);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(6, 7, 0), new FieldStats(4, 6, 0)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.or(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @Test
    public void testOr() {
        PredicateBuilder builder = new PredicateBuilder(RowType.of(new IntType(), new IntType()));
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.OR,
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(0, DataTypes.INT()),
                                literal(3)),
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(1, DataTypes.INT()),
                                literal(5)));
        Predicate predicate = expression.accept(new PredicateConverter(builder));

        assertThat(predicate.test(new Object[] {4, 6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {3, 6})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {3, 5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null, 5})).isEqualTo(true);

        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0), new FieldStats(4, 6, 0)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(3, 6, 0), new FieldStats(6, 8, 0)
                                }))
                .isEqualTo(true);
        assertThat(
                        predicate.test(
                                3,
                                new FieldStats[] {
                                    new FieldStats(6, 7, 0), new FieldStats(8, 10, 0)
                                }))
                .isEqualTo(false);

        assertThat(predicate.negate().orElse(null))
                .isEqualTo(PredicateBuilder.and(builder.notEqual(0, 3), builder.notEqual(1, 5)));
    }

    @MethodSource("provideLikeExpressions")
    @ParameterizedTest
    public void testStartsWith(
            CallExpression callExpression,
            List<Object[]> valuesList,
            List<Boolean> expectedForValues,
            List<Long> rowCountList,
            List<FieldStats[]> statsList,
            List<Boolean> expectedForStats) {
        Predicate predicate =
                callExpression.accept(new PredicateConverter(RowType.of(new VarCharType())));
        IntStream.range(0, valuesList.size())
                .forEach(
                        i ->
                                assertThat(predicate.test(valuesList.get(i)))
                                        .isEqualTo(expectedForValues.get(i)));
        IntStream.range(0, rowCountList.size())
                .forEach(
                        i ->
                                assertThat(predicate.test(rowCountList.get(i), statsList.get(i)))
                                        .isEqualTo(expectedForStats.get(i)));
    }

    @Test
    public void testUnsupportedExpression() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.AND,
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(0, DataTypes.INT()),
                                literal(3)),
                        call(
                                BuiltInFunctionDefinitions.SIMILAR,
                                field(1, DataTypes.INT()),
                                literal(5)));
        assertThatThrownBy(
                        () ->
                                expression.accept(
                                        new PredicateConverter(
                                                RowType.of(new IntType(), new IntType()))))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedStartsPatternForLike() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        // starts pattern with '_' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("abc_", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // starts pattern like 'abc%xyz' or 'abc_xyz'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("abc%xyz", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("abc_xyz", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // starts pattern like 'abc%xyz' or 'abc_xyz' with '%' or '_' to escape
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "=%abc=%%xyz=_",
                                                        STRING()), // matches "%abc%(?s:.*)xyz_"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "abc=%%xyz",
                                                        STRING()), // matches "abc%(?s:.*)xyz"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "abc=%_xyz",
                                                        STRING()), // matches "abc%.xyz"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "abc=_%xyz",
                                                        STRING()), // matches "abc_(?s:.*)xyz"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "abc=__xyz",
                                                        STRING()), // matches "abc_.xyz"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // starts pattern with wildcard '%' at the beginning to escape
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("=%%", STRING()), // matches "%(?s:.*)"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedEndsPatternForLike() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        // ends pattern with '%' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("%456", STRING())) // matches "(?s:.*)456"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // ends pattern with '_' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_456", STRING())) // matches ".456"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // ends pattern with '[]' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_[456]", STRING())) // matches ".[456]"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%[h-m]",
                                                        STRING())) // matches "(?s:.*)[h-m]"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // ends pattern with '[^]' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%[^h-m]",
                                                        STRING())) // matches "(?s:.*)[^h-m]"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_[^xyz]", STRING())) // matches ".[^xyz]"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // ends pattern escape wildcard '%'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%=%456",
                                                        STRING()), // matches "(?s:.*)%456"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%=_456",
                                                        STRING()), // matches "(?s:.*)_456"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // ends pattern escape wildcard '_'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_=_456", STRING()), // matches "._456"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedEqualsPatternForLike() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        // equals pattern
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("123456", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // equals pattern escape '%'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("12=%45", STRING()), // equals "12%45"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // equals pattern escape '_'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("12=_45", STRING()), // equals "12_45"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedMiddlePatternForLike() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        // middle pattern with '%' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("%345%", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern with '_' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_345_", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern with both '%' and '_' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("_345%", STRING())) // matches ".345(?s:.*)"
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal("%345_", STRING())) // matches "(?s:.*)345."
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern with '[]' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%[a-c]_",
                                                        STRING())) // matches "(?s:.*)[a-c]."
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern with '[^]' as wildcard
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%[^abc]_",
                                                        STRING())) // matches "(?s:.*)[^abc]."
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern escape '%'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%34=%5%",
                                                        STRING()), // matches "(?s:.*)34%5(.*)"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        // middle pattern escape '_'
        assertThatThrownBy(
                        () ->
                                call(
                                                BuiltInFunctionDefinitions.LIKE,
                                                field(0, STRING()),
                                                literal(
                                                        "%34=_5%",
                                                        STRING()), // matches "(?s:.*)34_5(.*)"
                                                literal("=", STRING()))
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedType() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        DataType structType = DataTypes.ROW(DataTypes.INT()).bridgedTo(Row.class);
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, structType),
                        literal(Row.of(1), structType));
        assertThatThrownBy(() -> expression.accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    public static Stream<Arguments> provideLikeExpressions() {
        CallExpression expr1 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("abd%", STRING()));
        List<Object[]> valuesList1 =
                Arrays.asList(
                        new Object[] {null},
                        new Object[] {StringData.fromString("a")},
                        new Object[] {StringData.fromString("ab")},
                        new Object[] {StringData.fromString("abd")},
                        new Object[] {StringData.fromString("abd%")},
                        new Object[] {StringData.fromString("abd1")},
                        new Object[] {StringData.fromString("abde@")},
                        new Object[] {StringData.fromString("abd_")},
                        new Object[] {StringData.fromString("abd_%")});
        List<Boolean> expectedForValues1 =
                Arrays.asList(false, false, false, true, true, true, true, true, true);
        List<Long> rowCountList1 = Arrays.asList(0L, 3L, 3L, 3L);
        List<FieldStats[]> statsList1 =
                Arrays.asList(
                        new FieldStats[] {new FieldStats(null, null, 0L)},
                        new FieldStats[] {new FieldStats(null, null, 3L)},
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("ab"),
                                    StringData.fromString("abc123"),
                                    1L)
                        },
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("abc"), StringData.fromString("abe"), 1L)
                        });
        List<Boolean> expectedForStats1 = Arrays.asList(false, false, false, true);

        CallExpression expr2 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("test=_%", STRING()),
                        literal("=", STRING()));
        List<Object[]> valuesList2 =
                Arrays.asList(
                        new Object[] {StringData.fromString("test%")},
                        new Object[] {StringData.fromString("test_123")},
                        new Object[] {StringData.fromString("test_%")},
                        new Object[] {StringData.fromString("test__")});
        List<Boolean> expectedForValues2 = Arrays.asList(false, true, true, true);
        List<Long> rowCountList2 = Collections.singletonList(3L);
        List<FieldStats[]> statsList2 =
                Collections.singletonList(
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("test_123"),
                                    StringData.fromString("test_789"),
                                    0L)
                        });
        List<Boolean> expectedForStats2 = Collections.singletonList(true);

        // currently, SQL wildcards '[]' and '[^]' are deemed as normal characters in Flink
        CallExpression expr3 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("[a-c]xyz%", STRING()));
        List<Object[]> valuesList3 =
                Arrays.asList(
                        new Object[] {StringData.fromString("axyz")},
                        new Object[] {StringData.fromString("bxyz")},
                        new Object[] {StringData.fromString("cxyz")},
                        new Object[] {StringData.fromString("[a-c]xyz")});
        List<Boolean> expectedForValues3 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList3 = Collections.singletonList(3L);
        List<FieldStats[]> statsList3 =
                Collections.singletonList(
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("[a-c]xyz"),
                                    StringData.fromString("[a-c]xyzz"),
                                    0L)
                        });
        List<Boolean> expectedForStats3 = Collections.singletonList(true);

        CallExpression expr4 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("[^a-d]xyz%", STRING()));
        List<Object[]> valuesList4 =
                Arrays.asList(
                        new Object[] {StringData.fromString("exyz")},
                        new Object[] {StringData.fromString("fxyz")},
                        new Object[] {StringData.fromString("axyz")},
                        new Object[] {StringData.fromString("[^a-d]xyz")});
        List<Boolean> expectedForValues4 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList4 = Collections.singletonList(3L);
        List<FieldStats[]> statsList4 =
                Collections.singletonList(
                        new FieldStats[] {
                            new FieldStats(
                                    StringData.fromString("[^a-d]xyz"),
                                    StringData.fromString("[^a-d]xyzz"),
                                    1L)
                        });
        List<Boolean> expectedForStats4 = Collections.singletonList(true);

        return Stream.of(
                Arguments.of(
                        expr1,
                        valuesList1,
                        expectedForValues1,
                        rowCountList1,
                        statsList1,
                        expectedForStats1),
                Arguments.of(
                        expr2,
                        valuesList2,
                        expectedForValues2,
                        rowCountList2,
                        statsList2,
                        expectedForStats2),
                Arguments.of(
                        expr3,
                        valuesList3,
                        expectedForValues3,
                        rowCountList3,
                        statsList3,
                        expectedForStats3),
                Arguments.of(
                        expr4,
                        valuesList4,
                        expectedForValues4,
                        rowCountList4,
                        statsList4,
                        expectedForStats4));
    }

    private static FieldReferenceExpression field(int i, DataType type) {
        return new FieldReferenceExpression("name", type, 0, i);
    }

    private static CallExpression call(FunctionDefinition function, ResolvedExpression... args) {
        return new CallExpression(function, Arrays.asList(args), DataTypes.BOOLEAN());
    }
}
