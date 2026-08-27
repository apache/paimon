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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.NotBetween;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SimpleColStatsTestUtils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PredicateConverter}. */
public class PredicateConverterTest {

    private static final PredicateBuilder BUILDER =
            new PredicateBuilder(
                    LogicalTypeConversion.toDataType(
                            new RowType(
                                    Arrays.asList(
                                            new RowType.RowField("long1", new BigIntType()),
                                            new RowType.RowField("double1", new DoubleType()),
                                            new RowType.RowField(
                                                    "string1", DataTypes.STRING().getLogicalType()),
                                            new RowType.RowField(
                                                    "boolField",
                                                    DataTypes.BOOLEAN().getLogicalType())))));

    private static final PredicateConverter CONVERTER = new PredicateConverter(BUILDER);

    @MethodSource("provideResolvedExpression")
    @ParameterizedTest
    public void testVisitAndAutoTypeInference(ResolvedExpression expression, Predicate expected) {
        if (expression instanceof CallExpression) {
            assertThat(CONVERTER.visit((CallExpression) expression)).isEqualTo(expected);
        } else {
            assertThatThrownBy(() -> CONVERTER.visit(expression))
                    .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        }
    }

    public static Stream<Arguments> provideResolvedExpression() {
        FieldReferenceExpression longRefExpr =
                new FieldReferenceExpression(
                        "long1", DataTypes.BIGINT(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ValueLiteralExpression intLitExpr = new ValueLiteralExpression(10);
        ValueLiteralExpression intLitExpr2 = new ValueLiteralExpression(20);
        long longLit = 10L;
        ValueLiteralExpression nullLongLitExpr =
                new ValueLiteralExpression(null, DataTypes.BIGINT());

        FieldReferenceExpression doubleRefExpr =
                new FieldReferenceExpression(
                        "double1", DataTypes.DOUBLE(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ValueLiteralExpression floatLitExpr = new ValueLiteralExpression(3.14f);
        double doubleLit = 3.14d;

        FieldReferenceExpression stringRefExpr =
                new FieldReferenceExpression(
                        "string1", DataTypes.STRING(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        String stringLit = "haha";
        // same type
        ValueLiteralExpression stringLitExpr1 =
                new ValueLiteralExpression(stringLit, DataTypes.STRING().notNull());
        // different type, char(4)
        ValueLiteralExpression stringLitExpr2 = new ValueLiteralExpression(stringLit);

        FieldReferenceExpression boolRefExpr =
                new FieldReferenceExpression("boolField", DataTypes.BOOLEAN(), 3, 3);

        return Stream.of(
                Arguments.of(longRefExpr, null),
                Arguments.of(intLitExpr, null),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NULL,
                                Collections.singletonList(longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.isNull(0)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NOT_NULL,
                                Collections.singletonList(doubleRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.isNotNull(1)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                // test literal on left
                                Arrays.asList(intLitExpr, longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(nullLongLitExpr, longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.notEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.notEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterThan(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterOrEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterOrEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessThan(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessOrEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessOrEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.AND,
                                Arrays.asList(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                                Arrays.asList(longRefExpr, intLitExpr),
                                                DataTypes.BOOLEAN()),
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                Arrays.asList(doubleRefExpr, floatLitExpr),
                                                DataTypes.BOOLEAN())),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.and(
                                BUILDER.lessOrEqual(0, longLit), BUILDER.equal(1, doubleLit))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.OR,
                                Arrays.asList(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                                Arrays.asList(longRefExpr, intLitExpr),
                                                DataTypes.BOOLEAN()),
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                Arrays.asList(doubleRefExpr, floatLitExpr),
                                                DataTypes.BOOLEAN())),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.or(
                                BUILDER.notEqual(0, longLit), BUILDER.equal(1, doubleLit))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IN,
                                Arrays.asList(
                                        longRefExpr, intLitExpr, nullLongLitExpr, intLitExpr2),
                                DataTypes.BOOLEAN()),
                        BUILDER.in(0, Arrays.asList(10L, null, 20L))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(stringLitExpr1, stringRefExpr),
                                DataTypes.STRING()),
                        BUILDER.equal(2, BinaryString.fromString("haha"))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(stringLitExpr2, stringRefExpr),
                                DataTypes.STRING()),
                        BUILDER.equal(2, BinaryString.fromString("haha"))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.BETWEEN,
                                Arrays.asList(longRefExpr, intLitExpr, intLitExpr2),
                                DataTypes.BOOLEAN()),
                        BUILDER.between(0, 10L, 20L)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_TRUE,
                                Arrays.asList(boolRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(3, true)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_FALSE,
                                Arrays.asList(boolRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(3, false)));
    }

    @Test
    public void testBetweenWithImplicitNumericConversion() {
        Predicate predicate =
                call(
                                BuiltInFunctionDefinitions.BETWEEN,
                                field(0, DataTypes.BIGINT()),
                                literal(10),
                                literal(20))
                        .accept(new PredicateConverter(RowType.of(new BigIntType())));

        assertThat(predicate.test(GenericRow.of(9L))).isFalse();
        assertThat(predicate.test(GenericRow.of(10L))).isTrue();
        assertThat(predicate.test(GenericRow.of(15L))).isTrue();
        assertThat(predicate.test(GenericRow.of(20L))).isTrue();
        assertThat(predicate.test(GenericRow.of(21L))).isFalse();
        assertThat(predicate.test(GenericRow.of((Object) null))).isFalse();
    }

    @Test
    public void testBetweenWithNullBounds() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        FieldReferenceExpression field = field(0, DataTypes.BIGINT());
        Predicate nullLowerBound =
                call(
                                BuiltInFunctionDefinitions.BETWEEN,
                                field,
                                literal(null, DataTypes.BIGINT()),
                                literal(20))
                        .accept(converter);
        Predicate nullUpperBound =
                call(
                                BuiltInFunctionDefinitions.BETWEEN,
                                field,
                                literal(10),
                                literal(null, DataTypes.BIGINT()))
                        .accept(converter);

        assertThat(nullLowerBound.test(GenericRow.of(15L))).isFalse();
        assertThat(nullUpperBound.test(GenericRow.of(15L))).isFalse();
    }

    @Test
    public void testInAndNotInRowSemantics() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        CallExpression in =
                call(
                        BuiltInFunctionDefinitions.IN,
                        field(0, DataTypes.BIGINT()),
                        literal(1, DataTypes.INT()),
                        literal(null, DataTypes.BIGINT()),
                        literal(3, DataTypes.INT()));

        Predicate inPredicate = in.accept(converter);
        Predicate notInPredicate = call(BuiltInFunctionDefinitions.NOT, in).accept(converter);

        assertThat(inPredicate).isEqualTo(builder.in(0, Arrays.asList(1L, null, 3L)));
        assertThat(notInPredicate).isEqualTo(builder.notIn(0, Arrays.asList(1L, null, 3L)));
        assertThat(inPredicate.test(GenericRow.of(1L))).isTrue();
        assertThat(inPredicate.test(GenericRow.of(2L))).isFalse();
        assertThat(inPredicate.test(GenericRow.of(3L))).isTrue();
        assertThat(inPredicate.test(GenericRow.of((Object) null))).isFalse();
        for (Object value : Arrays.asList(null, 1L, 2L, 3L, 4L)) {
            assertThat(notInPredicate.test(GenericRow.of(value))).isFalse();
        }
    }

    @Test
    public void testLargeInAndNotIn() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        List<ResolvedExpression> children = new ArrayList<>();
        List<Object> expectedLiterals = new ArrayList<>();
        children.add(field(0, DataTypes.BIGINT()));
        for (int i = 0; i < 21; i++) {
            children.add(literal(i, DataTypes.INT()));
            expectedLiterals.add((long) i);
        }
        CallExpression in =
                new CallExpression(
                        false, null, BuiltInFunctionDefinitions.IN, children, DataTypes.BOOLEAN());

        Predicate inPredicate = in.accept(converter);
        Predicate notInPredicate = call(BuiltInFunctionDefinitions.NOT, in).accept(converter);

        assertThat(inPredicate).isEqualTo(builder.in(0, expectedLiterals));
        assertThat(inPredicate).isInstanceOf(LeafPredicate.class);
        assertThat(((LeafPredicate) inPredicate).function()).isEqualTo(In.INSTANCE);
        assertThat(((LeafPredicate) inPredicate).literals())
                .containsExactlyElementsOf(expectedLiterals);
        assertThat(notInPredicate).isEqualTo(builder.notIn(0, expectedLiterals));
        assertThat(inPredicate.test(GenericRow.of(20L))).isTrue();
        assertThat(inPredicate.test(GenericRow.of(21L))).isFalse();
        assertThat(inPredicate.test(GenericRow.of((Object) null))).isFalse();
        assertThat(notInPredicate.test(GenericRow.of(20L))).isFalse();
        assertThat(notInPredicate.test(GenericRow.of(21L))).isTrue();
        assertThat(notInPredicate.test(GenericRow.of((Object) null))).isFalse();
    }

    @Test
    public void testNotBetweenStructure() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        CallExpression between =
                call(
                        BuiltInFunctionDefinitions.BETWEEN,
                        field(0, DataTypes.BIGINT()),
                        literal(10, DataTypes.INT()),
                        literal(20, DataTypes.INT()));

        Predicate predicate = call(BuiltInFunctionDefinitions.NOT, between).accept(converter);

        assertThat(predicate).isEqualTo(builder.between(0, 10L, 20L).negate().get());
        assertThat(predicate).isInstanceOf(LeafPredicate.class);
        assertThat(((LeafPredicate) predicate).function()).isEqualTo(NotBetween.INSTANCE);
        assertThat(((LeafPredicate) predicate).literals()).containsExactly(10L, 20L);
    }

    @MethodSource("provideNegatedComparisons")
    @ParameterizedTest
    public void testNegatedComparisons(
            FunctionDefinition function, boolean literalOnLeft, Predicate expected) {
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        ResolvedExpression field = field(0, DataTypes.BIGINT());
        ResolvedExpression literal = literal(1, DataTypes.INT());
        CallExpression comparison =
                literalOnLeft ? call(function, literal, field) : call(function, field, literal);

        assertThat(call(BuiltInFunctionDefinitions.NOT, comparison).accept(converter))
                .isEqualTo(expected);
    }

    public static Stream<Arguments> provideNegatedComparisons() {
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        return Stream.of(
                Arguments.of(BuiltInFunctionDefinitions.EQUALS, false, builder.notEqual(0, 1L)),
                Arguments.of(BuiltInFunctionDefinitions.NOT_EQUALS, false, builder.equal(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.GREATER_THAN, false, builder.lessOrEqual(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        false,
                        builder.lessThan(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.LESS_THAN, false, builder.greaterOrEqual(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        false,
                        builder.greaterThan(0, 1L)),
                Arguments.of(BuiltInFunctionDefinitions.EQUALS, true, builder.notEqual(0, 1L)),
                Arguments.of(BuiltInFunctionDefinitions.NOT_EQUALS, true, builder.equal(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        true,
                        builder.greaterOrEqual(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        true,
                        builder.greaterThan(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.LESS_THAN, true, builder.lessOrEqual(0, 1L)),
                Arguments.of(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        true,
                        builder.lessThan(0, 1L)));
    }

    @Test
    public void testGenericNotComparisonAndDoubleNot() {
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        CallExpression equal =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, DataTypes.BIGINT()),
                        literal(10, DataTypes.INT()));
        CallExpression equalNull =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, DataTypes.BIGINT()),
                        literal(null, DataTypes.BIGINT()));

        Predicate notEqual = call(BuiltInFunctionDefinitions.NOT, equal).accept(converter);
        Predicate notEqualNull = call(BuiltInFunctionDefinitions.NOT, equalNull).accept(converter);
        Predicate doubleNot =
                call(BuiltInFunctionDefinitions.NOT, call(BuiltInFunctionDefinitions.NOT, equal))
                        .accept(converter);

        assertThat(notEqual).isEqualTo(builder.notEqual(0, 10L));
        assertThat(notEqualNull).isEqualTo(builder.notEqual(0, null));
        assertThat(doubleNot).isEqualTo(builder.equal(0, 10L));
        assertThat(notEqual.test(GenericRow.of(9L))).isTrue();
        assertThat(notEqual.test(GenericRow.of(10L))).isFalse();
        assertThat(notEqual.test(GenericRow.of((Object) null))).isFalse();
        assertThat(notEqualNull.test(GenericRow.of(10L))).isFalse();
        assertThat(notEqualNull.test(GenericRow.of((Object) null))).isFalse();
    }

    @Test
    public void testGenericNotAndOr() {
        PredicateBuilder builder = predicateBuilder(RowType.of(new BigIntType()));
        PredicateConverter converter = new PredicateConverter(RowType.of(new BigIntType()));
        CallExpression equal10 =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, DataTypes.BIGINT()),
                        literal(10L, DataTypes.BIGINT()));
        CallExpression equal20 =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, DataTypes.BIGINT()),
                        literal(20L, DataTypes.BIGINT()));

        Predicate notAnd =
                call(
                                BuiltInFunctionDefinitions.NOT,
                                call(BuiltInFunctionDefinitions.AND, equal10, equal20))
                        .accept(converter);
        Predicate notOr =
                call(
                                BuiltInFunctionDefinitions.NOT,
                                call(BuiltInFunctionDefinitions.OR, equal10, equal20))
                        .accept(converter);

        assertThat(notAnd)
                .isEqualTo(PredicateBuilder.or(builder.notEqual(0, 10L), builder.notEqual(0, 20L)));
        assertThat(notOr)
                .isEqualTo(
                        PredicateBuilder.and(builder.notEqual(0, 10L), builder.notEqual(0, 20L)));
        assertThat(notAnd.test(GenericRow.of(10L))).isTrue();
        assertThat(notAnd.test(GenericRow.of((Object) null))).isFalse();
        assertThat(notOr.test(GenericRow.of(10L))).isFalse();
        assertThat(notOr.test(GenericRow.of(15L))).isTrue();
        assertThat(notOr.test(GenericRow.of((Object) null))).isFalse();
    }

    @Test
    public void testBooleanTruthPredicatesAndNot() {
        PredicateBuilder builder =
                predicateBuilder(RowType.of(DataTypes.BOOLEAN().getLogicalType()));
        PredicateConverter converter =
                new PredicateConverter(RowType.of(DataTypes.BOOLEAN().getLogicalType()));
        ResolvedExpression boolField = field(0, DataTypes.BOOLEAN());
        CallExpression isTrue = call(BuiltInFunctionDefinitions.IS_TRUE, boolField);
        CallExpression isFalse = call(BuiltInFunctionDefinitions.IS_FALSE, boolField);
        CallExpression isNotTrue = call(BuiltInFunctionDefinitions.IS_NOT_TRUE, boolField);
        CallExpression isNotFalse = call(BuiltInFunctionDefinitions.IS_NOT_FALSE, boolField);

        Predicate truePredicate = isTrue.accept(converter);
        Predicate falsePredicate = isFalse.accept(converter);
        Predicate notTruePredicate = isNotTrue.accept(converter);
        Predicate notFalsePredicate = isNotFalse.accept(converter);

        assertThat(truePredicate).isEqualTo(builder.equal(0, true));
        assertThat(falsePredicate).isEqualTo(builder.equal(0, false));
        assertBooleanResults(truePredicate, true, false, false);
        assertBooleanResults(falsePredicate, false, true, false);
        assertBooleanResults(notTruePredicate, false, true, true);
        assertBooleanResults(notFalsePredicate, true, false, true);
        assertBooleanResults(
                call(BuiltInFunctionDefinitions.NOT, isTrue).accept(converter), false, true, true);
        assertBooleanResults(
                call(BuiltInFunctionDefinitions.NOT, isFalse).accept(converter), true, false, true);
        assertBooleanResults(
                call(BuiltInFunctionDefinitions.NOT, isNotTrue).accept(converter),
                true,
                false,
                false);
        assertBooleanResults(
                call(BuiltInFunctionDefinitions.NOT, isNotFalse).accept(converter),
                false,
                true,
                false);
    }

    @Test
    public void testUnsupportedNotLike() {
        RowType rowType = RowType.of(new VarCharType());
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        CallExpression unsupportedLike =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("%middle%", STRING()));

        assertThatThrownBy(
                        () ->
                                call(BuiltInFunctionDefinitions.NOT, unsupportedLike)
                                        .accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);

        CallExpression prefixLike =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("prefix%", STRING()));
        CallExpression notPrefixLike = call(BuiltInFunctionDefinitions.NOT, prefixLike);
        assertThatThrownBy(() -> notPrefixLike.accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        assertThat(PredicateConverter.convert(rowType, notPrefixLike)).isEmpty();
    }

    private static void assertBooleanResults(
            Predicate predicate,
            boolean expectedForTrue,
            boolean expectedForFalse,
            boolean expectedForNull) {
        assertThat(predicate.test(GenericRow.of(true))).isEqualTo(expectedForTrue);
        assertThat(predicate.test(GenericRow.of(false))).isEqualTo(expectedForFalse);
        assertThat(predicate.test(GenericRow.of((Object) null))).isEqualTo(expectedForNull);
    }

    private static PredicateBuilder predicateBuilder(RowType rowType) {
        return new PredicateBuilder(LogicalTypeConversion.toDataType(rowType));
    }

    @MethodSource("provideLikeExpressions")
    @ParameterizedTest
    public void testStartsWith(
            CallExpression callExpression,
            List<Object[]> valuesList,
            List<Boolean> expectedForValues,
            List<Long> rowCountList,
            List<SimpleColStats[]> statsList,
            List<Boolean> expectedForStats) {
        Predicate predicate =
                callExpression.accept(new PredicateConverter(RowType.of(new VarCharType())));
        IntStream.range(0, valuesList.size())
                .forEach(
                        i ->
                                assertThat(predicate.test(GenericRow.of(valuesList.get(i))))
                                        .isEqualTo(expectedForValues.get(i)));
        IntStream.range(0, rowCountList.size())
                .forEach(
                        i ->
                                assertThat(
                                                SimpleColStatsTestUtils.test(
                                                        predicate,
                                                        rowCountList.get(i),
                                                        statsList.get(i)))
                                        .isEqualTo(expectedForStats.get(i)));
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
                        new Object[] {BinaryString.fromString("a")},
                        new Object[] {BinaryString.fromString("ab")},
                        new Object[] {BinaryString.fromString("abd")},
                        new Object[] {BinaryString.fromString("abd%")},
                        new Object[] {BinaryString.fromString("abd1")},
                        new Object[] {BinaryString.fromString("abde@")},
                        new Object[] {BinaryString.fromString("abd_")},
                        new Object[] {BinaryString.fromString("abd_%")});
        List<Boolean> expectedForValues1 =
                Arrays.asList(false, false, false, true, true, true, true, true, true);
        List<Long> rowCountList1 = Arrays.asList(0L, 3L, 3L, 3L);
        List<SimpleColStats[]> statsList1 =
                Arrays.asList(
                        new SimpleColStats[] {new SimpleColStats(null, null, 0L)},
                        new SimpleColStats[] {new SimpleColStats(null, null, 3L)},
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("ab"),
                                    BinaryString.fromString("abc123"),
                                    1L)
                        },
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("abc"),
                                    BinaryString.fromString("abe"),
                                    1L)
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
                        new Object[] {BinaryString.fromString("test%")},
                        new Object[] {BinaryString.fromString("test_123")},
                        new Object[] {BinaryString.fromString("test_%")},
                        new Object[] {BinaryString.fromString("test__")});
        List<Boolean> expectedForValues2 = Arrays.asList(false, true, true, true);
        List<Long> rowCountList2 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList2 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("test_123"),
                                    BinaryString.fromString("test_789"),
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
                        new Object[] {BinaryString.fromString("axyz")},
                        new Object[] {BinaryString.fromString("bxyz")},
                        new Object[] {BinaryString.fromString("cxyz")},
                        new Object[] {BinaryString.fromString("[a-c]xyz")});
        List<Boolean> expectedForValues3 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList3 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList3 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("[a-c]xyz"),
                                    BinaryString.fromString("[a-c]xyzz"),
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
                        new Object[] {BinaryString.fromString("exyz")},
                        new Object[] {BinaryString.fromString("fxyz")},
                        new Object[] {BinaryString.fromString("axyz")},
                        new Object[] {BinaryString.fromString("[^a-d]xyz")});
        List<Boolean> expectedForValues4 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList4 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList4 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("[^a-d]xyz"),
                                    BinaryString.fromString("[^a-d]xyzz"),
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

    @Test
    public void testUnsupportedFieldReferenceExpression() {
        PredicateConverter converter = new PredicateConverter(RowType.of(new VarCharType()));
        DataType structType = DataTypes.ROW(DataTypes.INT()).bridgedTo(Row.class);
        assertThatThrownBy(() -> field(0, structType).accept(converter))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    // ==================== Nested OR Conversion Tests ====================
    //
    // When Flink expands IN(v1,...,vN) it produces a deeply nested binary OR tree:
    //   OR(=(f,v1), OR(=(f,v2), OR(..., =(f,vN))))
    // PredicateConverter iteratively flattens this tree into a list of predicates,
    // which PredicateBuilder.or() combines into a binary tree.

    @Test
    public void testNestedOrOfEquals() {
        // Build OR(=(long1, 0), OR(=(long1, 1), ...)) with 25 values
        FieldReferenceExpression longRef =
                new FieldReferenceExpression(
                        "long1", DataTypes.BIGINT(), Integer.MAX_VALUE, Integer.MAX_VALUE);

        ResolvedExpression orTree = null;
        for (int i = 24; i >= 0; i--) {
            CallExpression equal =
                    call(BuiltInFunctionDefinitions.EQUALS, longRef, new ValueLiteralExpression(i));
            if (orTree == null) {
                orTree = equal;
            } else {
                orTree = call(BuiltInFunctionDefinitions.OR, equal, orTree);
            }
        }

        Predicate result = CONVERTER.visit((CallExpression) orTree);

        // OR-of-equals → flattened → PredicateBuilder.or() → binary tree
        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) result;
        assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        assertThat(compound.children()).hasSize(2);
    }

    @Test
    public void testNestedOrOfDifferentPredicates() {
        // Build OR(>(long1, 0), OR(>(long1, 1), ...)) with 25 values
        FieldReferenceExpression longRef =
                new FieldReferenceExpression(
                        "long1", DataTypes.BIGINT(), Integer.MAX_VALUE, Integer.MAX_VALUE);

        ResolvedExpression orTree = null;
        for (int i = 24; i >= 0; i--) {
            CallExpression greater =
                    call(
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            longRef,
                            new ValueLiteralExpression(i));
            if (orTree == null) {
                orTree = greater;
            } else {
                orTree = call(BuiltInFunctionDefinitions.OR, greater, orTree);
            }
        }

        Predicate result = CONVERTER.visit((CallExpression) orTree);

        // General OR → binary tree (root has 2 children)
        assertThat(result).isInstanceOf(CompoundPredicate.class);
        CompoundPredicate compound = (CompoundPredicate) result;
        assertThat(compound.function()).isEqualTo(Or.INSTANCE);
        assertThat(compound.children()).hasSize(2);
    }

    private static FieldReferenceExpression field(int i, DataType type) {
        return new FieldReferenceExpression("f" + i, type, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private static CallExpression call(FunctionDefinition function, ResolvedExpression... args) {
        return new CallExpression(false, null, function, Arrays.asList(args), DataTypes.BOOLEAN());
    }
}
