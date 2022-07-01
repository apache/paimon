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
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PredicateConverter}. */
public class PredicateConverterTest {

    private static final PredicateBuilder BUILDER =
            new PredicateBuilder(RowType.of(new BigIntType(), new DoubleType()));

    private static final PredicateConverter CONVERTER = new PredicateConverter(BUILDER);

    @MethodSource("provideResolvedExpression")
    @ParameterizedTest
    public void testVisitAndAutoTypeInference(ResolvedExpression expression, Predicate expected) {
        if (expression instanceof CallExpression) {
            assertThat(CONVERTER.visit((CallExpression) expression)).isEqualTo(expected);
        } else {
            assertThatThrownBy(() -> CONVERTER.visit(expression))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Unsupported expression");
        }
    }

    public static Stream<Arguments> provideResolvedExpression() {
        FieldReferenceExpression longRefExpr =
                new FieldReferenceExpression("long1", DataTypes.BIGINT(), 0, 0);
        ValueLiteralExpression intLitExpr = new ValueLiteralExpression(10);
        long longLit = 10L;
        ValueLiteralExpression nullLongLitExpr =
                new ValueLiteralExpression(null, DataTypes.BIGINT());

        FieldReferenceExpression doubleRefExpr =
                new FieldReferenceExpression("double1", DataTypes.DOUBLE(), 0, 1);
        ValueLiteralExpression floatLitExpr = new ValueLiteralExpression(3.14f);
        double doubleLit = 3.14d;

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
                                BUILDER.notEqual(0, longLit), BUILDER.equal(1, doubleLit))));
    }
}
