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

    private static final PredicateConverter CONVERTER = new PredicateConverter();

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
        Literal longLit = new Literal(DataTypes.BIGINT().getLogicalType(), 10L);

        FieldReferenceExpression doubleRefExpr =
                new FieldReferenceExpression("double1", DataTypes.DOUBLE(), 0, 1);
        ValueLiteralExpression floatLitExpr = new ValueLiteralExpression(3.14f);
        Literal doubleLit = new Literal(DataTypes.DOUBLE().getLogicalType(), 3.14d);

        return Stream.of(
                Arguments.of(longRefExpr, null),
                Arguments.of(intLitExpr, null),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NULL,
                                Collections.singletonList(longRefExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.isNull(0)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NOT_NULL,
                                Collections.singletonList(doubleRefExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.isNotNull(1)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                // test literal on left
                                Arrays.asList(intLitExpr, longRefExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.equal(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.notEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.greaterThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.greaterOrEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.lessThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.lessOrEqual(0, longLit)),
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
                                PredicateBuilder.lessOrEqual(0, longLit),
                                PredicateBuilder.equal(1, doubleLit))),
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
                                PredicateBuilder.notEqual(0, longLit),
                                PredicateBuilder.equal(1, doubleLit))));
    }
}
