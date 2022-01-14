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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.store.file.predicate.PredicateConverter.CONVERTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Predicate}s. */
public class PredicateTest {

    @Test
    public void testEqual() {
        CallExpression expression =
                call(BuiltInFunctionDefinitions.EQUALS, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual() {
        CallExpression expression =
                call(BuiltInFunctionDefinitions.NOT_EQUALS, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 5, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(0, 6, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(5, 5, 0)})).isEqualTo(false);
    }

    @Test
    public void testGreater() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(CONVERTER);

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
    }

    @Test
    public void testGreaterReverse() {
        CallExpression expression =
                call(BuiltInFunctionDefinitions.LESS_THAN, literal(5), field(0, DataTypes.INT()));
        Predicate predicate = expression.accept(CONVERTER);

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
    }

    @Test
    public void testGreaterOrEqual() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(CONVERTER);

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
    }

    @Test
    public void testLess() {
        CallExpression expression =
                call(BuiltInFunctionDefinitions.LESS_THAN, field(0, DataTypes.INT()), literal(5));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);
    }

    @Test
    public void testLessOrEqual() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                        field(0, DataTypes.INT()),
                        literal(5));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {5})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {6})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(4, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(1, new FieldStats[] {new FieldStats(null, null, 1)}))
                .isEqualTo(false);
    }

    @Test
    public void testIsNull() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.IS_NULL,
                        field(0, DataTypes.INT()),
                        literal(null, DataTypes.INT()));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(false);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(true);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(false);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1)})).isEqualTo(true);
    }

    @Test
    public void testIsNotNull() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.IS_NOT_NULL,
                        field(0, DataTypes.INT()),
                        literal(null, DataTypes.INT()));
        Predicate predicate = expression.accept(CONVERTER);

        assertThat(predicate.test(new Object[] {4})).isEqualTo(true);
        assertThat(predicate.test(new Object[] {null})).isEqualTo(false);

        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(6, 7, 0)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(5, 7, 1)})).isEqualTo(true);
        assertThat(predicate.test(3, new FieldStats[] {new FieldStats(null, null, 3)}))
                .isEqualTo(false);
    }

    @Test
    public void testAnd() {
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
        Predicate predicate = expression.accept(CONVERTER);

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
    }

    @Test
    public void testOr() {
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
        Predicate predicate = expression.accept(CONVERTER);

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
                                BuiltInFunctionDefinitions.LIKE,
                                field(1, DataTypes.INT()),
                                literal(5)));
        assertThatThrownBy(() -> expression.accept(CONVERTER))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    @Test
    public void testUnsupportedType() {
        DataType structType = DataTypes.ROW(DataTypes.INT()).bridgedTo(Row.class);
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        field(0, structType),
                        literal(Row.of(1), structType));
        assertThatThrownBy(() -> expression.accept(CONVERTER))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    private FieldReferenceExpression field(int i, DataType type) {
        return new FieldReferenceExpression("name", type, 0, i);
    }

    private CallExpression call(FunctionDefinition function, ResolvedExpression... args) {
        return new CallExpression(function, Arrays.asList(args), DataTypes.BOOLEAN());
    }
}
