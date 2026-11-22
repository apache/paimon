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

import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Binary literals must keep their bytes when pushed down, including implicit widening casts. */
class BinaryLiteralPredicateConverterTest {
    @Test
    void testBinaryToVarbinaryComparisons() {
        byte[] value = new byte[] {0, 1, (byte) 0x80, (byte) 0xff, 0};
        for (BuiltInFunctionDefinition function :
                Arrays.asList(
                        BuiltInFunctionDefinitions.EQUALS,
                        BuiltInFunctionDefinitions.NOT_EQUALS,
                        BuiltInFunctionDefinitions.GREATER_THAN,
                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                        BuiltInFunctionDefinitions.LESS_THAN,
                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL)) {
            for (boolean reversed : new boolean[] {false, true}) {
                Predicate predicate =
                        convert(function, DataTypes.BYTES(), DataTypes.BINARY(5), value, reversed);
                assertThat((byte[]) ((LeafPredicate) predicate).literals().get(0)).isEqualTo(value);
            }
        }
    }

    @Test
    void testSameRootAndWiderVariableBinary() {
        byte[] value = new byte[] {0, (byte) 0xff};
        for (DataType literalType : Arrays.asList(DataTypes.BINARY(2), DataTypes.VARBINARY(2))) {
            Predicate predicate =
                    convert(
                            BuiltInFunctionDefinitions.EQUALS,
                            DataTypes.VARBINARY(8),
                            literalType,
                            value,
                            false);
            assertThat((byte[]) ((LeafPredicate) predicate).literals().get(0)).isEqualTo(value);
        }
    }

    @Test
    void testNullBinaryLiteral() {
        Predicate predicate =
                convert(
                        BuiltInFunctionDefinitions.EQUALS,
                        DataTypes.BYTES(),
                        DataTypes.BINARY(2),
                        null,
                        false);
        assertThat(((LeafPredicate) predicate).literals()).containsExactly((Object) null);
    }

    @Test
    void testNarrowingBinaryIsNotPushedDown() {
        assertThatThrownBy(
                        () ->
                                convert(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        DataTypes.VARBINARY(1),
                                        DataTypes.BINARY(2),
                                        new byte[] {0, 1},
                                        false))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    private static Predicate convert(
            BuiltInFunctionDefinition function,
            DataType fieldType,
            DataType literalType,
            byte[] value,
            boolean reversed) {
        ResolvedExpression field = new FieldReferenceExpression("k", fieldType, 0, 0);
        ResolvedExpression literal =
                new ValueLiteralExpression(
                        value, value == null ? literalType.nullable() : literalType.notNull());
        CallExpression call =
                CallExpression.permanent(
                        function,
                        reversed ? Arrays.asList(literal, field) : Arrays.asList(field, literal),
                        DataTypes.BOOLEAN());
        return new PredicateConverter(
                        (RowType) DataTypes.ROW(DataTypes.FIELD("k", fieldType)).getLogicalType())
                .visit(call);
    }
}
