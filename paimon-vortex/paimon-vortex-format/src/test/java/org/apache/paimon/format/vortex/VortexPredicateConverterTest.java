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

package org.apache.paimon.format.vortex;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import dev.vortex.api.Expression;
import dev.vortex.api.expressions.Binary;
import dev.vortex.api.expressions.GetItem;
import dev.vortex.api.expressions.Literal;
import dev.vortex.api.expressions.Not;
import dev.vortex.api.expressions.Root;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for {@link VortexPredicateConverter}. */
public class VortexPredicateConverterTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("f_int", DataTypes.INT())
                    .field("f_bigint", DataTypes.BIGINT())
                    .field("f_string", DataTypes.STRING())
                    .build();

    private static final PredicateBuilder BUILDER = new PredicateBuilder(ROW_TYPE);

    private static Expression field(String name) {
        return GetItem.of(Root.INSTANCE, name);
    }

    @Test
    public void testEqual() {
        Predicate predicate = BUILDER.equal(0, 42);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.eq(field("f_int"), Literal.int32(42)), result);
    }

    @Test
    public void testNotEqual() {
        Predicate predicate = BUILDER.notEqual(1, 100L);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.notEq(field("f_bigint"), Literal.int64(100L)), result);
    }

    @Test
    public void testGreaterThan() {
        Predicate predicate = BUILDER.greaterThan(0, 10);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.gt(field("f_int"), Literal.int32(10)), result);
    }

    @Test
    public void testGreaterOrEqual() {
        Predicate predicate = BUILDER.greaterOrEqual(0, 10);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.gtEq(field("f_int"), Literal.int32(10)), result);
    }

    @Test
    public void testLessThan() {
        Predicate predicate = BUILDER.lessThan(1, 50L);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.lt(field("f_bigint"), Literal.int64(50L)), result);
    }

    @Test
    public void testLessOrEqual() {
        Predicate predicate = BUILDER.lessOrEqual(1, 50L);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.ltEq(field("f_bigint"), Literal.int64(50L)), result);
    }

    @Test
    public void testIsNull() {
        Predicate predicate = BUILDER.isNull(0);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Not.of(Binary.notEq(field("f_int"), Literal.nullLit())), result);
    }

    @Test
    public void testIsNotNull() {
        Predicate predicate = BUILDER.isNotNull(0);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.notEq(field("f_int"), Literal.nullLit()), result);
    }

    @Test
    public void testAnd() {
        Predicate p1 = BUILDER.greaterThan(0, 10);
        Predicate p2 = BUILDER.lessThan(0, 100);
        Predicate and = PredicateBuilder.and(p1, p2);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(and));
        assertEquals(
                Binary.and(
                        Binary.gt(field("f_int"), Literal.int32(10)),
                        Binary.lt(field("f_int"), Literal.int32(100))),
                result);
    }

    @Test
    public void testOr() {
        Predicate p1 = BUILDER.equal(0, 1);
        Predicate p2 = BUILDER.equal(0, 2);
        Predicate or = PredicateBuilder.or(p1, p2);
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(or));
        assertEquals(
                Binary.or(
                        Binary.eq(field("f_int"), Literal.int32(1)),
                        Binary.eq(field("f_int"), Literal.int32(2))),
                result);
    }

    @Test
    public void testMultiplePredicatesAsAnd() {
        Predicate p1 = BUILDER.greaterThan(0, 5);
        Predicate p2 = BUILDER.lessThan(1, 200L);
        Expression result = VortexPredicateConverter.toVortexExpression(Arrays.asList(p1, p2));
        assertEquals(
                Binary.and(
                        Binary.gt(field("f_int"), Literal.int32(5)),
                        Binary.lt(field("f_bigint"), Literal.int64(200L))),
                result);
    }

    @Test
    public void testNullPredicates() {
        assertNull(VortexPredicateConverter.toVortexExpression(null));
    }

    @Test
    public void testEmptyPredicates() {
        assertNull(VortexPredicateConverter.toVortexExpression(Collections.emptyList()));
    }

    @Test
    public void testStringLiteral() {
        Predicate predicate =
                BUILDER.equal(2, org.apache.paimon.data.BinaryString.fromString("hello"));
        Expression result =
                VortexPredicateConverter.toVortexExpression(Collections.singletonList(predicate));
        assertEquals(Binary.eq(field("f_string"), Literal.string("hello")), result);
    }
}
