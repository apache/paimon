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
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.NestedFieldTransform;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.NestedFieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link PredicateConverter} converts a predicate on a field nested inside a row, which
 * Flink hands down as a {@code NestedFieldReferenceExpression}.
 */
public class NestedPredicateConverterTest {

    private static final RowType DEEP =
            RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"x"});

    private static final RowType NESTED =
            RowType.of(
                    new DataType[] {
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.DOUBLE(),
                        DataTypes.BOOLEAN(),
                        DEEP
                    },
                    new String[] {"a", "b", "d", "flag", "inner"});

    private static final RowType TABLE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), NESTED, DataTypes.INT()},
                    new String[] {"pk", "s", "t"});

    /**
     * The type the converter itself works on. Round-tripping through Flink's type system is what
     * {@link PredicateConverter#convert} does, and it assigns its own field ids, so expected
     * predicates have to be built from the same type to compare equal.
     */
    private static final RowType CONVERTED =
            LogicalTypeConversion.toDataType(LogicalTypeConversion.toLogicalType(TABLE));

    private static final PredicateBuilder BUILDER = new PredicateBuilder(CONVERTED);

    private static NestedFieldTransform transform(String... path) {
        return new NestedFieldTransform(
                new FieldRef(1, "s", CONVERTED.getTypeAt(1)), Arrays.asList(path));
    }

    private static final NestedFieldTransform S_A = transform("a");
    private static final NestedFieldTransform S_B = transform("b");
    private static final NestedFieldTransform S_D = transform("d");
    private static final NestedFieldTransform S_FLAG = transform("flag");
    private static final NestedFieldTransform S_INNER_X = transform("inner", "x");

    private static NestedFieldReferenceExpression ref(
            org.apache.flink.table.types.DataType type, String... path) {
        int[] indices = new int[path.length];
        return new NestedFieldReferenceExpression(path, indices, type);
    }

    private static NestedFieldReferenceExpression intRef(String... path) {
        return ref(org.apache.flink.table.api.DataTypes.INT(), path);
    }

    private static ResolvedExpression call(
            BuiltInFunctionDefinition func, ResolvedExpression... children) {
        return CallExpression.permanent(
                func, Arrays.asList(children), org.apache.flink.table.api.DataTypes.BOOLEAN());
    }

    private static ResolvedExpression not(ResolvedExpression child) {
        return call(BuiltInFunctionDefinitions.NOT, child);
    }

    private static Predicate convert(ResolvedExpression expression) {
        return PredicateConverter.convert(LogicalTypeConversion.toLogicalType(TABLE), expression)
                .orElse(null);
    }

    // ------------------------------------------------------------------------------------
    // comparisons
    // ------------------------------------------------------------------------------------

    @Test
    public void testNestedComparisons() {
        ValueLiteralExpression seven = new ValueLiteralExpression(7);

        assertThat(convert(call(BuiltInFunctionDefinitions.EQUALS, intRef("s", "a"), seven)))
                .isEqualTo(BUILDER.equal(S_A, 7));
        assertThat(convert(call(BuiltInFunctionDefinitions.NOT_EQUALS, intRef("s", "a"), seven)))
                .isEqualTo(BUILDER.notEqual(S_A, 7));
        assertThat(convert(call(BuiltInFunctionDefinitions.GREATER_THAN, intRef("s", "a"), seven)))
                .isEqualTo(BUILDER.greaterThan(S_A, 7));
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                        intRef("s", "a"),
                                        seven)))
                .isEqualTo(BUILDER.greaterOrEqual(S_A, 7));
        assertThat(convert(call(BuiltInFunctionDefinitions.LESS_THAN, intRef("s", "a"), seven)))
                .isEqualTo(BUILDER.lessThan(S_A, 7));
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                        intRef("s", "a"),
                                        seven)))
                .isEqualTo(BUILDER.lessOrEqual(S_A, 7));
    }

    /** The field may be on either side of the comparison; the operator flips with it. */
    @Test
    public void testNestedComparisonWithLiteralOnTheLeft() {
        ValueLiteralExpression seven = new ValueLiteralExpression(7);

        assertThat(convert(call(BuiltInFunctionDefinitions.GREATER_THAN, seven, intRef("s", "a"))))
                .isEqualTo(BUILDER.lessThan(S_A, 7));
        assertThat(convert(call(BuiltInFunctionDefinitions.LESS_THAN, seven, intRef("s", "a"))))
                .isEqualTo(BUILDER.greaterThan(S_A, 7));
    }

    @Test
    public void testNegatedNestedComparison() {
        ValueLiteralExpression seven = new ValueLiteralExpression(7);

        assertThat(convert(not(call(BuiltInFunctionDefinitions.EQUALS, intRef("s", "a"), seven))))
                .isEqualTo(BUILDER.notEqual(S_A, 7));
        assertThat(
                        convert(
                                not(
                                        call(
                                                BuiltInFunctionDefinitions.GREATER_THAN,
                                                intRef("s", "a"),
                                                seven))))
                .isEqualTo(BUILDER.lessOrEqual(S_A, 7));
    }

    // ------------------------------------------------------------------------------------
    // set, range, null and string predicates
    // ------------------------------------------------------------------------------------

    @Test
    public void testNestedInAndNotIn() {
        ResolvedExpression in =
                call(
                        BuiltInFunctionDefinitions.IN,
                        intRef("s", "a"),
                        new ValueLiteralExpression(1),
                        new ValueLiteralExpression(2));

        assertThat(convert(in)).isEqualTo(BUILDER.in(S_A, Arrays.asList(1, 2)));
        assertThat(convert(not(in))).isEqualTo(BUILDER.notIn(S_A, Arrays.asList(1, 2)));
    }

    /** {@code v NOT IN (..., NULL, ...)} is never true, whatever the field. */
    @Test
    public void testNestedNotInWithNullLiteralIsAlwaysFalse() {
        ResolvedExpression in =
                call(
                        BuiltInFunctionDefinitions.IN,
                        intRef("s", "a"),
                        new ValueLiteralExpression(1),
                        new ValueLiteralExpression(
                                null, org.apache.flink.table.api.DataTypes.INT()));

        assertThat(convert(not(in))).isEqualTo(PredicateBuilder.alwaysFalse());
    }

    @Test
    public void testNestedIsNullAndIsNotNull() {
        assertThat(convert(call(BuiltInFunctionDefinitions.IS_NULL, intRef("s", "a"))))
                .isEqualTo(BUILDER.isNull(S_A));
        assertThat(convert(call(BuiltInFunctionDefinitions.IS_NOT_NULL, intRef("s", "a"))))
                .isEqualTo(BUILDER.isNotNull(S_A));
        assertThat(convert(not(call(BuiltInFunctionDefinitions.IS_NULL, intRef("s", "a")))))
                .isEqualTo(BUILDER.isNotNull(S_A));
    }

    @Test
    public void testNestedBetweenAndNotBetween() {
        ResolvedExpression between =
                call(
                        BuiltInFunctionDefinitions.BETWEEN,
                        intRef("s", "a"),
                        new ValueLiteralExpression(1),
                        new ValueLiteralExpression(3));

        assertThat(convert(between)).isEqualTo(BUILDER.between(S_A, 1, 3));
        assertThat(convert(not(between)))
                .isEqualTo(BUILDER.between(S_A, 1, 3).negate().orElseThrow(AssertionError::new));
    }

    @Test
    public void testNestedLikePrefixBecomesStartsWith() {
        ResolvedExpression like =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        ref(org.apache.flink.table.api.DataTypes.STRING(), "s", "b"),
                        new ValueLiteralExpression("ab%"));

        assertThat(convert(like)).isEqualTo(BUILDER.startsWith(S_B, BinaryString.fromString("ab")));
    }

    @Test
    public void testNestedBooleanTests() {
        NestedFieldReferenceExpression flag =
                ref(org.apache.flink.table.api.DataTypes.BOOLEAN(), "s", "flag");

        assertThat(convert(call(BuiltInFunctionDefinitions.IS_TRUE, flag)))
                .isEqualTo(BUILDER.equal(S_FLAG, true));
        assertThat(convert(call(BuiltInFunctionDefinitions.IS_FALSE, flag)))
                .isEqualTo(BUILDER.equal(S_FLAG, false));
        assertThat(convert(call(BuiltInFunctionDefinitions.IS_NOT_TRUE, flag)))
                .isEqualTo(
                        PredicateBuilder.or(
                                BUILDER.isNull(S_FLAG), BUILDER.notEqual(S_FLAG, true)));
        assertThat(convert(call(BuiltInFunctionDefinitions.IS_NOT_FALSE, flag)))
                .isEqualTo(
                        PredicateBuilder.or(
                                BUILDER.isNull(S_FLAG), BUILDER.notEqual(S_FLAG, false)));
    }

    // ------------------------------------------------------------------------------------
    // floating point: negated comparisons are not equivalent, so they must not be pushed
    // ------------------------------------------------------------------------------------

    @Test
    public void testNegatedNestedFloatingPointIsNotConverted() {
        NestedFieldReferenceExpression d =
                ref(org.apache.flink.table.api.DataTypes.DOUBLE(), "s", "d");
        ValueLiteralExpression one = new ValueLiteralExpression(1.0d);

        assertThat(convert(not(call(BuiltInFunctionDefinitions.EQUALS, d, one)))).isNull();
        assertThat(convert(not(call(BuiltInFunctionDefinitions.GREATER_THAN, d, one)))).isNull();
        assertThat(
                        convert(
                                not(
                                        call(
                                                BuiltInFunctionDefinitions.IN,
                                                d,
                                                new ValueLiteralExpression(1.0d)))))
                .isNull();
        assertThat(
                        convert(
                                not(
                                        call(
                                                BuiltInFunctionDefinitions.BETWEEN,
                                                d,
                                                one,
                                                new ValueLiteralExpression(2.0d)))))
                .isNull();
    }

    /** A plain, non-negated comparison on a nested double is still pushed down. */
    @Test
    public void testNestedFloatingPointIsConvertedWhenNotNegated() {
        NestedFieldReferenceExpression d =
                ref(org.apache.flink.table.api.DataTypes.DOUBLE(), "s", "d");

        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.GREATER_THAN,
                                        d,
                                        new ValueLiteralExpression(1.0d))))
                .isEqualTo(BUILDER.greaterThan(S_D, 1.0d));
    }

    // ------------------------------------------------------------------------------------
    // paths
    // ------------------------------------------------------------------------------------

    @Test
    public void testPathThroughSeveralRows() {
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        intRef("s", "inner", "x"),
                                        new ValueLiteralExpression(7))))
                .isEqualTo(BUILDER.equal(S_INNER_X, 7));
    }

    @Test
    public void testNestedCombinesWithTopLevelPredicates() {
        ResolvedExpression nested =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        intRef("s", "a"),
                        new ValueLiteralExpression(7));
        ResolvedExpression topLevel =
                call(
                        BuiltInFunctionDefinitions.EQUALS,
                        new FieldReferenceExpression(
                                "pk", org.apache.flink.table.api.DataTypes.INT(), 0, 0),
                        new ValueLiteralExpression(1));

        assertThat(convert(call(BuiltInFunctionDefinitions.AND, nested, topLevel)))
                .isEqualTo(PredicateBuilder.and(BUILDER.equal(S_A, 7), BUILDER.equal(0, 1)));
        assertThat(convert(call(BuiltInFunctionDefinitions.OR, nested, topLevel)))
                .isEqualTo(PredicateBuilder.or(BUILDER.equal(S_A, 7), BUILDER.equal(0, 1)));
    }

    /** A path whose leaf is not a field of the row is left for Flink to evaluate. */
    @Test
    public void testUnknownLeafIsNotConverted() {
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        intRef("s", "missing"),
                                        new ValueLiteralExpression(7))))
                .isNull();
    }

    /** Nor is a path rooted at a field the table does not have. */
    @Test
    public void testUnknownRootIsNotConverted() {
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        intRef("missing", "a"),
                                        new ValueLiteralExpression(7))))
                .isNull();
    }

    /** Only rows can be descended into; a path rooted at a non-row field is not converted. */
    @Test
    public void testPathUnderNonRowFieldIsNotConverted() {
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        intRef("t", "a"),
                                        new ValueLiteralExpression(7))))
                .isNull();
    }

    /** A reference that names only a top-level field is not a nested path. */
    @Test
    public void testSingleComponentPathIsNotConverted() {
        assertThat(
                        convert(
                                call(
                                        BuiltInFunctionDefinitions.EQUALS,
                                        intRef("pk"),
                                        new ValueLiteralExpression(7))))
                .isNull();
    }
}
