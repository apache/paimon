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
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;

/**
 * Convert {@link Expression} to {@link Predicate}.
 *
 * <p>For {@link FieldReferenceExpression}, please use name instead of index, if the project
 * pushdown is before and the filter pushdown is after, the index of the filter will be projected.
 */
public class PredicateConverter implements ExpressionVisitor<Predicate> {

    private final PredicateBuilder builder;

    public PredicateConverter(RowType type) {
        this(new PredicateBuilder(toDataType(type)));
    }

    public PredicateConverter(PredicateBuilder builder) {
        this.builder = builder;
    }

    /** Accepts simple LIKE patterns like "abc%". */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^%]+)%");

    @Override
    public Predicate visit(CallExpression call) {
        return visit(call, false);
    }

    private Predicate visit(CallExpression call, boolean negated) {
        FunctionDefinition func = call.getFunctionDefinition();
        List<Expression> children = call.getChildren();

        if (func == BuiltInFunctionDefinitions.AND) {
            requireAtLeastArity(children, 2);
            List<Predicate> predicates = flattenAndConvert(children, func, negated);
            return negated ? PredicateBuilder.or(predicates) : PredicateBuilder.and(predicates);
        } else if (func == BuiltInFunctionDefinitions.OR) {
            requireAtLeastArity(children, 2);
            List<Predicate> predicates = flattenAndConvert(children, func, negated);
            return negated ? PredicateBuilder.and(predicates) : PredicateBuilder.or(predicates);
        } else if (func == BuiltInFunctionDefinitions.NOT) {
            requireArity(children, 1);
            return visit(children.get(0), !negated);
        } else if (func == BuiltInFunctionDefinitions.EQUALS) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::notEqual, builder::notEqual),
                    op(builder::notEqual, builder::notEqual),
                    op(builder::equal, builder::equal),
                    op(builder::equal, builder::equal));
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::equal, builder::equal),
                    op(builder::equal, builder::equal),
                    op(builder::notEqual, builder::notEqual),
                    op(builder::notEqual, builder::notEqual));
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::lessOrEqual, builder::lessOrEqual),
                    op(builder::greaterOrEqual, builder::greaterOrEqual),
                    op(builder::greaterThan, builder::greaterThan),
                    op(builder::lessThan, builder::lessThan));
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::lessThan, builder::lessThan),
                    op(builder::greaterThan, builder::greaterThan),
                    op(builder::greaterOrEqual, builder::greaterOrEqual),
                    op(builder::lessOrEqual, builder::lessOrEqual));
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::greaterOrEqual, builder::greaterOrEqual),
                    op(builder::lessOrEqual, builder::lessOrEqual),
                    op(builder::lessThan, builder::lessThan),
                    op(builder::greaterThan, builder::greaterThan));
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return visitComparison(
                    children,
                    negated,
                    op(builder::greaterThan, builder::greaterThan),
                    op(builder::lessThan, builder::lessThan),
                    op(builder::lessOrEqual, builder::lessOrEqual),
                    op(builder::greaterOrEqual, builder::greaterOrEqual));
        } else if (func == BuiltInFunctionDefinitions.IN) {
            requireAtLeastArity(children, 2);
            ResolvedField field = resolveField(children.get(0));
            List<Object> literals = new ArrayList<>();
            for (int i = 1; i < children.size(); i++) {
                literals.add(extractLiteral(field.type(), children.get(i)));
            }
            if (negated) {
                // SQL WHERE: v NOT IN (..., NULL, ...) is never true regardless of
                // type, so this runs before the float residual. Passing NULL to
                // notIn is also unsafe for BSI/range-bitmap file-index evaluation.
                if (literals.contains(null)) {
                    return PredicateBuilder.alwaysFalse();
                }
                rejectNegatedFloatingPoint(field);
                return build(field, builder::notIn, builder::notIn, literals);
            }
            return build(field, builder::in, builder::in, literals);
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            requireArity(children, 1);
            ResolvedField field = resolveField(children.get(0));
            return negated ? isNotNull(field) : isNull(field);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            requireArity(children, 1);
            ResolvedField field = resolveField(children.get(0));
            return negated ? isNull(field) : isNotNull(field);
        } else if (func == BuiltInFunctionDefinitions.BETWEEN) {
            requireArity(children, 3);
            ResolvedField field = resolveField(children.get(0));
            DataType fieldType = field.type();
            Object lower = extractLiteral(fieldType, children.get(1));
            Object upper = extractLiteral(fieldType, children.get(2));
            Predicate between =
                    field.isNested()
                            ? builder.between(field.transform, lower, upper)
                            : builder.between(field.index, lower, upper);
            if (negated) {
                rejectNegatedFloatingPoint(field);
                // LeafTernaryFunction.test returns false if any literal is null, but
                // 12 NOT BETWEEN 15 AND NULL is TRUE (TRUE OR UNKNOWN). Keep residual
                // so Flink can evaluate the three-valued cases.
                if (lower == null || upper == null) {
                    throw new UnsupportedExpression();
                }
                return negate(between);
            }
            return between;
        } else if (func == BuiltInFunctionDefinitions.LIKE) {
            if (children.size() != 2 && children.size() != 3) {
                throw new UnsupportedExpression();
            }
            ResolvedField field = resolveField(children.get(0));
            if (field.type()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                String sqlPattern = extractNonNullLiteral(field.type(), children.get(1)).toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : extractNonNullLiteral(field.type(), children.get(2)).toString();
                String escapedSqlPattern = sqlPattern;
                boolean allowQuick = false;
                if (escape == null && !sqlPattern.contains("_")) {
                    allowQuick = true;
                } else if (escape != null) {
                    if (escape.length() != 1) {
                        throw new UnsupportedExpression();
                    }
                    char escapeChar = escape.charAt(0);
                    boolean matched = true;
                    int i = 0;
                    StringBuilder sb = new StringBuilder();
                    while (i < sqlPattern.length() && matched) {
                        char c = sqlPattern.charAt(i);
                        if (c == escapeChar) {
                            if (i == (sqlPattern.length() - 1)) {
                                throw new UnsupportedExpression();
                            }
                            char nextChar = sqlPattern.charAt(i + 1);
                            if (nextChar == '%') {
                                matched = false;
                            } else if ((nextChar == '_') || (nextChar == escapeChar)) {
                                sb.append(nextChar);
                                i += 1;
                            } else {
                                throw new UnsupportedExpression();
                            }
                        } else if (c == '_') {
                            matched = false;
                        } else {
                            sb.append(c);
                        }
                        i = i + 1;
                    }
                    if (matched) {
                        allowQuick = true;
                        escapedSqlPattern = sb.toString();
                    }
                }
                if (allowQuick) {
                    Matcher beginMatcher = BEGIN_PATTERN.matcher(escapedSqlPattern);
                    if (beginMatcher.matches()) {
                        if (negated) {
                            // StartsWith has no negated predicate, so NOT LIKE must remain a
                            // residual filter evaluated by Flink.
                            throw new UnsupportedExpression();
                        }
                        return build(
                                field,
                                builder::startsWith,
                                builder::startsWith,
                                BinaryString.fromString(beginMatcher.group(1)));
                    }
                }
            }
        } else if (func == BuiltInFunctionDefinitions.IS_TRUE) {
            requireArity(children, 1);
            return booleanTest(resolveField(children.get(0)), true, negated);
        } else if (func == BuiltInFunctionDefinitions.IS_FALSE) {
            requireArity(children, 1);
            return booleanTest(resolveField(children.get(0)), false, negated);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_TRUE) {
            requireArity(children, 1);
            return booleanTest(resolveField(children.get(0)), true, !negated);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_FALSE) {
            requireArity(children, 1);
            return booleanTest(resolveField(children.get(0)), false, !negated);
        }

        throw new UnsupportedExpression();
    }

    private Predicate visit(Expression expression, boolean negated) {
        if (expression instanceof CallExpression) {
            return visit((CallExpression) expression, negated);
        }
        throw new UnsupportedExpression();
    }

    private Predicate booleanTest(ResolvedField field, boolean expected, boolean complement) {
        if (field.type().getLogicalType().getTypeRoot() != LogicalTypeRoot.BOOLEAN) {
            throw new UnsupportedExpression();
        }
        Predicate equals = build(field, builder::equal, builder::equal, expected);
        if (!complement) {
            return equals;
        }
        return PredicateBuilder.or(
                isNull(field), build(field, builder::notEqual, builder::notEqual, expected));
    }

    private Predicate isNull(ResolvedField field) {
        return field.isNested() ? builder.isNull(field.transform) : builder.isNull(field.index);
    }

    private Predicate isNotNull(ResolvedField field) {
        return field.isNested()
                ? builder.isNotNull(field.transform)
                : builder.isNotNull(field.index);
    }

    private Predicate negate(Predicate predicate) {
        return predicate.negate().orElseThrow(UnsupportedExpression::new);
    }

    /**
     * Iteratively flattens a nested AND/OR expression tree into a flat list of child predicates,
     * avoiding stack overflow caused by recursive {@code accept} calls on deeply nested trees (e.g.
     * when Flink expands a large IN clause into nested OR expressions).
     *
     * @param children the children of the top-level AND/OR {@link CallExpression}
     * @param targetFunc the function definition to flatten ({@code AND} or {@code OR})
     * @param negated whether to negate every flattened child and combine them using De Morgan's law
     * @return a flat list of converted child predicates in original order
     */
    private List<Predicate> flattenAndConvert(
            List<Expression> children, FunctionDefinition targetFunc, boolean negated) {
        List<Predicate> result = new ArrayList<>();
        Deque<Expression> stack = new ArrayDeque<>();
        for (int i = children.size() - 1; i >= 0; i--) {
            stack.push(children.get(i));
        }
        while (!stack.isEmpty()) {
            Expression expr = stack.pop();
            if (expr instanceof CallExpression) {
                CallExpression ce = (CallExpression) expr;
                if (ce.getFunctionDefinition() == targetFunc) {
                    List<Expression> ceChildren = ce.getChildren();
                    requireAtLeastArity(ceChildren, 2);
                    for (int i = ceChildren.size() - 1; i >= 0; i--) {
                        stack.push(ceChildren.get(i));
                    }
                } else {
                    result.add(visit(ce, negated));
                }
            } else {
                result.add(visit(expr, negated));
            }
        }
        return result;
    }

    private Predicate visitComparison(
            List<Expression> children,
            boolean negated,
            LeafFunction negatedVisit1,
            LeafFunction negatedVisit2,
            LeafFunction visit1,
            LeafFunction visit2) {
        // Flink FLOAT/DOUBLE comparisons use Java operators; Paimon uses
        // Float/Double.compareTo. Negated equality, inequality, IN and BETWEEN
        // are therefore not equivalent (NaN identity and signed zeros). Simple
        // SQL such as NOT (d > 1.0) is often simplified to d <= 1.0 before
        // pushdown; keep this guard for unsimplified NOT that still reaches
        // applyFilters (for example De Morgan over AND/OR).
        if (negated && isFloatingPointComparison(children)) {
            throw new UnsupportedExpression();
        }
        return negated
                ? visitBiFunction(children, negatedVisit1, negatedVisit2)
                : visitBiFunction(children, visit1, visit2);
    }

    private void rejectNegatedFloatingPoint(ResolvedField field) {
        if (isFloatingPointType(field.type())) {
            throw new UnsupportedExpression();
        }
    }

    private boolean isFloatingPointComparison(List<Expression> children) {
        for (Expression child : children) {
            Optional<FieldReferenceExpression> field = extractFieldReference(child);
            if (field.isPresent() && isFloatingPointType(field.get().getOutputDataType())) {
                return true;
            }
            if (NestedFieldReferences.isNestedFieldReference(child)
                    && isFloatingPointType(NestedFieldReferences.outputDataType(child))) {
                return true;
            }
        }
        return false;
    }

    private boolean isFloatingPointType(DataType type) {
        LogicalTypeRoot root = type.getLogicalType().getTypeRoot();
        return root == LogicalTypeRoot.FLOAT || root == LogicalTypeRoot.DOUBLE;
    }

    private Predicate visitBiFunction(
            List<Expression> children, LeafFunction visit1, LeafFunction visit2) {
        requireArity(children, 2);
        if (isFieldReference(children.get(0))) {
            ResolvedField field = resolveField(children.get(0));
            return visit1.apply(field, extractLiteral(field.type(), children.get(1)));
        }
        if (isFieldReference(children.get(1))) {
            ResolvedField field = resolveField(children.get(1));
            return visit2.apply(field, extractLiteral(field.type(), children.get(0)));
        }

        throw new UnsupportedExpression();
    }

    private boolean isFieldReference(Expression expression) {
        return expression instanceof FieldReferenceExpression
                || NestedFieldReferences.isNestedFieldReference(expression);
    }

    /** A {@link PredicateBuilder} method that builds a leaf predicate over a field. */
    @FunctionalInterface
    private interface LeafFunction {
        Predicate apply(ResolvedField field, Object literal);
    }

    /**
     * Pairs the two {@link PredicateBuilder} overloads of one operation, so that a field can be
     * addressed either by index or, when it is nested inside a row, by transform.
     */
    private static LeafFunction op(
            BiFunction<Integer, Object, Predicate> byIndex,
            BiFunction<Transform, Object, Predicate> byTransform) {
        return (field, literal) -> build(field, byIndex, byTransform, literal);
    }

    private ResolvedField resolveField(Expression expression) {
        if (NestedFieldReferences.isNestedFieldReference(expression)) {
            return resolveNestedField(expression);
        }
        FieldReferenceExpression field =
                extractFieldReference(expression).orElseThrow(UnsupportedExpression::new);
        return ResolvedField.topLevel(field, resolveFieldIndex(field));
    }

    /**
     * Resolves a field nested inside a row. Flink hands the path down as the names of the fields
     * walked through, starting at the top-level one, which is what {@link NestedFieldTransform}
     * addresses the field by as well.
     */
    private ResolvedField resolveNestedField(Expression expression) {
        String[] fieldNames = NestedFieldReferences.fieldNames(expression);
        if (fieldNames.length < 2) {
            throw new UnsupportedExpression();
        }

        int rootIndex = builder.indexOf(fieldNames[0]);
        if (rootIndex < 0) {
            throw new UnsupportedExpression();
        }
        FieldRef rootRef =
                new FieldRef(rootIndex, fieldNames[0], builder.rowType().getTypeAt(rootIndex));
        List<String> path = Arrays.asList(fieldNames).subList(1, fieldNames.length);
        try {
            return ResolvedField.nested(
                    NestedFieldReferences.outputDataType(expression),
                    new NestedFieldTransform(rootRef, path));
        } catch (IllegalArgumentException e) {
            // The path does not address a field of this table: the root is not a row, or a field
            // along the way was renamed or dropped. Leave the filter for Flink to evaluate.
            throw new UnsupportedExpression();
        }
    }

    /**
     * Binds a {@link PredicateBuilder} method to a field, choosing the overload that addresses it:
     * by index for a top-level field, by transform for one nested inside a row.
     */
    private static <T> Predicate build(
            ResolvedField field,
            BiFunction<Integer, T, Predicate> byIndex,
            BiFunction<Transform, T, Predicate> byTransform,
            T argument) {
        return field.isNested()
                ? byTransform.apply(field.transform, argument)
                : byIndex.apply(field.index, argument);
    }

    private int resolveFieldIndex(FieldReferenceExpression field) {
        int index = builder.indexOf(field.getName());
        if (index < 0) {
            throw new UnsupportedExpression();
        }
        return index;
    }

    private void requireArity(List<Expression> children, int expected) {
        if (children.size() != expected) {
            throw new UnsupportedExpression();
        }
    }

    private void requireAtLeastArity(List<Expression> children, int minimum) {
        if (children.size() < minimum) {
            throw new UnsupportedExpression();
        }
    }

    private Optional<FieldReferenceExpression> extractFieldReference(Expression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return Optional.of((FieldReferenceExpression) expression);
        }
        return Optional.empty();
    }

    private Object extractLiteral(DataType expectedType, Expression expression) {
        LogicalType expectedLogicalType = expectedType.getLogicalType();
        if (!supportsPredicate(expectedLogicalType)) {
            throw new UnsupportedExpression();
        }

        if (expression instanceof ValueLiteralExpression) {
            ValueLiteralExpression valueExpression = (ValueLiteralExpression) expression;
            if (valueExpression.isNull()) {
                return null;
            }

            DataType actualType = valueExpression.getOutputDataType();
            LogicalType actualLogicalType = actualType.getLogicalType();
            Optional<?> valueOpt = valueExpression.getValueAs(actualType.getConversionClass());
            if (valueOpt.isPresent()) {
                Object value = valueOpt.get();
                if (actualLogicalType.getTypeRoot().equals(expectedLogicalType.getTypeRoot())) {
                    return FlinkRowWrapper.fromFlinkObject(
                            DataStructureConverters.getConverter(expectedType)
                                    .toInternalOrNull(value),
                            expectedLogicalType);
                } else if (supportsImplicitCast(actualLogicalType, expectedLogicalType)) {
                    try {
                        return TypeUtils.castFromString(
                                value.toString(), toDataType(expectedLogicalType));
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        throw new UnsupportedExpression();
    }

    private Object extractNonNullLiteral(DataType expectedType, Expression expression) {
        Object literal = extractLiteral(expectedType, expression);
        if (literal == null) {
            throw new UnsupportedExpression();
        }
        return literal;
    }

    private boolean supportsPredicate(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    /**
     * A field a predicate can be built on: either a top-level field, addressed by its index, or a
     * field nested inside a row, addressed by a {@link NestedFieldTransform}.
     */
    private static class ResolvedField {

        private final DataType type;
        private final int index;
        @Nullable private final Transform transform;

        private ResolvedField(DataType type, int index, @Nullable Transform transform) {
            this.type = type;
            this.index = index;
            this.transform = transform;
        }

        static ResolvedField topLevel(FieldReferenceExpression expression, int index) {
            return new ResolvedField(expression.getOutputDataType(), index, null);
        }

        static ResolvedField nested(DataType type, Transform transform) {
            return new ResolvedField(type, -1, transform);
        }

        boolean isNested() {
            return transform != null;
        }

        DataType type() {
            return type;
        }
    }

    @Override
    public Predicate visit(ValueLiteralExpression valueLiteralExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(FieldReferenceExpression fieldReferenceExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(TypeLiteralExpression typeLiteralExpression) {
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(Expression expression) {
        throw new UnsupportedExpression();
    }

    /**
     * Try best to convert a {@link ResolvedExpression} to {@link Predicate}.
     *
     * @param filter a resolved expression
     * @return {@link Predicate} if no {@link UnsupportedExpression} thrown.
     */
    public static Optional<Predicate> convert(RowType rowType, ResolvedExpression filter) {
        try {
            return Optional.ofNullable(filter.accept(new PredicateConverter(rowType)));
        } catch (UnsupportedExpression e) {
            return Optional.empty();
        }
    }

    /** Encounter an unsupported expression, the caller can choose to ignore this filter branch. */
    public static class UnsupportedExpression extends RuntimeException {}
}
