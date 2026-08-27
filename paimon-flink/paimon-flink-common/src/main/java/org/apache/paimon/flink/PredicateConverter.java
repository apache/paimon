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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
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

import java.util.ArrayDeque;
import java.util.ArrayList;
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
            return negated
                    ? visitBiFunction(children, builder::notEqual, builder::notEqual)
                    : visitBiFunction(children, builder::equal, builder::equal);
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return negated
                    ? visitBiFunction(children, builder::equal, builder::equal)
                    : visitBiFunction(children, builder::notEqual, builder::notEqual);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            return negated
                    ? visitBiFunction(children, builder::lessOrEqual, builder::greaterOrEqual)
                    : visitBiFunction(children, builder::greaterThan, builder::lessThan);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return negated
                    ? visitBiFunction(children, builder::lessThan, builder::greaterThan)
                    : visitBiFunction(children, builder::greaterOrEqual, builder::lessOrEqual);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            return negated
                    ? visitBiFunction(children, builder::greaterOrEqual, builder::lessOrEqual)
                    : visitBiFunction(children, builder::lessThan, builder::greaterThan);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return negated
                    ? visitBiFunction(children, builder::greaterThan, builder::lessThan)
                    : visitBiFunction(children, builder::lessOrEqual, builder::greaterOrEqual);
        } else if (func == BuiltInFunctionDefinitions.IN) {
            requireAtLeastArity(children, 2);
            ResolvedField field = resolveField(children.get(0));
            List<Object> literals = new ArrayList<>();
            for (int i = 1; i < children.size(); i++) {
                literals.add(extractLiteral(field.expression.getOutputDataType(), children.get(i)));
            }
            return negated
                    ? builder.notIn(field.index, literals)
                    : builder.in(field.index, literals);
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            requireArity(children, 1);
            ResolvedField field = resolveField(children.get(0));
            return negated ? builder.isNotNull(field.index) : builder.isNull(field.index);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            requireArity(children, 1);
            ResolvedField field = resolveField(children.get(0));
            return negated ? builder.isNull(field.index) : builder.isNotNull(field.index);
        } else if (func == BuiltInFunctionDefinitions.BETWEEN) {
            requireArity(children, 3);
            ResolvedField field = resolveField(children.get(0));
            Object lower = extractLiteral(field.expression.getOutputDataType(), children.get(1));
            Object upper = extractLiteral(field.expression.getOutputDataType(), children.get(2));
            Predicate between = builder.between(field.index, lower, upper);
            return negated ? negate(between) : between;
        } else if (func == BuiltInFunctionDefinitions.LIKE) {
            if (children.size() != 2 && children.size() != 3) {
                throw new UnsupportedExpression();
            }
            ResolvedField field = resolveField(children.get(0));
            if (field.expression
                    .getOutputDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                String sqlPattern =
                        extractNonNullLiteral(field.expression.getOutputDataType(), children.get(1))
                                .toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : extractNonNullLiteral(
                                                field.expression.getOutputDataType(),
                                                children.get(2))
                                        .toString();
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
                        return builder.startsWith(
                                field.index, BinaryString.fromString(beginMatcher.group(1)));
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
        if (field.expression.getOutputDataType().getLogicalType().getTypeRoot()
                != LogicalTypeRoot.BOOLEAN) {
            throw new UnsupportedExpression();
        }
        Predicate equals = builder.equal(field.index, expected);
        if (!complement) {
            return equals;
        }
        return PredicateBuilder.or(
                builder.isNull(field.index), builder.notEqual(field.index, expected));
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

    private Predicate visitBiFunction(
            List<Expression> children,
            BiFunction<Integer, Object, Predicate> visit1,
            BiFunction<Integer, Object, Predicate> visit2) {
        requireArity(children, 2);
        Optional<FieldReferenceExpression> fieldRefExpr = extractFieldReference(children.get(0));
        if (fieldRefExpr.isPresent()) {
            int fieldIndex = resolveFieldIndex(fieldRefExpr.get());
            Object literal =
                    extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(1));
            return visit1.apply(fieldIndex, literal);
        } else {
            fieldRefExpr = extractFieldReference(children.get(1));
            if (fieldRefExpr.isPresent()) {
                int fieldIndex = resolveFieldIndex(fieldRefExpr.get());
                Object literal =
                        extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(0));
                return visit2.apply(fieldIndex, literal);
            }
        }

        throw new UnsupportedExpression();
    }

    private ResolvedField resolveField(Expression expression) {
        FieldReferenceExpression field =
                extractFieldReference(expression).orElseThrow(UnsupportedExpression::new);
        return new ResolvedField(field, resolveFieldIndex(field));
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

    private static class ResolvedField {

        private final FieldReferenceExpression expression;
        private final int index;

        private ResolvedField(FieldReferenceExpression expression, int index) {
            this.expression = expression;
            this.index = index;
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
