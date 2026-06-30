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
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        FunctionDefinition func = call.getFunctionDefinition();
        List<Expression> children = call.getChildren();

        if (func == BuiltInFunctionDefinitions.AND) {
            return PredicateBuilder.and(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.OR) {
            return PredicateBuilder.or(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.EQUALS) {
            return visitBiFunction(children, builder::equal, builder::equal);
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return visitBiFunction(children, builder::notEqual, builder::notEqual);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            return visitBiFunction(children, builder::greaterThan, builder::lessThan);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::greaterOrEqual, builder::lessOrEqual);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            return visitBiFunction(children, builder::lessThan, builder::greaterThan);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::lessOrEqual, builder::greaterOrEqual);
        } else if (func == BuiltInFunctionDefinitions.IN) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            List<Object> literals = new ArrayList<>();
            for (int i = 1; i < children.size(); i++) {
                literals.add(extractLiteral(fieldRefExpr.getOutputDataType(), children.get(i)));
            }
            return builder.in(builder.indexOf(fieldRefExpr.getName()), literals);
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNull)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNotNull)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.BETWEEN) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.between(
                    builder.indexOf(fieldRefExpr.getName()), children.get(1), children.get(2));
        } else if (func == BuiltInFunctionDefinitions.LIKE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            if (fieldRefExpr
                    .getOutputDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                String sqlPattern =
                        Objects.requireNonNull(
                                        extractLiteral(
                                                fieldRefExpr.getOutputDataType(), children.get(1)))
                                .toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : Objects.requireNonNull(
                                                extractLiteral(
                                                        fieldRefExpr.getOutputDataType(),
                                                        children.get(2)))
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
                        return builder.startsWith(
                                builder.indexOf(fieldRefExpr.getName()),
                                BinaryString.fromString(beginMatcher.group(1)));
                    }
                }
            }
        } else if (func == BuiltInFunctionDefinitions.IS_TRUE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.equal(builder.indexOf(fieldRefExpr.getName()), Boolean.TRUE);
        } else if (func == BuiltInFunctionDefinitions.IS_FALSE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.equal(builder.indexOf(fieldRefExpr.getName()), Boolean.FALSE);
        } else if (func == BuiltInFunctionDefinitions.NOT) {
            // NOT predicate - negate the inner predicate
            Predicate innerPredicate = children.get(0).accept(this);
            return innerPredicate.negate().orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_TRUE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.notEqual(builder.indexOf(fieldRefExpr.getName()), Boolean.TRUE);
        } else if (func == BuiltInFunctionDefinitions.NOT_BETWEEN) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.between(builder.indexOf(fieldRefExpr.getName()), children.get(1), children.get(2))
                    .negate()
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.SIMILAR) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            if (fieldRefExpr
                    .getOutputDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                String sqlPattern = 
                        Objects.requireNonNull(
                                        extractLiteral(
                                                fieldRefExpr.getOutputDataType(), children.get(1)))
                                .toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : Objects.requireNonNull(
                                                extractLiteral(
                                                        fieldRefExpr.getOutputDataType(),
                                                        children.get(2)))
                                        .toString();
                String likePattern = convertSimilarToLike(sqlPattern, escape);
                return builder.like(
                        builder.indexOf(fieldRefExpr.getName()),
                        BinaryString.fromString(likePattern));
            }
        }
        
        throw new UnsupportedExpression();
    }

    private Predicate visitBiFunction(
            List<Expression> children,
            BiFunction<Integer, Object, Predicate> visit1,
            BiFunction<Integer, Object, Predicate> visit2) {
        Optional<FieldReferenceExpression> fieldRefExpr = extractFieldReference(children.get(0));
        if (fieldRefExpr.isPresent()) {
            Object literal =
                    extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(1));
            return visit1.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
        } else {
            fieldRefExpr = extractFieldReference(children.get(1));
            if (fieldRefExpr.isPresent()) {
                Object literal =
                        extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(0));
                return visit2.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
            }
        }

        throw new UnsupportedExpression();
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
     * Converts a SQL SIMILAR TO pattern to an equivalent SQL LIKE pattern so that it can be
     * evaluated by {@link PredicateBuilder#like}.
     *
     * <p>The conversion handles only the subset of SIMILAR TO syntax that maps directly to SQL
     * LIKE:
     * <ul>
     *   <li>{@code %} (any-string wildcard) is preserved as-is.</li>
     *   <li>{@code _} (single-character wildcard) is preserved as-is.</li>
     *   <li>Escape sequences: {@code escape + '_'} and {@code escape + '%'} become their literal
     *       equivalents, emitted as {@code \ + char} so that the downstream
     *       {@link Like} function (which uses {@code \} as its default escape) treats them as
     *       literals. {@code escape + escape} becomes a literal escape character.</li>
     * </ul>
     *
     * <p>SIMILAR TO-only features (character classes {@code [...]}, alternation {@code |},
     * quantifiers {@code *}, {@code +}, {@code ?}, and grouping {@code ()}) are not supported and
     * will cause an {@link UnsupportedExpression} to be thrown.
     *
     * @param sqlPattern the SIMILAR TO pattern string
     * @param escape the escape character string (single char), or {@code null} for no escaping
     * @return an equivalent SQL LIKE pattern (using {@code \} as the escape character)
     * @throws UnsupportedExpression if the pattern uses SIMILAR TO-only features
     */
    private String convertSimilarToLike(String sqlPattern, String escape) {
        if (sqlPattern == null || sqlPattern.isEmpty()) {
            return sqlPattern;
        }

        // Sentinel 0 means no escape character is defined.
        char escapeChar = (escape != null && !escape.isEmpty()) ? escape.charAt(0) : 0;
        // The output LIKE pattern will always use '\' as its escape char, because that is what
        // Like.sqlToRegexLike uses by default.
        final char outputEscape = '\\';

        StringBuilder like = new StringBuilder();

        for (int i = 0; i < sqlPattern.length(); i++) {
            char c = sqlPattern.charAt(i);

            if (escapeChar != 0 && c == escapeChar) {
                // Escape sequence
                if (i + 1 >= sqlPattern.length()) {
                    throw new UnsupportedExpression();
                }
                char next = sqlPattern.charAt(i + 1);
                if (next == '_' || next == '%') {
                    // Escaped wildcard -> literal in the LIKE output.
                    // Emit as outputEscape + wildcard so Like treats it as a literal.
                    like.append(outputEscape).append(next);
                } else if (next == escapeChar) {
                    // Escaped escape char -> emit as a literal character.
                    // If the escape char itself is special to LIKE (% or _), escape it; otherwise
                    // emit it as-is since Like only special-cases % and _.
                    like.append(escapeChar);
                } else {
                    // Unknown escape sequence - not supported
                    throw new UnsupportedExpression();
                }
                i++;
            } else if (c == '%' || c == '_') {
                // SIMILAR TO wildcards are the same as SQL LIKE wildcards
                like.append(c);
            } else if (c == '[' || c == '|' || c == '(' || c == ')'
                    || c == '*' || c == '+' || c == '?') {
                // SIMILAR TO-only features: not representable in SQL LIKE
                throw new UnsupportedExpression();
            } else if (c == outputEscape) {
                // A literal backslash in the pattern needs to be escaped in the output,
                // since the output escape char is '\'.
                like.append(outputEscape).append(outputEscape);
            } else {
                like.append(c);
            }
        }

        return like.toString();
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
