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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.And;
import org.apache.paimon.predicate.Between;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotContains;
import org.apache.paimon.predicate.NotEndsWith;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.NotIn;
import org.apache.paimon.predicate.NotStartsWith;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.StartsWith;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import dev.vortex.api.Expression;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Converts Paimon {@link Predicate} to Vortex {@link Expression}. */
public class VortexPredicateConverter implements PredicateVisitor<Expression> {

    public static final VortexPredicateConverter INSTANCE = new VortexPredicateConverter();

    @Nullable
    public static Expression toVortexExpression(@Nullable List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return null;
        }
        return PredicateBuilder.and(predicates).visit(INSTANCE);
    }

    @Override
    public Expression visit(LeafPredicate predicate) {
        Optional<FieldRef> fieldRefOpt = predicate.fieldRefOptional();
        if (!fieldRefOpt.isPresent()) {
            return null;
        }
        FieldRef fieldRef = fieldRefOpt.get();
        LeafFunction function = predicate.function();
        List<Object> literals = predicate.literals();

        // Builders clone their inputs and leave freeing to the caller (see the vortex-jni
        // expression.rs module doc), so the column and the literals are ours to release once
        // the call consuming them has returned.
        try (Expression field = Expression.column(fieldRef.name())) {
            if (function instanceof IsNull) {
                return Expression.isNull(field);
            }
            if (function instanceof IsNotNull) {
                return Expression.isNotNull(field);
            }

            if (literals == null || literals.isEmpty()) {
                return null;
            }

            if (function instanceof In || function instanceof NotIn) {
                return in(field, fieldRef.type(), literals, function instanceof NotIn);
            }
            if (function instanceof Between) {
                return between(field, fieldRef.type(), literals);
            }
            if (isStringMatch(function)) {
                return stringMatch(field, fieldRef.type(), function, literals.get(0));
            }

            Expression.BinaryOp op = binaryOp(function);
            if (op == null || literals.get(0) == null) {
                return null;
            }

            try (Expression vortexLiteral = toLiteral(fieldRef.type(), literals.get(0))) {
                if (vortexLiteral == null) {
                    return null;
                }
                return Expression.binary(op, field, vortexLiteral);
            }
        }
    }

    @Override
    public Expression visit(CompoundPredicate predicate) {
        boolean isAnd = predicate.function() instanceof And;
        if (!isAnd && !(predicate.function() instanceof Or)) {
            return null;
        }

        List<Expression> children = new ArrayList<>();
        try {
            for (Predicate child : predicate.children()) {
                Expression expr = child.visit(this);
                if (expr == null) {
                    // An Or must push all children or none: dropping a disjunct narrows the
                    // filter and would drop rows the predicate matches. Dropping a conjunct
                    // from an And only widens it, which the best-effort contract allows.
                    if (!isAnd) {
                        return null;
                    }
                    continue;
                }
                children.add(expr);
            }
            if (children.isEmpty()) {
                return null;
            }
            Expression[] operands = children.toArray(new Expression[0]);
            return isAnd ? Expression.and(operands) : Expression.or(operands);
        } finally {
            // and/or clone their operands, so the children are ours either way -- and on the Or
            // give-up path above, the ones already collected would otherwise be unreachable.
            children.forEach(Expression::close);
        }
    }

    @Nullable
    private static Expression.BinaryOp binaryOp(LeafFunction function) {
        if (function instanceof Equal) {
            return Expression.BinaryOp.EQ;
        } else if (function instanceof NotEqual) {
            return Expression.BinaryOp.NOT_EQ;
        } else if (function instanceof GreaterThan) {
            return Expression.BinaryOp.GT;
        } else if (function instanceof GreaterOrEqual) {
            return Expression.BinaryOp.GTE;
        } else if (function instanceof LessThan) {
            return Expression.BinaryOp.LT;
        } else if (function instanceof LessOrEqual) {
            return Expression.BinaryOp.LTE;
        }
        return null;
    }

    /**
     * {@code IN} and {@code NOT IN} with SQL null rules, which Paimon follows too: a null field
     * never matches, and a null in the list makes {@code NOT IN} false for every row.
     */
    @Nullable
    private static Expression in(
            Expression field, DataType type, List<Object> literals, boolean negated) {
        List<Expression> elements = new ArrayList<>();
        Expression elementType = null;
        Expression list = null;
        try {
            elementType = nullLiteral(type);
            if (elementType == null) {
                return null;
            }

            for (Object literal : literals) {
                Expression element = literal == null ? nullLiteral(type) : toLiteral(type, literal);
                if (element == null) {
                    return null;
                }
                elements.add(element);
            }

            list = Expression.literalList(elementType, true, elements.toArray(new Expression[0]));
            return negated ? Expression.notIn(field, list) : Expression.in(field, list);
        } finally {
            elements.forEach(Expression::close);
            if (elementType != null) {
                elementType.close();
            }
            if (list != null) {
                list.close();
            }
        }
    }

    @Nullable
    private static Expression between(Expression field, DataType type, List<Object> literals) {
        if (literals.size() != 2 || literals.get(0) == null || literals.get(1) == null) {
            return null;
        }

        try (Expression lower = toLiteral(type, literals.get(0));
                Expression upper = toLiteral(type, literals.get(1))) {
            if (lower == null || upper == null) {
                return null;
            }
            return Expression.between(field, lower, upper, false, false);
        }
    }

    private static boolean isStringMatch(LeafFunction function) {
        return function instanceof StartsWith
                || function instanceof NotStartsWith
                || function instanceof EndsWith
                || function instanceof NotEndsWith
                || function instanceof Contains
                || function instanceof NotContains;
    }

    /**
     * Prefix, suffix and substring matches as {@code LIKE} patterns. Paimon tests them on non-null
     * strings only, as {@code LIKE} and {@code NOT LIKE} do.
     */
    @Nullable
    private static Expression stringMatch(
            Expression field, DataType type, LeafFunction function, @Nullable Object literal) {
        DataTypeRoot root = type.getTypeRoot();
        if (literal == null || (root != DataTypeRoot.CHAR && root != DataTypeRoot.VARCHAR)) {
            return null;
        }

        String escaped = escapeLike(literal.toString());
        String pattern;
        if (function instanceof StartsWith || function instanceof NotStartsWith) {
            pattern = escaped + "%";
        } else if (function instanceof EndsWith || function instanceof NotEndsWith) {
            pattern = "%" + escaped;
        } else {
            pattern = "%" + escaped + "%";
        }

        boolean negated =
                function instanceof NotStartsWith
                        || function instanceof NotEndsWith
                        || function instanceof NotContains;
        try (Expression patternLiteral = Expression.literal(pattern)) {
            return Expression.like(field, patternLiteral, negated, false);
        }
    }

    private static String escapeLike(String value) {
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' || c == '%' || c == '_') {
                escaped.append('\\');
            }
            escaped.append(c);
        }
        return escaped.toString();
    }

    /** A typed null literal, which also gives the element type of a list literal. */
    @Nullable
    private static Expression nullLiteral(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Expression.nullLiteral(Expression.DType.BOOL);
            case TINYINT:
                return Expression.nullLiteral(Expression.DType.I8);
            case SMALLINT:
                return Expression.nullLiteral(Expression.DType.I16);
            case INTEGER:
                return Expression.nullLiteral(Expression.DType.I32);
            case BIGINT:
                return Expression.nullLiteral(Expression.DType.I64);
            case FLOAT:
                return Expression.nullLiteral(Expression.DType.F32);
            case DOUBLE:
                return Expression.nullLiteral(Expression.DType.F64);
            case CHAR:
            case VARCHAR:
                return Expression.nullLiteral(Expression.DType.UTF8);
            case DATE:
                return Expression.nullLiteralDate(Expression.TimeUnit.DAYS);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Expression.nullLiteralDecimal(
                        decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return Expression.nullLiteralTimestamp(
                        timestampUnit(((TimestampType) type).getPrecision()), null);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Expression.nullLiteralTimestamp(
                        timestampUnit(((LocalZonedTimestampType) type).getPrecision()), "UTC");
            default:
                return null;
        }
    }

    private static Expression.TimeUnit timestampUnit(int precision) {
        if (precision == 0) {
            return Expression.TimeUnit.SECONDS;
        } else if (precision <= 3) {
            return Expression.TimeUnit.MILLISECONDS;
        } else if (precision <= 6) {
            return Expression.TimeUnit.MICROSECONDS;
        }
        return Expression.TimeUnit.NANOSECONDS;
    }

    @Nullable
    private static Expression toLiteral(DataType type, Object value) {
        if (value == null) {
            return nullLiteral(type);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Expression.literal((Boolean) value);
            case TINYINT:
                return Expression.literal((Byte) value);
            case SMALLINT:
                return Expression.literal((Short) value);
            case INTEGER:
                return Expression.literal((Integer) value);
            case DATE:
                return Expression.literalDate((Integer) value, Expression.TimeUnit.DAYS);
            case BIGINT:
                return Expression.literal((Long) value);
            case FLOAT:
                return Expression.literal((Float) value);
            case DOUBLE:
                return Expression.literal((Double) value);
            case CHAR:
            case VARCHAR:
                return Expression.literal(
                        value instanceof BinaryString ? value.toString() : (String) value);
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return Expression.literalDecimal(
                        decimal.toBigDecimal().unscaledValue(),
                        decimal.precision(),
                        decimal.scale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Timestamp ts = (Timestamp) value;
                return toTimestampLiteral(ts, ((TimestampType) type).getPrecision(), null);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp lzTs = (Timestamp) value;
                return toTimestampLiteral(
                        lzTs, ((LocalZonedTimestampType) type).getPrecision(), "UTC");
            default:
                return null;
        }
    }

    private static Expression toTimestampLiteral(
            Timestamp ts, int precision, @Nullable String timeZone) {
        // The literal carries the precision of the engine that produced it, the file carries the
        // precision of the column. When the literal does not land exactly on the column's grain,
        // no single rounding direction is right for every operator, so refuse to push it down and
        // let the caller drop the leaf instead.
        if (precision == 0) {
            if (ts.getNanoOfMillisecond() != 0 || ts.getMillisecond() % 1000 != 0) {
                return null;
            }
            return Expression.literalTimestamp(
                    ts.getMillisecond() / 1000, Expression.TimeUnit.SECONDS, timeZone);
        } else if (precision <= 3) {
            if (ts.getNanoOfMillisecond() != 0) {
                return null;
            }
            return Expression.literalTimestamp(
                    ts.getMillisecond(), Expression.TimeUnit.MILLISECONDS, timeZone);
        } else if (precision <= 6) {
            if (ts.getNanoOfMillisecond() % 1000 != 0) {
                return null;
            }
            return Expression.literalTimestamp(
                    ts.getMillisecond() * 1000 + ts.getNanoOfMillisecond() / 1000,
                    Expression.TimeUnit.MICROSECONDS,
                    timeZone);
        } else {
            // A TIMESTAMP value reaches year 9999, so nanoseconds since the epoch are not
            // representable as an int64 outside roughly [1677-09-21, 2262-04-11]. A wrapped bound
            // silently excludes matching rows, so refuse to push the literal down for the same
            // reason as the grain checks above.
            long nanos;
            try {
                nanos =
                        Math.addExact(
                                Math.multiplyExact(ts.getMillisecond(), 1_000_000L),
                                ts.getNanoOfMillisecond());
            } catch (ArithmeticException e) {
                return null;
            }
            return Expression.literalTimestamp(nanos, Expression.TimeUnit.NANOSECONDS, timeZone);
        }
    }
}
