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
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.IsNotNull;
import org.apache.paimon.predicate.IsNull;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.LessOrEqual;
import org.apache.paimon.predicate.LessThan;
import org.apache.paimon.predicate.NotEqual;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.types.DataType;
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
        Expression field = Expression.column(fieldRef.name());

        if (predicate.function() instanceof IsNull) {
            return Expression.isNull(field);
        }
        if (predicate.function() instanceof IsNotNull) {
            return Expression.isNotNull(field);
        }

        List<Object> literals = predicate.literals();
        if (literals == null || literals.isEmpty()) {
            return null;
        }

        Expression vortexLiteral = toLiteral(fieldRef.type(), literals.get(0));
        if (vortexLiteral == null) {
            return null;
        }

        if (predicate.function() instanceof Equal) {
            return Expression.binary(Expression.BinaryOp.EQ, field, vortexLiteral);
        } else if (predicate.function() instanceof NotEqual) {
            return Expression.binary(Expression.BinaryOp.NOT_EQ, field, vortexLiteral);
        } else if (predicate.function() instanceof GreaterThan) {
            return Expression.binary(Expression.BinaryOp.GT, field, vortexLiteral);
        } else if (predicate.function() instanceof GreaterOrEqual) {
            return Expression.binary(Expression.BinaryOp.GTE, field, vortexLiteral);
        } else if (predicate.function() instanceof LessThan) {
            return Expression.binary(Expression.BinaryOp.LT, field, vortexLiteral);
        } else if (predicate.function() instanceof LessOrEqual) {
            return Expression.binary(Expression.BinaryOp.LTE, field, vortexLiteral);
        }

        return null;
    }

    @Override
    public Expression visit(CompoundPredicate predicate) {
        if (predicate.function() instanceof And) {
            List<Expression> children = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Expression expr = child.visit(this);
                if (expr != null) {
                    children.add(expr);
                }
            }
            if (children.isEmpty()) {
                return null;
            }
            return Expression.and(children.toArray(new Expression[0]));
        } else if (predicate.function() instanceof Or) {
            List<Expression> children = new ArrayList<>();
            for (Predicate child : predicate.children()) {
                Expression expr = child.visit(this);
                if (expr == null) {
                    return null;
                }
                children.add(expr);
            }
            return Expression.or(children.toArray(new Expression[0]));
        }

        return null;
    }

    @Nullable
    private static Expression toLiteral(DataType type, Object value) {
        if (value == null) {
            return Expression.nullLiteral(Expression.DType.I32);
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return Expression.literal((Boolean) value);
            case TINYINT:
                return Expression.literal((Byte) value);
            case SMALLINT:
                return Expression.literal((Short) value);
            case INTEGER:
            case DATE:
                return Expression.literal((Integer) value);
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
        if (precision <= 3) {
            return Expression.literalTimestamp(
                    ts.getMillisecond(), Expression.TimeUnit.MILLISECONDS, timeZone);
        } else if (precision <= 6) {
            return Expression.literalTimestamp(
                    ts.getMillisecond() * 1000 + ts.getNanoOfMillisecond() / 1000,
                    Expression.TimeUnit.MICROSECONDS,
                    timeZone);
        } else {
            return Expression.literalTimestamp(
                    ts.getMillisecond() * 1_000_000 + ts.getNanoOfMillisecond(),
                    Expression.TimeUnit.NANOSECONDS,
                    timeZone);
        }
    }
}
