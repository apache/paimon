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

package org.apache.paimon.cli.sql;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts Calcite RexNode filter expressions to Paimon Predicates for split-level pruning. Filters
 * that cannot be converted are silently skipped (Calcite re-applies them row-by-row).
 */
public class RexToPredicate {

    @Nullable
    public static Predicate convert(List<RexNode> filters, RowType rowType) {
        if (filters == null || filters.isEmpty()) {
            return null;
        }
        PredicateBuilder builder = new PredicateBuilder(rowType);
        List<Predicate> predicates = new ArrayList<>();
        for (RexNode filter : filters) {
            Predicate p = convertNode(filter, builder, rowType);
            if (p != null) {
                predicates.add(p);
            }
        }
        if (predicates.isEmpty()) {
            return null;
        }
        return PredicateBuilder.and(predicates);
    }

    @Nullable
    private static Predicate convertNode(RexNode node, PredicateBuilder builder, RowType rowType) {
        if (!(node instanceof RexCall)) {
            return null;
        }
        RexCall call = (RexCall) node;
        SqlKind kind = call.getKind();

        switch (kind) {
            case AND:
                return convertAnd(call, builder, rowType);
            case OR:
                return convertOr(call, builder, rowType);
            case EQUALS:
                return convertComparison(call, builder, rowType, CompOp.EQUAL);
            case NOT_EQUALS:
                return convertComparison(call, builder, rowType, CompOp.NOT_EQUAL);
            case LESS_THAN:
                return convertComparison(call, builder, rowType, CompOp.LESS_THAN);
            case LESS_THAN_OR_EQUAL:
                return convertComparison(call, builder, rowType, CompOp.LESS_OR_EQUAL);
            case GREATER_THAN:
                return convertComparison(call, builder, rowType, CompOp.GREATER_THAN);
            case GREATER_THAN_OR_EQUAL:
                return convertComparison(call, builder, rowType, CompOp.GREATER_OR_EQUAL);
            case IS_NULL:
                return convertIsNull(call, builder, true);
            case IS_NOT_NULL:
                return convertIsNull(call, builder, false);
            default:
                return null;
        }
    }

    @Nullable
    private static Predicate convertAnd(RexCall call, PredicateBuilder builder, RowType rowType) {
        List<Predicate> parts = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            Predicate p = convertNode(operand, builder, rowType);
            if (p != null) {
                parts.add(p);
            }
        }
        if (parts.isEmpty()) {
            return null;
        }
        return PredicateBuilder.and(parts);
    }

    @Nullable
    private static Predicate convertOr(RexCall call, PredicateBuilder builder, RowType rowType) {
        List<Predicate> parts = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            Predicate p = convertNode(operand, builder, rowType);
            if (p == null) {
                return null;
            }
            parts.add(p);
        }
        if (parts.isEmpty()) {
            return null;
        }
        return PredicateBuilder.or(parts);
    }

    @Nullable
    private static Predicate convertComparison(
            RexCall call, PredicateBuilder builder, RowType rowType, CompOp op) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return null;
        }

        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        int colIdx;
        Object literal;

        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            colIdx = ((RexInputRef) left).getIndex();
            literal = extractLiteral((RexLiteral) right, rowType.getFields().get(colIdx));
        } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
            colIdx = ((RexInputRef) right).getIndex();
            literal = extractLiteral((RexLiteral) left, rowType.getFields().get(colIdx));
            op = op.flip();
        } else {
            return null;
        }

        if (literal == null) {
            return null;
        }

        switch (op) {
            case EQUAL:
                return builder.equal(colIdx, literal);
            case NOT_EQUAL:
                return builder.notEqual(colIdx, literal);
            case LESS_THAN:
                return builder.lessThan(colIdx, literal);
            case LESS_OR_EQUAL:
                return builder.lessOrEqual(colIdx, literal);
            case GREATER_THAN:
                return builder.greaterThan(colIdx, literal);
            case GREATER_OR_EQUAL:
                return builder.greaterOrEqual(colIdx, literal);
            default:
                return null;
        }
    }

    @Nullable
    private static Predicate convertIsNull(RexCall call, PredicateBuilder builder, boolean isNull) {
        if (call.getOperands().size() != 1) {
            return null;
        }
        RexNode operand = call.getOperands().get(0);
        if (!(operand instanceof RexInputRef)) {
            return null;
        }
        int idx = ((RexInputRef) operand).getIndex();
        return isNull ? builder.isNull(idx) : builder.isNotNull(idx);
    }

    @Nullable
    private static Object extractLiteral(RexLiteral literal, DataField field) {
        if (RexLiteral.isNullLiteral(literal)) {
            return null;
        }
        DataTypeRoot root = field.type().getTypeRoot();
        switch (root) {
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.intValue() : null;
                }
            case BIGINT:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.longValue() : null;
                }
            case FLOAT:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.floatValue() : null;
                }
            case DOUBLE:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.doubleValue() : null;
                }
            case TINYINT:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.byteValue() : null;
                }
            case SMALLINT:
                {
                    BigDecimal bd = literal.getValueAs(BigDecimal.class);
                    return bd != null ? bd.shortValue() : null;
                }
            case DECIMAL:
                return literal.getValueAs(BigDecimal.class);
            case CHAR:
            case VARCHAR:
                {
                    NlsString nls = literal.getValueAs(NlsString.class);
                    if (nls != null) {
                        return BinaryString.fromString(nls.getValue());
                    }
                    String s = literal.getValueAs(String.class);
                    return s != null ? BinaryString.fromString(s) : null;
                }
            case BOOLEAN:
                return literal.getValueAs(Boolean.class);
            default:
                return null;
        }
    }

    private enum CompOp {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_OR_EQUAL,
        GREATER_THAN,
        GREATER_OR_EQUAL;

        CompOp flip() {
            switch (this) {
                case LESS_THAN:
                    return GREATER_THAN;
                case LESS_OR_EQUAL:
                    return GREATER_OR_EQUAL;
                case GREATER_THAN:
                    return LESS_THAN;
                case GREATER_OR_EQUAL:
                    return LESS_OR_EQUAL;
                default:
                    return this;
            }
        }
    }
}
