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

package dev.vortex.api.expressions;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.vortex.api.Expression;
import dev.vortex.proto.ExprProtos;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/** Binary expression operating on two child expressions with a binary operator. */
public final class Binary implements Expression {
    private final BinaryOp operator;
    private final Expression left;
    private final Expression right;

    private Binary(BinaryOp operator, Expression left, Expression right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    public static Binary parse(byte[] metadata, List<Expression> children) {
        if (children.size() != 2) {
            throw new IllegalArgumentException(
                    "Binary expression must have exactly two children, found: " + children.size());
        }
        try {
            ExprProtos.BinaryOpts opts = ExprProtos.BinaryOpts.parseFrom(metadata);
            BinaryOp operator = BinaryOp.fromProto(opts.getOp());
            return new Binary(operator, children.get(0), children.get(1));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to parse Binary metadata", e);
        }
    }

    public static Binary of(BinaryOp operator, Expression left, Expression right) {
        return new Binary(operator, left, right);
    }

    public static Binary and(Expression first, Expression... rest) {
        Expression rhs = Stream.of(rest).reduce(Binary::and).orElse(Literal.bool(true));
        return new Binary(BinaryOp.AND, first, rhs);
    }

    public static Binary or(Expression first, Expression... rest) {
        Expression rhs = Stream.of(rest).reduce(Binary::or).orElse(Literal.bool(false));
        return new Binary(BinaryOp.OR, first, rhs);
    }

    public static Binary eq(Expression left, Expression right) {
        return new Binary(BinaryOp.EQ, left, right);
    }

    public static Binary notEq(Expression left, Expression right) {
        return new Binary(BinaryOp.NOT_EQ, left, right);
    }

    public static Binary gt(Expression left, Expression right) {
        return new Binary(BinaryOp.GT, left, right);
    }

    public static Binary gtEq(Expression left, Expression right) {
        return new Binary(BinaryOp.GT_EQ, left, right);
    }

    public static Binary lt(Expression left, Expression right) {
        return new Binary(BinaryOp.LT, left, right);
    }

    public static Binary ltEq(Expression left, Expression right) {
        return new Binary(BinaryOp.LT_EQ, left, right);
    }

    @Override
    public String id() {
        return "vortex.binary";
    }

    @Override
    public List<Expression> children() {
        return java.util.Arrays.asList(left, right);
    }

    @Override
    public Optional<byte[]> metadata() {
        return Optional.of(ExprProtos.BinaryOpts.newBuilder()
                .setOp(operator.toProto())
                .build()
                .toByteArray());
    }

    @Override
    public String toString() {
        return "(" + left + " " + operator + " " + right + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Binary binary = (Binary) o;
        return operator == binary.operator && Objects.equals(left, binary.left) && Objects.equals(right, binary.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, left, right);
    }

    @Override
    public <T> T accept(Visitor<T> visitor) {
        return visitor.visitBinary(this);
    }

    public BinaryOp getOperator() {
        return operator;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    public enum BinaryOp {
        /** Equality comparison operator (==) */
        EQ,
        /** Inequality comparison operator (!=) */
        NOT_EQ,
        /** Greater-than comparison operator (>) */
        GT,
        /** Greater-than-or-equal comparison operator (>=) */
        GT_EQ,
        /** Less-than comparison operator (&lt;) */
        LT,
        /** Less-than-or-equal comparison operator (&lt;=) */
        LT_EQ,
        /** Logical AND operator (&amp;&amp;) */
        AND,
        /** Logical OR operator (||) */
        OR,
        ;

        @Override
        public String toString() {
            switch (this) {
                case EQ:
                    return "==";
                case NOT_EQ:
                    return "!=";
                case GT:
                    return ">";
                case GT_EQ:
                    return ">=";
                case LT:
                    return "<";
                case LT_EQ:
                    return "<=";
                case AND:
                    return "&&";
                case OR:
                    return "||";
                default:
                    throw new IllegalStateException("Unknown Operator: " + this);
            }
        }

        static BinaryOp fromProto(ExprProtos.BinaryOpts.BinaryOp proto) {
            switch (proto) {
                case Eq:
                    return EQ;
                case NotEq:
                    return NOT_EQ;
                case Gt:
                    return GT;
                case Gte:
                    return GT_EQ;
                case Lt:
                    return LT;
                case Lte:
                    return LT_EQ;
                case And:
                    return AND;
                case Or:
                    return OR;
                default:
                    throw new IllegalArgumentException("Unsupported binary operator proto: " + proto);
            }
        }

        ExprProtos.BinaryOpts.BinaryOp toProto() {
            switch (this) {
                case EQ:
                    return ExprProtos.BinaryOpts.BinaryOp.Eq;
                case NOT_EQ:
                    return ExprProtos.BinaryOpts.BinaryOp.NotEq;
                case GT:
                    return ExprProtos.BinaryOpts.BinaryOp.Gt;
                case GT_EQ:
                    return ExprProtos.BinaryOpts.BinaryOp.Gte;
                case LT:
                    return ExprProtos.BinaryOpts.BinaryOp.Lt;
                case LT_EQ:
                    return ExprProtos.BinaryOpts.BinaryOp.Lte;
                case AND:
                    return ExprProtos.BinaryOpts.BinaryOp.And;
                case OR:
                    return ExprProtos.BinaryOpts.BinaryOp.Or;
                default:
                    throw new IllegalArgumentException("Unsupported binary operator: " + this);
            }
        }
    }
}
