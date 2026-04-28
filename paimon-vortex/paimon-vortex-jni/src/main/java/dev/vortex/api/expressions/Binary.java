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

/**
 * Represents a binary expression that operates on two child expressions using a binary operator.
 * Binary expressions support comparison operations (equality, inequality, relational comparisons)
 * and boolean algebra operations (AND, OR).
 *
 * <p>This class is immutable and implements the {@link Expression} interface, making it suitable
 * for use in expression trees and query processing pipelines.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * // Create equality comparison: left == right
 * Binary equalExpr = Binary.eq(leftExpr, rightExpr);
 *
 * // Create logical AND: expr1 &amp;&amp; expr2 &amp;&amp; expr3
 * Binary andExpr = Binary.and(expr1, expr2, expr3);
 *
 * // Create greater than comparison: left > right
 * Binary gtExpr = Binary.gt(leftExpr, rightExpr);
 * </pre>
 */
public final class Binary implements Expression {
    private final BinaryOp operator;
    private final Expression left;
    private final Expression right;

    private Binary(BinaryOp operator, Expression left, Expression right) {
        this.operator = operator;
        this.left = left;
        this.right = right;
    }

    /**
     * Parses a Binary expression from protobuf metadata and child expressions.
     *
     * @param metadata the serialized protobuf metadata containing the binary operator
     * @param children the list of child expressions, must contain exactly 2 expressions
     * @return a new Binary expression instance
     * @throws RuntimeException if children size is not 2 or if metadata parsing fails
     */
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

    /**
     * Creates a new Binary expression with the specified operator and operands.
     *
     * @param operator the binary operator to apply
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression instance
     */
    public static Binary of(BinaryOp operator, Expression left, Expression right) {
        return new Binary(operator, left, right);
    }

    /**
     * Creates a logical AND expression combining multiple expressions.
     * If only one expression is provided, it returns an AND with a literal true.
     * Multiple expressions are right-associated: {@code first && (rest[0] && rest[1] && ...)}.
     *
     * @param first the first expression (left operand)
     * @param rest additional expressions to AND together (right operands)
     * @return a new Binary expression representing the logical AND operation
     */
    public static Binary and(Expression first, Expression... rest) {
        Expression rhs = Stream.of(rest).reduce(Binary::and).orElse(Literal.bool(true));
        return new Binary(BinaryOp.AND, first, rhs);
    }

    /**
     * Creates a logical OR expression combining multiple expressions.
     * If only one expression is provided, it returns an OR with a literal false.
     * Multiple expressions are right-associated: {@code first || (rest[0] || rest[1] || ...)}.
     *
     * @param first the first expression (left operand)
     * @param rest additional expressions to OR together (right operands)
     * @return a new Binary expression representing the logical OR operation
     */
    public static Binary or(Expression first, Expression... rest) {
        Expression rhs = Stream.of(rest).reduce(Binary::or).orElse(Literal.bool(false));
        return new Binary(BinaryOp.OR, first, rhs);
    }

    /**
     * Creates an equality comparison expression (==).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left == right
     */
    public static Binary eq(Expression left, Expression right) {
        return new Binary(BinaryOp.EQ, left, right);
    }

    /**
     * Creates an inequality comparison expression (!=).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left != right
     */
    public static Binary notEq(Expression left, Expression right) {
        return new Binary(BinaryOp.NOT_EQ, left, right);
    }

    /**
     * Creates a greater-than comparison expression (>).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left > right
     */
    public static Binary gt(Expression left, Expression right) {
        return new Binary(BinaryOp.GT, left, right);
    }

    /**
     * Creates a greater-than-or-equal comparison expression (>=).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left >= right
     */
    public static Binary gtEq(Expression left, Expression right) {
        return new Binary(BinaryOp.GT_EQ, left, right);
    }

    /**
     * Creates a less-than comparison expression (&lt;).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left &lt; right
     */
    public static Binary lt(Expression left, Expression right) {
        return new Binary(BinaryOp.LT, left, right);
    }

    /**
     * Creates a less-than-or-equal comparison expression (&lt;=).
     *
     * @param left the left operand expression
     * @param right the right operand expression
     * @return a new Binary expression representing left &lt;= right
     */
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

    /**
     * Returns the binary operator used in this expression.
     *
     * @return the binary operator
     */
    public BinaryOp getOperator() {
        return operator;
    }

    /**
     * Returns the left operand expression.
     *
     * @return the left operand expression
     */
    public Expression getLeft() {
        return left;
    }

    /**
     * Returns the right operand expression.
     *
     * @return the right operand expression
     */
    public Expression getRight() {
        return right;
    }

    /**
     * Enumeration of binary operators supported by binary expressions.
     * Includes comparison operators and boolean algebra operators.
     */
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
