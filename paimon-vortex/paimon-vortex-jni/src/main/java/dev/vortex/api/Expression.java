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

package dev.vortex.api;

import dev.vortex.jni.NativeExpression;

import java.math.BigInteger;

/** A Vortex expression backed by a native pointer. */
public final class Expression implements AutoCloseable {

    private long pointer;

    private Expression(long pointer) {
        this.pointer = pointer;
    }

    public long nativePointer() {
        return pointer;
    }

    @Override
    public void close() {
        if (pointer != 0) {
            NativeExpression.free(pointer);
            pointer = 0;
        }
    }

    // -- Structure navigation --

    public static Expression root() {
        return new Expression(NativeExpression.root());
    }

    public static Expression column(String name) {
        long rootPtr = NativeExpression.root();
        try {
            return new Expression(NativeExpression.getItem(name, rootPtr));
        } finally {
            // Vortex builders do not take ownership of their inputs -- see the module doc of
            // vortex-jni/src/expression.rs at 0.73.0 -- so releasing this intermediate is our
            // responsibility, and getItem has already cloned it into the expression we return.
            // It is never wrapped in an Expression, so without this no caller can reach it.
            NativeExpression.free(rootPtr);
        }
    }

    public static Expression select(String[] columns, Expression parent) {
        return new Expression(NativeExpression.select(columns, parent.pointer));
    }

    /** The index of each row in its file, as a non-nullable u64. */
    public static Expression rowIdx() {
        return new Expression(NativeExpression.rowIdx());
    }

    /** A struct whose fields are the given expressions, in order, under the given names. */
    public static Expression pack(String[] fieldNames, Expression[] exprs, boolean nullable) {
        return new Expression(NativeExpression.pack(fieldNames, pointers(exprs), nullable));
    }

    // -- Logical combinators --

    public static Expression and(Expression... exprs) {
        return new Expression(NativeExpression.and(pointers(exprs)));
    }

    public static Expression or(Expression... exprs) {
        return new Expression(NativeExpression.or(pointers(exprs)));
    }

    public static Expression not(Expression expr) {
        return new Expression(NativeExpression.not(expr.pointer));
    }

    // -- Comparison / binary ops --

    public static Expression binary(BinaryOp op, Expression left, Expression right) {
        return new Expression(NativeExpression.binary(op.code(), left.pointer, right.pointer));
    }

    // -- Null checks --

    public static Expression isNull(Expression expr) {
        return new Expression(NativeExpression.isNull(expr.pointer));
    }

    public static Expression isNotNull(Expression expr) {
        return new Expression(NativeExpression.isNotNull(expr.pointer));
    }

    // -- Pattern, range and list membership --

    /**
     * SQL {@code LIKE}. {@code negated} gives {@code NOT LIKE} and {@code caseInsensitive} gives
     * {@code ILIKE}.
     */
    public static Expression like(
            Expression expr, Expression pattern, boolean negated, boolean caseInsensitive) {
        return new Expression(
                NativeExpression.like(expr.pointer, pattern.pointer, negated, caseInsensitive));
    }

    /**
     * {@code lower <= value <= upper}. A strict bound excludes its end: {@code lowerStrict} gives
     * {@code lower < value} and {@code upperStrict} gives {@code value < upper}.
     */
    public static Expression between(
            Expression value,
            Expression lower,
            Expression upper,
            boolean lowerStrict,
            boolean upperStrict) {
        return new Expression(
                NativeExpression.between(
                        value.pointer, lower.pointer, upper.pointer, lowerStrict, upperStrict));
    }

    /**
     * SQL {@code value IN (list)}. The list must be a list literal. A null value, or a non-match
     * against a list that holds a null, gives null and so never admits the row.
     */
    public static Expression in(Expression value, Expression list) {
        return new Expression(NativeExpression.listContains(list.pointer, value.pointer, true));
    }

    /**
     * SQL {@code value NOT IN (list)}. A null value, or a non-match against a list that holds a
     * null, gives null and so never admits the row.
     */
    public static Expression notIn(Expression value, Expression list) {
        long in = NativeExpression.listContains(list.pointer, value.pointer, true);
        try {
            return new Expression(NativeExpression.not(in));
        } finally {
            NativeExpression.free(in);
        }
    }

    // -- Primitive literals --

    public static Expression literal(boolean value) {
        return new Expression(NativeExpression.literalBool(value, false));
    }

    public static Expression literal(byte value) {
        return new Expression(NativeExpression.literalI8(value, false));
    }

    public static Expression literal(short value) {
        return new Expression(NativeExpression.literalI16(value, false));
    }

    public static Expression literal(int value) {
        return new Expression(NativeExpression.literalI32(value, false));
    }

    public static Expression literal(long value) {
        return new Expression(NativeExpression.literalI64(value, false));
    }

    public static Expression literal(float value) {
        return new Expression(NativeExpression.literalF32(value, false));
    }

    public static Expression literal(double value) {
        return new Expression(NativeExpression.literalF64(value, false));
    }

    public static Expression literal(String value) {
        return new Expression(NativeExpression.literalString(value));
    }

    public static Expression literal(byte[] value) {
        return new Expression(NativeExpression.literalBinary(value));
    }

    // -- Decimal literals --

    public static Expression literalDecimal(BigInteger unscaledValue, int precision, int scale) {
        byte[] bytes = unscaledValue.toByteArray();
        return new Expression(NativeExpression.literalDecimal(bytes, precision, scale, false));
    }

    public static Expression nullLiteralDecimal(int precision, int scale) {
        return new Expression(
                NativeExpression.literalDecimal(new byte[] {0}, precision, scale, true));
    }

    // -- Date/time literals --

    public static Expression literalDate(long value, TimeUnit unit) {
        return new Expression(NativeExpression.literalDate(value, unit.tag(), false));
    }

    public static Expression nullLiteralDate(TimeUnit unit) {
        return new Expression(NativeExpression.literalDate(0L, unit.tag(), true));
    }

    public static Expression literalTimestamp(long value, TimeUnit unit, String timezone) {
        return new Expression(
                NativeExpression.literalTimestamp(value, unit.tag(), timezone, false));
    }

    public static Expression nullLiteralTimestamp(TimeUnit unit, String timezone) {
        return new Expression(NativeExpression.literalTimestamp(0L, unit.tag(), timezone, true));
    }

    /** A time-of-day literal: {@code value} units since midnight. */
    public static Expression literalTime(long value, TimeUnit unit) {
        return new Expression(NativeExpression.literalTime(value, unit.tag(), false));
    }

    public static Expression nullLiteralTime(TimeUnit unit) {
        return new Expression(NativeExpression.literalTime(0L, unit.tag(), true));
    }

    // -- Null literals --

    public static Expression nullLiteral(DType dtype) {
        return new Expression(NativeExpression.literalNull(dtype.tag()));
    }

    // -- List literals --

    /**
     * A list literal. {@code elementType} is a typed literal, normally a typed null, that gives the
     * element dtype. Every element must itself be a literal and is cast to that dtype.
     */
    public static Expression literalList(
            Expression elementType, boolean elementsNullable, Expression... elements) {
        return new Expression(
                NativeExpression.literalList(
                        pointers(elements), elementType.pointer, elementsNullable, false));
    }

    private static long[] pointers(Expression[] exprs) {
        long[] ptrs = new long[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            ptrs[i] = exprs[i].pointer;
        }
        return ptrs;
    }

    /** Binary operation codes. */
    public enum BinaryOp {
        EQ((byte) 0),
        NOT_EQ((byte) 1),
        GT((byte) 2),
        GTE((byte) 3),
        LT((byte) 4),
        LTE((byte) 5),
        AND((byte) 6),
        OR((byte) 7),
        ADD((byte) 8),
        SUB((byte) 9),
        MUL((byte) 10),
        DIV((byte) 11);

        private final byte code;

        BinaryOp(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }
    }

    /** Data type tags for null literals. */
    public enum DType {
        BOOL((byte) 0),
        I8((byte) 1),
        I16((byte) 2),
        I32((byte) 3),
        I64((byte) 4),
        F32((byte) 5),
        F64((byte) 6),
        UTF8((byte) 7),
        BINARY((byte) 8),
        U8((byte) 9),
        U16((byte) 10),
        U32((byte) 11),
        U64((byte) 12),
        F16((byte) 13);

        private final byte tag;

        DType(byte tag) {
            this.tag = tag;
        }

        public byte tag() {
            return tag;
        }
    }

    /** Time unit tags for date/time literals. */
    public enum TimeUnit {
        NANOSECONDS((byte) 0),
        MICROSECONDS((byte) 1),
        MILLISECONDS((byte) 2),
        SECONDS((byte) 3),
        DAYS((byte) 4);

        private final byte tag;

        TimeUnit(byte tag) {
            this.tag = tag;
        }

        public byte tag() {
            return tag;
        }
    }
}
