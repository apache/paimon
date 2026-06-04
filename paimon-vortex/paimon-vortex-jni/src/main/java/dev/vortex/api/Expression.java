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
        return new Expression(NativeExpression.getItem(name, rootPtr));
    }

    public static Expression select(String[] columns, Expression parent) {
        return new Expression(NativeExpression.select(columns, parent.pointer));
    }

    // -- Logical combinators --

    public static Expression and(Expression... exprs) {
        long[] ptrs = new long[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            ptrs[i] = exprs[i].pointer;
        }
        return new Expression(NativeExpression.and(ptrs));
    }

    public static Expression or(Expression... exprs) {
        long[] ptrs = new long[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            ptrs[i] = exprs[i].pointer;
        }
        return new Expression(NativeExpression.or(ptrs));
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

    // -- Decimal literals --

    public static Expression literalDecimal(BigInteger unscaledValue, int precision, int scale) {
        byte[] bytes = unscaledValue.toByteArray();
        return new Expression(NativeExpression.literalDecimal(bytes, precision, scale, false));
    }

    // -- Date/time literals --

    public static Expression literalDate(long value, TimeUnit unit) {
        return new Expression(NativeExpression.literalDate(value, unit.tag(), false));
    }

    public static Expression literalTimestamp(long value, TimeUnit unit, String timezone) {
        return new Expression(
                NativeExpression.literalTimestamp(value, unit.tag(), timezone, false));
    }

    // -- Null literals --

    public static Expression nullLiteral(DType dtype) {
        return new Expression(NativeExpression.literalNull(dtype.tag()));
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
        OR((byte) 7);

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
        BINARY((byte) 8);

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
