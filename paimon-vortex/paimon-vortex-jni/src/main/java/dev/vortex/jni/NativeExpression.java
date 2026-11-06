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

package dev.vortex.jni;

/**
 * Native methods for Vortex expression construction.
 *
 * <p>Builders do not take ownership of their inputs. The caller must free every pointer it gets.
 */
public final class NativeExpression {

    static {
        NativeLoader.loadJni();
    }

    private NativeExpression() {}

    public static native long root();

    public static native long rowIdx();

    public static native long getItem(String path, long parentPtr);

    public static native long select(String[] columns, long parentPtr);

    public static native long pack(String[] fieldNames, long[] exprPtrs, boolean nullable);

    public static native long merge(long[] exprPtrs, byte duplicateHandling);

    public static native long and(long[] exprPtrs);

    public static native long or(long[] exprPtrs);

    public static native long binary(byte opCode, long leftPtr, long rightPtr);

    public static native long spatial(byte function, long[] operandPtrs);

    public static native long not(long exprPtr);

    public static native long isNull(long exprPtr);

    public static native long isNotNull(long exprPtr);

    public static native long like(
            long exprPtr, long patternPtr, boolean negated, boolean caseInsensitive);

    public static native long between(
            long valuePtr, long lowerPtr, long upperPtr, boolean lowerStrict, boolean upperStrict);

    public static native long listContains(long listPtr, long needlePtr, boolean sqlNullSemantics);

    public static native long literalBool(boolean value, boolean isNull);

    public static native long literalI8(byte value, boolean isNull);

    public static native long literalI16(short value, boolean isNull);

    public static native long literalI32(int value, boolean isNull);

    public static native long literalI64(long value, boolean isNull);

    public static native long literalU8(byte bits, boolean isNull);

    public static native long literalU16(short bits, boolean isNull);

    public static native long literalU32(int bits, boolean isNull);

    public static native long literalU64(long bits, boolean isNull);

    public static native long literalF16(float value, boolean isNull);

    public static native long literalF32(float value, boolean isNull);

    public static native long literalF64(double value, boolean isNull);

    public static native long literalString(String value);

    public static native long literalBinary(byte[] value);

    public static native long literalDecimal(
            byte[] bigIntBytes, int precision, int scale, boolean isNull);

    public static native long literalDate(long value, byte timeUnitTag, boolean isNull);

    public static native long literalTimestamp(
            long value, byte timeUnitTag, String timezone, boolean isNull);

    public static native long literalTime(long value, byte timeUnitTag, boolean isNull);

    public static native long literalUuid(byte[] bigEndianBytes, boolean isNull);

    public static native long literalGeometry(byte[] wkb);

    public static native long literalNull(byte dTypeTag);

    public static native long literalList(
            long[] elementPtrs, long elementTypePtr, boolean elementsNullable, boolean isNull);

    public static native long literalFixedSizeList(
            long[] elementPtrs,
            long elementTypePtr,
            boolean elementsNullable,
            int size,
            boolean isNull);

    public static native long literalStruct(String[] fieldNames, long[] fieldPtrs, boolean isNull);

    public static native long literalMap(
            long[] keyPtrs,
            long[] valuePtrs,
            long keyTypePtr,
            long valueTypePtr,
            boolean valuesNullable,
            boolean isNull);

    public static native void free(long exprPtr);
}
