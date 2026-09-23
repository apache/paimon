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

/** Native methods for Vortex expression construction. */
public final class NativeExpression {

    static {
        NativeLoader.loadJni();
    }

    private NativeExpression() {}

    public static native long root();

    public static native long getItem(String path, long parentPtr);

    public static native long select(String[] columns, long parentPtr);

    public static native long and(long[] exprPtrs);

    public static native long or(long[] exprPtrs);

    public static native long binary(byte opCode, long leftPtr, long rightPtr);

    public static native long not(long exprPtr);

    public static native long isNull(long exprPtr);

    public static native long isNotNull(long exprPtr);

    public static native long like(
            long exprPtr, long patternPtr, boolean caseSensitive, boolean negated);

    public static native long between(
            long exprPtr, long lowPtr, long highPtr, boolean lowInclusive, boolean highInclusive);

    public static native long literalBool(boolean value, boolean isNull);

    public static native long literalI8(byte value, boolean isNull);

    public static native long literalI16(short value, boolean isNull);

    public static native long literalI32(int value, boolean isNull);

    public static native long literalI64(long value, boolean isNull);

    public static native long literalF32(float value, boolean isNull);

    public static native long literalF64(double value, boolean isNull);

    public static native long literalString(String value);

    public static native long literalBinary(byte[] value);

    public static native long literalDecimal(
            byte[] bigIntBytes, int precision, int scale, boolean isNull);

    public static native long literalDate(long value, byte timeUnitTag, boolean isNull);

    public static native long literalTimestamp(
            long value, byte timeUnitTag, String timezone, boolean isNull);

    public static native long literalNull(byte dTypeTag);

    public static native void free(long exprPtr);
}
