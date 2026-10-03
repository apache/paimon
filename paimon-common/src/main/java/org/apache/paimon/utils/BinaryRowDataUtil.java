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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.memory.MemorySegment;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Utilities for {@link BinaryRow}. Many of the methods in this class are used in code generation.
 *
 * <p>This is directly copied from {@link BinaryRowDataUtil}.
 */
public class BinaryRowDataUtil {

    public static final sun.misc.Unsafe UNSAFE = MemorySegment.UNSAFE;
    public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    public static final BinaryRow EMPTY_ROW = new BinaryRow(0);
    // JDK 9+: delegate to the SIMD-intrinsified Arrays.equals range overload. The Object +
    // offset we receive is BASE + index for a byte[], so the index is offset - BASE.
    private static final MethodHandle ARRAYS_EQUALS_RANGE;

    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
        MethodHandle mh;
        try {
            mh =
                    MethodHandles.publicLookup()
                            .findStatic(
                                    Arrays.class,
                                    "equals",
                                    MethodType.methodType(
                                            boolean.class,
                                            byte[].class,
                                            int.class,
                                            int.class,
                                            byte[].class,
                                            int.class,
                                            int.class));
        } catch (Throwable t) {
            mh = null;
        }
        ARRAYS_EQUALS_RANGE = mh;
    }

    public static boolean byteArrayEquals(byte[] left, byte[] right, int length) {
        return byteArrayEquals(left, BYTE_ARRAY_BASE_OFFSET, right, BYTE_ARRAY_BASE_OFFSET, length);
    }

    public static boolean byteArrayEquals(
            Object left, long leftOffset, Object right, long rightOffset, int length) {
        if (ARRAYS_EQUALS_RANGE != null) {
            int lFrom = (int) (leftOffset - BYTE_ARRAY_BASE_OFFSET);
            int rFrom = (int) (rightOffset - BYTE_ARRAY_BASE_OFFSET);
            try {
                return (boolean)
                        ARRAYS_EQUALS_RANGE.invokeExact(
                                (byte[]) left,
                                lFrom,
                                lFrom + length,
                                (byte[]) right,
                                rFrom,
                                rFrom + length);
            } catch (Throwable ignored) {
                // Fall through to the Unsafe loop on any unexpected failure.
            }
        }
        int i = 0;

        while (i <= length - 8) {
            if (UNSAFE.getLong(left, leftOffset + i) != UNSAFE.getLong(right, rightOffset + i)) {
                return false;
            }
            i += 8;
        }

        while (i < length) {
            if (UNSAFE.getByte(left, leftOffset + i) != UNSAFE.getByte(right, rightOffset + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }

    public static boolean byteBufferEquals(ByteBuffer a, ByteBuffer b) {
        int n = a.remaining();
        if (n != b.remaining()) {
            return false;
        }
        if (a.hasArray() && b.hasArray()) {
            return byteArrayEquals(
                    a.array(),
                    BYTE_ARRAY_BASE_OFFSET + a.arrayOffset() + a.position(),
                    b.array(),
                    BYTE_ARRAY_BASE_OFFSET + b.arrayOffset() + b.position(),
                    n);
        }
        int aPos = a.position();
        int bPos = b.position();
        int i = 0;
        while (i <= n - 8) {
            if (a.getLong(aPos + i) != b.getLong(bPos + i)) {
                return false;
            }
            i += 8;
        }
        while (i < n) {
            if (a.get(aPos + i) != b.get(bPos + i)) {
                return false;
            }
            i += 1;
        }
        return true;
    }
}
