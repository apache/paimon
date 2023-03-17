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

/**
 * Utilities for {@link BinaryRow}. Many of the methods in this class are used in code generation.
 *
 * <p>This is directly copied from {@link BinaryRowDataUtil}.
 */
public class BinaryRowDataUtil {

    public static final sun.misc.Unsafe UNSAFE = MemorySegment.UNSAFE;
    public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

    static {
        int size = EMPTY_ROW.getFixedLengthPartSize();
        byte[] bytes = new byte[size];
        EMPTY_ROW.pointTo(MemorySegment.wrap(bytes), 0, size);
    }

    public static boolean byteArrayEquals(byte[] left, byte[] right, int length) {
        return byteArrayEquals(left, BYTE_ARRAY_BASE_OFFSET, right, BYTE_ARRAY_BASE_OFFSET, length);
    }

    public static boolean byteArrayEquals(
            Object left, long leftOffset, Object right, long rightOffset, int length) {
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
}
