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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegment;

import java.nio.ByteOrder;

import static org.apache.paimon.memory.MemorySegment.UNSAFE;

/** Util for sort. */
public class SortUtil {

    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    private static final int LONG_BYTES = 8;

    public static void minNormalizedKey(MemorySegment target, int offset, int numBytes) {
        // write min value.
        for (int i = 0; i < numBytes; i++) {
            target.put(offset + i, (byte) 0);
        }
    }

    /** Max unsigned byte is -1. */
    public static void maxNormalizedKey(MemorySegment target, int offset, int numBytes) {
        // write max value.
        for (int i = 0; i < numBytes; i++) {
            target.put(offset + i, (byte) -1);
        }
    }

    public static void putShortNormalizedKey(
            short value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 2) {
            // default case, full normalized key
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            target.put(offset + 1, (byte) ((value) & 0xff));
        } else if (numBytes <= 0) {
        } else if (numBytes == 1) {
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
        } else {
            int highByte = ((value >>> 8) & 0xff);
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            target.put(offset + 1, (byte) ((value) & 0xff));
            for (int i = 2; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    public static void putByteNormalizedKey(
            byte value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 1) {
            // default case, full normalized key. need to explicitly convert to int to
            // avoid false results due to implicit type conversion to int when subtracting
            // the min byte value
            int highByte = value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
        } else if (numBytes <= 0) {
        } else {
            int highByte = value & 0xff;
            highByte -= Byte.MIN_VALUE;
            target.put(offset, (byte) highByte);
            for (int i = 1; i < numBytes; i++) {
                target.put(offset + i, (byte) 0);
            }
        }
    }

    public static void putBooleanNormalizedKey(
            boolean value, MemorySegment target, int offset, int numBytes) {
        if (numBytes > 0) {
            target.put(offset, (byte) (value ? 1 : 0));

            for (offset = offset + 1; numBytes > 1; numBytes--) {
                target.put(offset++, (byte) 0);
            }
        }
    }

    /** UTF-8 supports bytes comparison. */
    public static void putStringNormalizedKey(
            BinaryString value, MemorySegment target, int offset, int numBytes) {
        final int limit = offset + numBytes;
        final int end = value.getSizeInBytes();
        for (int i = 0; i < end && offset < limit; i++) {
            target.put(offset++, value.byteAt(i));
        }

        for (int i = offset; i < limit; i++) {
            target.put(i, (byte) 0);
        }
    }

    /** Just support the compact precision decimal. */
    public static void putDecimalNormalizedKey(
            Decimal record, MemorySegment target, int offset, int len) {
        assert record.isCompact();
        putLongNormalizedKey(record.toUnscaledLong(), target, offset, len);
    }

    public static void putIntNormalizedKey(
            int value, MemorySegment target, int offset, int numBytes) {
        putUnsignedIntegerNormalizedKey(value - Integer.MIN_VALUE, target, offset, numBytes);
    }

    public static void putLongNormalizedKey(
            long value, MemorySegment target, int offset, int numBytes) {
        putUnsignedLongNormalizedKey(value - Long.MIN_VALUE, target, offset, numBytes);
    }

    /** See http://stereopsis.com/radix.html for more details. */
    public static void putFloatNormalizedKey(
            float value, MemorySegment target, int offset, int numBytes) {
        int iValue = Float.floatToIntBits(value);
        iValue ^= ((iValue >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
        putUnsignedIntegerNormalizedKey(iValue, target, offset, numBytes);
    }

    /** See http://stereopsis.com/radix.html for more details. */
    public static void putDoubleNormalizedKey(
            double value, MemorySegment target, int offset, int numBytes) {
        long lValue = Double.doubleToLongBits(value);
        lValue ^= ((lValue >> (Long.SIZE - 1)) | Long.MIN_VALUE);
        putUnsignedLongNormalizedKey(lValue, target, offset, numBytes);
    }

    public static void putBinaryNormalizedKey(
            byte[] value, MemorySegment target, int offset, int numBytes) {
        final int limit = offset + numBytes;
        final int end = value.length;
        for (int i = 0; i < end && offset < limit; i++) {
            target.put(offset++, value[i]);
        }

        for (int i = offset; i < limit; i++) {
            target.put(i, (byte) 0);
        }
    }

    /** Support the compact precision TimestampData. */
    public static void putTimestampNormalizedKey(
            Timestamp value, MemorySegment target, int offset, int numBytes) {
        assert value.getNanoOfMillisecond() == 0;
        putLongNormalizedKey(value.getMillisecond(), target, offset, numBytes);
    }

    public static void putUnsignedIntegerNormalizedKey(
            int value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 4) {
            // default case, full normalized key
            target.putIntBigEndian(offset, value);
        } else if (numBytes > 0) {
            if (numBytes < 4) {
                for (int i = 0; numBytes > 0; numBytes--, i++) {
                    target.put(offset + i, (byte) (value >>> ((3 - i) << 3)));
                }
            } else {
                target.putIntBigEndian(offset, value);
                for (int i = 4; i < numBytes; i++) {
                    target.put(offset + i, (byte) 0);
                }
            }
        }
    }

    public static void putUnsignedLongNormalizedKey(
            long value, MemorySegment target, int offset, int numBytes) {
        if (numBytes == 8) {
            // default case, full normalized key
            target.putLongBigEndian(offset, value);
        } else if (numBytes > 0) {
            if (numBytes < 8) {
                for (int i = 0; numBytes > 0; numBytes--, i++) {
                    target.put(offset + i, (byte) (value >>> ((7 - i) << 3)));
                }
            } else {
                target.putLongBigEndian(offset, value);
                for (int i = 8; i < numBytes; i++) {
                    target.put(offset + i, (byte) 0);
                }
            }
        }
    }

    public static int compareBinary(byte[] a, byte[] b) {
        return compareBinary(a, 0, a.length, b, 0, b.length);
    }

    public static int compareBinary(
            byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {
        // Short circuit equal case
        if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
            return 0;
        }
        int minLength = Math.min(length1, length2);
        int minWords = minLength / LONG_BYTES;
        int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
        int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
        for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
            long lw = UNSAFE.getLong(buffer1, offset1Adj + (long) i);
            long rw = UNSAFE.getLong(buffer2, offset2Adj + (long) i);
            long diff = lw ^ rw;

            if (diff != 0) {
                if (!LITTLE_ENDIAN) {
                    return lessThanUnsigned(lw, rw) ? -1 : 1;
                }

                // Use binary search
                int n = 0;
                int y;
                int x = (int) diff;
                if (x == 0) {
                    x = (int) (diff >>> 32);
                    n = 32;
                }

                y = x << 16;
                if (y == 0) {
                    n += 16;
                } else {
                    x = y;
                }

                y = x << 8;
                if (y == 0) {
                    n += 8;
                }
                return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
            }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * LONG_BYTES; i < minLength; i++) {
            int result =
                    unsignedByteToInt(buffer1[offset1 + i])
                            - unsignedByteToInt(buffer2[offset2 + i]);
            if (result != 0) {
                return result;
            }
        }
        return length1 - length2;
    }

    private static int unsignedByteToInt(byte value) {
        return value & 0xff;
    }

    private static boolean lessThanUnsigned(long x1, long x2) {
        return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
    }
}
