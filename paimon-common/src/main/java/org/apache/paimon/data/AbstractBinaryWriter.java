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

package org.apache.paimon.data;

import org.apache.paimon.data.serializer.InternalArraySerializer;
import org.apache.paimon.data.serializer.InternalMapSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.paimon.data.BinarySection.MAX_FIX_PART_DATA_SIZE;

/**
 * Use the special format to write data to a {@link MemorySegment} (its capacity grows
 * automatically).
 *
 * <p>If write a format binary: 1. New a writer. 2. Write each field by writeXX or setNullAt.
 * (Variable length fields can not be written repeatedly.) 3. Invoke {@link #complete()}.
 *
 * <p>If want to reuse this writer, please invoke {@link #reset()} first.
 */
abstract class AbstractBinaryWriter implements BinaryWriter {

    protected MemorySegment segment;

    protected int cursor;

    /** Set offset and size to fix len part. */
    protected abstract void setOffsetAndSize(int pos, int offset, long size);

    /** Get field offset. */
    protected abstract int getFieldOffset(int pos);

    /** After grow, need point to new memory. */
    protected abstract void afterGrow();

    protected abstract void setNullBit(int ordinal);

    /** See {@link MemorySegmentUtils#readBinaryString(MemorySegment[], int, int, long)}. */
    @Override
    public void writeString(int pos, BinaryString input) {
        if (input.getSegments() == null) {
            String javaObject = input.toString();
            writeBytes(pos, javaObject.getBytes(StandardCharsets.UTF_8));
        } else {
            int len = input.getSizeInBytes();
            if (len <= 7) {
                byte[] bytes = MemorySegmentUtils.allocateReuseBytes(len);
                MemorySegmentUtils.copyToBytes(
                        input.getSegments(), input.getOffset(), bytes, 0, len);
                writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
            } else {
                writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
            }
        }
    }

    private void writeBytes(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, len);
        }
    }

    @Override
    public void writeArray(int pos, InternalArray input, InternalArraySerializer serializer) {
        BinaryArray binary = serializer.toBinaryArray(input);
        writeSegmentsToVarLenPart(
                pos, binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
    }

    @Override
    public void writeMap(int pos, InternalMap input, InternalMapSerializer serializer) {
        BinaryMap binary = serializer.toBinaryMap(input);
        writeSegmentsToVarLenPart(
                pos, binary.getSegments(), binary.getOffset(), binary.getSizeInBytes());
    }

    @Override
    public void writeRow(int pos, InternalRow input, InternalRowSerializer serializer) {
        if (input instanceof BinarySection) {
            BinarySection row = (BinarySection) input;
            writeSegmentsToVarLenPart(
                    pos, row.getSegments(), row.getOffset(), row.getSizeInBytes());
        } else {
            BinaryRow row = serializer.toBinaryRow(input);
            writeSegmentsToVarLenPart(
                    pos, row.getSegments(), row.getOffset(), row.getSizeInBytes());
        }
    }

    @Override
    public void writeBinary(int pos, byte[] bytes) {
        int len = bytes.length;
        if (len <= BinarySection.MAX_FIX_PART_DATA_SIZE) {
            writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
        } else {
            writeBytesToVarLenPart(pos, bytes, len);
        }
    }

    @Override
    public void writeDecimal(int pos, Decimal value, int precision) {
        assert value == null || (value.precision() == precision);

        if (Decimal.isCompact(precision)) {
            assert value != null;
            writeLong(pos, value.toUnscaledLong());
        } else {
            // grow the global buffer before writing data.
            ensureCapacity(16);

            // zero-out the bytes
            segment.putLong(cursor, 0L);
            segment.putLong(cursor + 8, 0L);

            // Make sure Decimal object has the same scale as DecimalType.
            // Note that we may pass in null Decimal object to set null for it.
            if (value == null) {
                setNullBit(pos);
                // keep the offset for future update
                setOffsetAndSize(pos, cursor, 0);
            } else {
                final byte[] bytes = value.toUnscaledBytes();
                assert bytes.length <= 16;

                // Write the bytes to the variable length portion.
                segment.put(cursor, bytes, 0, bytes.length);
                setOffsetAndSize(pos, cursor, bytes.length);
            }

            // move the cursor forward.
            cursor += 16;
        }
    }

    @Override
    public void writeTimestamp(int pos, Timestamp value, int precision) {
        if (Timestamp.isCompact(precision)) {
            writeLong(pos, value.getMillisecond());
        } else {
            // store the nanoOfMillisecond in fixed-length part as offset and nanoOfMillisecond
            ensureCapacity(8);

            if (value == null) {
                setNullBit(pos);
                // zero-out the bytes
                segment.putLong(cursor, 0L);
                setOffsetAndSize(pos, cursor, 0);
            } else {
                segment.putLong(cursor, value.getMillisecond());
                setOffsetAndSize(pos, cursor, value.getNanoOfMillisecond());
            }

            cursor += 8;
        }
    }

    protected void zeroOutPaddingBytes(int numBytes) {
        if ((numBytes & 0x07) > 0) {
            segment.putLong(cursor + ((numBytes >> 3) << 3), 0L);
        }
    }

    protected void ensureCapacity(int neededSize) {
        final int length = cursor + neededSize;
        if (segment.size() < length) {
            grow(length);
        }
    }

    private void writeSegmentsToVarLenPart(
            int pos, MemorySegment[] segments, int offset, int size) {
        final int roundedSize = roundNumberOfBytesToNearestWord(size);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(size);

        if (segments.length == 1) {
            segments[0].copyTo(offset, segment, cursor, size);
        } else {
            writeMultiSegmentsToVarLenPart(segments, offset, size);
        }

        setOffsetAndSize(pos, cursor, size);

        // move the cursor forward.
        cursor += roundedSize;
    }

    private void writeMultiSegmentsToVarLenPart(MemorySegment[] segments, int offset, int size) {
        // Write the bytes to the variable length portion.
        int needCopy = size;
        int fromOffset = offset;
        int toOffset = cursor;
        for (MemorySegment sourceSegment : segments) {
            int remain = sourceSegment.size() - fromOffset;
            if (remain > 0) {
                int copySize = Math.min(remain, needCopy);
                sourceSegment.copyTo(fromOffset, segment, toOffset, copySize);
                needCopy -= copySize;
                toOffset += copySize;
                fromOffset = 0;
            } else {
                fromOffset -= sourceSegment.size();
            }
        }
    }

    private void writeBytesToVarLenPart(int pos, byte[] bytes, int len) {
        final int roundedSize = roundNumberOfBytesToNearestWord(len);

        // grow the global buffer before writing data.
        ensureCapacity(roundedSize);

        zeroOutPaddingBytes(len);

        // Write the bytes to the variable length portion.
        segment.put(cursor, bytes, 0, len);

        setOffsetAndSize(pos, cursor, len);

        // move the cursor forward.
        cursor += roundedSize;
    }

    /** Increases the capacity to ensure that it can hold at least the minimum capacity argument. */
    private void grow(int minCapacity) {
        int oldCapacity = segment.size();
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }
        segment = MemorySegment.wrap(Arrays.copyOf(segment.getArray(), newCapacity));
        afterGrow();
    }

    protected static int roundNumberOfBytesToNearestWord(int numBytes) {
        int remainder = numBytes & 0x07;
        if (remainder == 0) {
            return numBytes;
        } else {
            return numBytes + (8 - remainder);
        }
    }

    private static void writeBytesToFixLenPart(
            MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
        long firstByte = len | 0x80; // first bit is 1, other bits is len
        long sevenBytes = 0L; // real data
        if (BinaryRow.LITTLE_ENDIAN) {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
            }
        } else {
            for (int i = 0; i < len; i++) {
                sevenBytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
            }
        }

        final long offsetAndSize = (firstByte << 56) | sevenBytes;

        segment.putLong(fieldOffset, offsetAndSize);
    }

    public MemorySegment getSegments() {
        return segment;
    }
}
