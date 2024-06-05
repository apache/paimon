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

package org.apache.paimon.data.safe;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;

import static org.apache.paimon.data.BinaryArray.calculateHeaderInBytes;
import static org.apache.paimon.memory.MemorySegment.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.paimon.memory.MemorySegment.UNSAFE;
import static org.apache.paimon.memory.MemorySegmentUtils.BIT_BYTE_INDEX_MASK;
import static org.apache.paimon.memory.MemorySegmentUtils.byteIndex;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link BinaryRow} which is safe avoid core dump. */
public final class SafeBinaryArray implements InternalArray {

    private final int size;
    private final byte[] bytes;
    private final int offset;
    private final int elementOffset;

    public SafeBinaryArray(byte[] bytes, int offset) {
        checkArgument(bytes.length > offset + 4);
        final int size = UNSAFE.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
        assert size >= 0 : "size (" + size + ") should >= 0";

        this.size = size;
        this.bytes = bytes;
        this.offset = offset;
        this.elementOffset = offset + calculateHeaderInBytes(this.size);
    }

    @Override
    public int size() {
        return size;
    }

    private int getElementOffset(int ordinal, int elementSize) {
        return elementOffset + ordinal * elementSize;
    }

    @Override
    public boolean isNullAt(int pos) {
        byte current = bytes[offset + 4 + byteIndex(pos)];
        return (current & (1 << (pos & BIT_BYTE_INDEX_MASK))) != 0;
    }

    @Override
    public boolean getBoolean(int pos) {
        return bytes[getElementOffset(pos, 1)] != 0;
    }

    @Override
    public byte getByte(int pos) {
        return bytes[getElementOffset(pos, 1)];
    }

    @Override
    public short getShort(int pos) {
        int fieldOffset = getElementOffset(pos, 2);
        checkArgument(bytes.length >= fieldOffset + 2);
        return UNSAFE.getShort(bytes, fieldOffset + BYTE_ARRAY_BASE_OFFSET);
    }

    @Override
    public int getInt(int pos) {
        int fieldOffset = getElementOffset(pos, 4);
        checkArgument(bytes.length >= fieldOffset + 4);
        return UNSAFE.getInt(bytes, fieldOffset + BYTE_ARRAY_BASE_OFFSET);
    }

    @Override
    public long getLong(int pos) {
        int fieldOffset = getElementOffset(pos, 8);
        checkArgument(bytes.length >= fieldOffset + 8);
        return UNSAFE.getLong(bytes, fieldOffset + BYTE_ARRAY_BASE_OFFSET);
    }

    @Override
    public float getFloat(int pos) {
        int fieldOffset = getElementOffset(pos, 4);
        checkArgument(bytes.length >= fieldOffset + 4);
        return UNSAFE.getFloat(bytes, fieldOffset + BYTE_ARRAY_BASE_OFFSET);
    }

    @Override
    public double getDouble(int pos) {
        int fieldOffset = getElementOffset(pos, 8);
        checkArgument(bytes.length >= fieldOffset + 8);
        return UNSAFE.getDouble(bytes, fieldOffset + BYTE_ARRAY_BASE_OFFSET);
    }

    private MemorySegment[] toSegmentArray() {
        return new MemorySegment[] {MemorySegment.wrap(bytes)};
    }

    @Override
    public BinaryString getString(int pos) {
        int fieldOffset = getElementOffset(pos, 8);
        long offsetAndLen = getLong(pos);
        BinaryString string =
                MemorySegmentUtils.readBinaryString(
                        toSegmentArray(), offset, fieldOffset, offsetAndLen);
        checkArgument(bytes.length >= string.getOffset() + string.getSizeInBytes());
        return string;
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        long longValue = getLong(pos);
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(longValue, precision, scale);
        }

        final int size = ((int) longValue);
        int subOffset = (int) (longValue >> 32);
        byte[] decimalBytes = new byte[size];
        System.arraycopy(bytes, offset + subOffset, decimalBytes, 0, size);
        return Decimal.fromUnscaledBytes(decimalBytes, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        long longValue = getLong(pos);
        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(longValue);
        }

        final int nanoOfMillisecond = (int) longValue;
        final int subOffset = (int) (longValue >> 32);

        checkArgument(bytes.length >= offset + subOffset + 8);
        final long millisecond = UNSAFE.getLong(bytes, offset + subOffset + BYTE_ARRAY_BASE_OFFSET);
        return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    @Override
    public byte[] getBinary(int pos) {
        return getString(pos).toBytes();
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean[] toBooleanArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] toByteArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short[] toShortArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int[] toIntArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long[] toLongArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float[] toFloatArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double[] toDoubleArray() {
        throw new UnsupportedOperationException();
    }
}
