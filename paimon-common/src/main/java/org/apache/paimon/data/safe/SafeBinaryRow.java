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
import org.apache.paimon.memory.BytesUtils;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.data.BinaryRow.HEADER_SIZE_IN_BITS;
import static org.apache.paimon.data.BinaryRow.calculateBitSetWidthInBytes;
import static org.apache.paimon.memory.MemorySegmentUtils.BIT_BYTE_INDEX_MASK;
import static org.apache.paimon.memory.MemorySegmentUtils.byteIndex;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link BinaryRow} which is safe avoid core dump. */
public final class SafeBinaryRow implements InternalRow {

    private final int arity;
    private final int nullBitsSizeInBytes;
    private final byte[] bytes;
    private final int offset;

    public SafeBinaryRow(int arity, byte[] bytes, int offset) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
        this.bytes = bytes;
        this.offset = offset;
    }

    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    @Override
    public int getFieldCount() {
        return arity;
    }

    @Override
    public RowKind getRowKind() {
        byte kindValue = bytes[offset];
        return RowKind.fromByteValue(kindValue);
    }

    @Override
    public void setRowKind(RowKind kind) {
        bytes[offset] = kind.toByteValue();
    }

    @Override
    public boolean isNullAt(int pos) {
        int index = pos + HEADER_SIZE_IN_BITS;
        int offset = this.offset + byteIndex(index);
        byte current = bytes[offset];
        return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
    }

    @Override
    public boolean getBoolean(int pos) {
        return bytes[getFieldOffset(pos)] != 0;
    }

    @Override
    public byte getByte(int pos) {
        return bytes[getFieldOffset(pos)];
    }

    @Override
    public short getShort(int pos) {
        return BytesUtils.getShort(bytes, getFieldOffset(pos));
    }

    @Override
    public int getInt(int pos) {
        return BytesUtils.getInt(bytes, getFieldOffset(pos));
    }

    @Override
    public long getLong(int pos) {
        return BytesUtils.getLong(bytes, getFieldOffset(pos));
    }

    @Override
    public float getFloat(int pos) {
        return Float.intBitsToFloat(getInt(pos));
    }

    @Override
    public double getDouble(int pos) {
        return Double.longBitsToDouble(getLong(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(getBinary(pos));
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
        final long millisecond = BytesUtils.getLong(bytes, offset + subOffset);
        return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    @Override
    public byte[] getBinary(int pos) {
        return BytesUtils.readBinary(bytes, offset, getFieldOffset(pos), getLong(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return readArrayData(bytes, offset, getLong(pos));
    }

    private static InternalArray readArrayData(byte[] bytes, int baseOffset, long offsetAndSize) {
        int offset = (int) (offsetAndSize >> 32);
        return new SafeBinaryArray(bytes, offset + baseOffset);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return readNestedRow(bytes, numFields, offset, getLong(pos));
    }

    private static InternalRow readNestedRow(
            byte[] bytes, int numFields, int baseOffset, long offsetAndSize) {
        int offset = (int) (offsetAndSize >> 32);
        return new SafeBinaryRow(numFields, bytes, offset + baseOffset);
    }

    @Override
    public InternalMap getMap(int pos) {
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
}
