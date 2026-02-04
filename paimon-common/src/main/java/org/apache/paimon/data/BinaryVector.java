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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataType;

import java.util.Objects;

import static org.apache.paimon.memory.MemorySegment.UNSAFE;

/**
 * A binary implementation of {@link InternalVector} which is backed by {@link MemorySegment}s.
 *
 * <p>Compared to {@link BinaryArray}, {@link BinaryVector} stores only primitive types, and
 * nullable is not supported yet. Thus, the memory layout is very simple.
 *
 * <p>The binary layout of {@link BinaryVector}:
 *
 * <pre>
 * [values]
 * </pre>
 *
 * @since 2.0.0
 */
@Public
public final class BinaryVector extends BinarySection implements InternalVector, DataSetters {

    private static final long serialVersionUID = 1L;

    /** Offset for Arrays. */
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
    private static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
    private static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    private static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    private static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    public static int getPrimitiveElementSize(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
                return 4;
            case BIGINT:
            case DOUBLE:
                return 8;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    // The number of elements in this vector
    private final int size;

    public BinaryVector(int size) {
        this.size = size;
    }

    private void assertIndexIsValid(int ordinal) {
        assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
        assert ordinal < size : "ordinal (" + ordinal + ") should < " + size;
    }

    private int getElementOffset(int ordinal, int elementSize) {
        return offset + ordinal * elementSize;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return false;
    }

    @Override
    public void setNullAt(int pos) {
        assertIndexIsValid(pos);
        throw new UnsupportedOperationException("Nullable is not supported yet for VectorType.");
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getLong(segments, getElementOffset(pos, 8));
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setLong(segments, getElementOffset(pos, 8), value);
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getInt(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setInt(segments, getElementOffset(pos, 4), value);
    }

    @Override
    public BinaryString getString(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support String.");
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException("BinaryVector does not support Decimal.");
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("BinaryVector does not support Timestamp.");
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support Binary.");
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support Variant.");
    }

    @Override
    public Blob getBlob(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support Blob.");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support nested Array.");
    }

    @Override
    public InternalVector getVector(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support nested Vector.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("BinaryVector does not support Map.");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("BinaryVector does not support nested Row.");
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getBoolean(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), value);
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getByte(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setByte(segments, getElementOffset(pos, 1), value);
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getShort(segments, getElementOffset(pos, 2));
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setShort(segments, getElementOffset(pos, 2), value);
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getFloat(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setFloat(segments, getElementOffset(pos, 4), value);
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getDouble(segments, getElementOffset(pos, 8));
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.setDouble(segments, getElementOffset(pos, 8), value);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        throw new UnsupportedOperationException("BinaryVector does not support Decimal.");
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        throw new UnsupportedOperationException("BinaryVector does not support Timestamp.");
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] values = new boolean[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, BOOLEAN_ARRAY_OFFSET, size);
        return values;
    }

    @Override
    public byte[] toByteArray() {
        byte[] values = new byte[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, BYTE_ARRAY_BASE_OFFSET, size);
        return values;
    }

    @Override
    public short[] toShortArray() {
        short[] values = new short[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, SHORT_ARRAY_OFFSET, size * 2);
        return values;
    }

    @Override
    public int[] toIntArray() {
        int[] values = new int[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, INT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public long[] toLongArray() {
        long[] values = new long[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, LONG_ARRAY_OFFSET, size * 8);
        return values;
    }

    @Override
    public float[] toFloatArray() {
        float[] values = new float[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, FLOAT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public double[] toDoubleArray() {
        double[] values = new double[size];
        MemorySegmentUtils.copyToUnsafe(segments, offset, values, DOUBLE_ARRAY_OFFSET, size * 8);
        return values;
    }

    public BinaryVector copy() {
        return copy(new BinaryVector(size));
    }

    public BinaryVector copy(BinaryVector reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public int hashCode() {
        return Objects.hash(MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes), size);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryVector fromPrimitiveArray(boolean[] arr) {
        return fromPrimitiveArray(arr, BOOLEAN_ARRAY_OFFSET, arr.length, 1);
    }

    public static BinaryVector fromPrimitiveArray(byte[] arr) {
        return fromPrimitiveArray(arr, BYTE_ARRAY_BASE_OFFSET, arr.length, 1);
    }

    public static BinaryVector fromPrimitiveArray(short[] arr) {
        return fromPrimitiveArray(arr, SHORT_ARRAY_OFFSET, arr.length, 2);
    }

    public static BinaryVector fromPrimitiveArray(int[] arr) {
        return fromPrimitiveArray(arr, INT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryVector fromPrimitiveArray(long[] arr) {
        return fromPrimitiveArray(arr, LONG_ARRAY_OFFSET, arr.length, 8);
    }

    public static BinaryVector fromPrimitiveArray(float[] arr) {
        return fromPrimitiveArray(arr, FLOAT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryVector fromPrimitiveArray(double[] arr) {
        return fromPrimitiveArray(arr, DOUBLE_ARRAY_OFFSET, arr.length, 8);
    }

    private static BinaryVector fromPrimitiveArray(
            Object arr, int offset, int length, int elementSize) {
        // must align by 8 bytes
        final long valueRegionInBytes = (long) elementSize * length;
        long totalSizeInLongs = (valueRegionInBytes + 7) / 8;
        if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
            throw new UnsupportedOperationException(
                    "Cannot convert this vector to unsafe format as " + "it's too big.");
        }
        long totalSize = totalSizeInLongs * 8;

        final byte[] data = new byte[(int) totalSize];

        UNSAFE.copyMemory(arr, offset, data, BYTE_ARRAY_BASE_OFFSET, valueRegionInBytes);

        BinaryVector result = new BinaryVector(length);
        result.pointTo(MemorySegment.wrap(data), 0, (int) totalSize);
        return result;
    }

    public static BinaryVector fromInternalArray(InternalArray array, DataType elementType) {
        final BinaryVector vector;
        switch (elementType.getTypeRoot()) {
            case BOOLEAN:
                vector = BinaryVector.fromPrimitiveArray(array.toBooleanArray());
                break;
            case TINYINT:
                vector = BinaryVector.fromPrimitiveArray(array.toByteArray());
                break;
            case SMALLINT:
                vector = BinaryVector.fromPrimitiveArray(array.toShortArray());
                break;
            case INTEGER:
                vector = BinaryVector.fromPrimitiveArray(array.toIntArray());
                break;
            case BIGINT:
                vector = BinaryVector.fromPrimitiveArray(array.toLongArray());
                break;
            case FLOAT:
                vector = BinaryVector.fromPrimitiveArray(array.toFloatArray());
                break;
            case DOUBLE:
                vector = BinaryVector.fromPrimitiveArray(array.toDoubleArray());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported element type for vector " + elementType);
        }

        // Elements in BinaryVector must be not null.
        if ((array instanceof BinaryArray) || (array instanceof GenericArray)) {
            // For some implements the check can be skipped here since
            // it has already been done in previous toPrimitiveArray methods.
            return vector;
        }
        final int size = array.size();
        for (int i = 0; i < size; ++i) {
            if (array.isNullAt(i)) {
                throw new UnsupportedOperationException("BinaryVector refuse null elements.");
            }
        }
        return vector;
    }
}
