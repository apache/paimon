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

import java.lang.reflect.Array;

import static org.apache.paimon.memory.MemorySegment.UNSAFE;

/**
 * A binary implementation of {@link InternalArray} which is backed by {@link MemorySegment}s.
 *
 * <p>For fields that hold fixed-length primitive types, such as long, double or int, they are
 * stored compacted in bytes, just like the original java array.
 *
 * <p>The binary layout of {@link BinaryArray}:
 *
 * <pre>
 * [size(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 * </pre>
 *
 * @since 0.4.0
 */
@Public
public final class BinaryArray extends BinarySection implements InternalArray, DataSetters {

    private static final long serialVersionUID = 1L;

    /** Offset for Arrays. */
    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
    private static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
    private static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
    private static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    private static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
    private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

    public static int calculateHeaderInBytes(int numFields) {
        return 4 + ((numFields + 31) / 32) * 4;
    }

    /**
     * It store real value when type is primitive. It store the length and offset of variable-length
     * part when type is string, map, etc.
     */
    public static int calculateFixLengthPartSize(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
            case VARIANT:
                // long and double are 8 bytes;
                // otherwise it stores the length and offset of the variable-length part for types
                // such as is string, map, etc.
                return 8;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    // The number of elements in this array
    private transient int size;

    /** The position to start storing array elements. */
    private transient int elementOffset;

    public BinaryArray() {}

    private void assertIndexIsValid(int ordinal) {
        assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
        assert ordinal < size : "ordinal (" + ordinal + ") should < " + size;
    }

    private int getElementOffset(int ordinal, int elementSize) {
        return elementOffset + ordinal * elementSize;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        // Read the number of elements from the first 4 bytes.
        final int size = MemorySegmentUtils.getInt(segments, offset);
        assert size >= 0 : "size (" + size + ") should >= 0";

        this.size = size;
        this.segments = segments;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.elementOffset = offset + calculateHeaderInBytes(this.size);
    }

    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.bitGet(segments, offset + 4, pos);
    }

    @Override
    public void setNullAt(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
    }

    public void setNotNullAt(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitUnSet(segments, offset + 4, pos);
    }

    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getLong(segments, getElementOffset(pos, 8));
    }

    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setLong(segments, getElementOffset(pos, 8), value);
    }

    public void setNullLong(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setLong(segments, getElementOffset(pos, 8), 0L);
    }

    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getInt(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setInt(segments, getElementOffset(pos, 4), value);
    }

    public void setNullInt(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setInt(segments, getElementOffset(pos, 4), 0);
    }

    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);
        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    MemorySegmentUtils.getLong(segments, getElementOffset(pos, 8)),
                    precision,
                    scale);
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(
                    MemorySegmentUtils.getLong(segments, getElementOffset(pos, 8)));
        }

        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndNanoOfMilli = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
    }

    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndSize);
    }

    @Override
    public Variant getVariant(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndLen = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readVariant(segments, offset, offsetAndLen);
    }

    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        int fieldOffset = getElementOffset(pos, 8);
        final long offsetAndSize = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readRowData(segments, numFields, offset, offsetAndSize);
    }

    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getBoolean(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), value);
    }

    public void setNullBoolean(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setBoolean(segments, getElementOffset(pos, 1), false);
    }

    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getByte(segments, getElementOffset(pos, 1));
    }

    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setByte(segments, getElementOffset(pos, 1), value);
    }

    public void setNullByte(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setByte(segments, getElementOffset(pos, 1), (byte) 0);
    }

    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getShort(segments, getElementOffset(pos, 2));
    }

    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setShort(segments, getElementOffset(pos, 2), value);
    }

    public void setNullShort(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setShort(segments, getElementOffset(pos, 2), (short) 0);
    }

    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getFloat(segments, getElementOffset(pos, 4));
    }

    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setFloat(segments, getElementOffset(pos, 4), value);
    }

    public void setNullFloat(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setFloat(segments, getElementOffset(pos, 4), 0F);
    }

    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getDouble(segments, getElementOffset(pos, 8));
    }

    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setDouble(segments, getElementOffset(pos, 8), value);
    }

    public void setNullDouble(int pos) {
        assertIndexIsValid(pos);
        MemorySegmentUtils.bitSet(segments, offset + 4, pos);
        MemorySegmentUtils.setDouble(segments, getElementOffset(pos, 8), 0.0);
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // compact format
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (MemorySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // zero-out the bytes
            MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
            MemorySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // keep the offset for future update
                MemorySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert (bytes.length <= 16);

                // Write the bytes to the variable length portion.
                MemorySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            setLong(pos, value.getMillisecond());
        } else {
            int fieldOffset = getElementOffset(pos, 8);
            int cursor = (int) (MemorySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // zero-out the bytes
                MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
                // keep the offset for future update
                MemorySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // write millisecond to the variable length portion.
                MemorySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // write nanoOfMillisecond to the fixed-length portion.
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    public boolean anyNull() {
        for (int i = offset + 4; i < elementOffset; i += 4) {
            if (MemorySegmentUtils.getInt(segments, i) != 0) {
                return true;
            }
        }
        return false;
    }

    private void checkNoNull() {
        if (anyNull()) {
            throw new RuntimeException("Primitive array must not contain a null value.");
        }
    }

    @Override
    public boolean[] toBooleanArray() {
        checkNoNull();
        boolean[] values = new boolean[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, size);
        return values;
    }

    @Override
    public byte[] toByteArray() {
        checkNoNull();
        byte[] values = new byte[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, size);
        return values;
    }

    @Override
    public short[] toShortArray() {
        checkNoNull();
        short[] values = new short[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, SHORT_ARRAY_OFFSET, size * 2);
        return values;
    }

    @Override
    public int[] toIntArray() {
        checkNoNull();
        int[] values = new int[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, INT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public long[] toLongArray() {
        checkNoNull();
        long[] values = new long[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, LONG_ARRAY_OFFSET, size * 8);
        return values;
    }

    @Override
    public float[] toFloatArray() {
        checkNoNull();
        float[] values = new float[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, FLOAT_ARRAY_OFFSET, size * 4);
        return values;
    }

    @Override
    public double[] toDoubleArray() {
        checkNoNull();
        double[] values = new double[size];
        MemorySegmentUtils.copyToUnsafe(
                segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, size * 8);
        return values;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toObjectArray(DataType elementType) {
        Class<T> elementClass = (Class<T>) InternalRow.getDataClass(elementType);
        InternalArray.ElementGetter elementGetter = InternalArray.createElementGetter(elementType);
        T[] values = (T[]) Array.newInstance(elementClass, size);
        for (int i = 0; i < size; i++) {
            if (!isNullAt(i)) {
                values[i] = (T) elementGetter.getElementOrNull(this, i);
            }
        }
        return values;
    }

    public BinaryArray copy() {
        return copy(new BinaryArray());
    }

    public BinaryArray copy(BinaryArray reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }

    // ------------------------------------------------------------------------------------------
    // Construction Utilities
    // ------------------------------------------------------------------------------------------

    public static BinaryArray fromPrimitiveArray(boolean[] arr) {
        return fromPrimitiveArray(arr, BOOLEAN_ARRAY_OFFSET, arr.length, 1);
    }

    public static BinaryArray fromPrimitiveArray(byte[] arr) {
        return fromPrimitiveArray(arr, BYTE_ARRAY_BASE_OFFSET, arr.length, 1);
    }

    public static BinaryArray fromPrimitiveArray(short[] arr) {
        return fromPrimitiveArray(arr, SHORT_ARRAY_OFFSET, arr.length, 2);
    }

    public static BinaryArray fromPrimitiveArray(int[] arr) {
        return fromPrimitiveArray(arr, INT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArray fromPrimitiveArray(long[] arr) {
        return fromPrimitiveArray(arr, LONG_ARRAY_OFFSET, arr.length, 8);
    }

    public static BinaryArray fromPrimitiveArray(float[] arr) {
        return fromPrimitiveArray(arr, FLOAT_ARRAY_OFFSET, arr.length, 4);
    }

    public static BinaryArray fromPrimitiveArray(double[] arr) {
        return fromPrimitiveArray(arr, DOUBLE_ARRAY_OFFSET, arr.length, 8);
    }

    private static BinaryArray fromPrimitiveArray(
            Object arr, int offset, int length, int elementSize) {
        final long headerInBytes = calculateHeaderInBytes(length);
        final long valueRegionInBytes = elementSize * length;

        // must align by 8 bytes
        long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
        if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
            throw new UnsupportedOperationException(
                    "Cannot convert this array to unsafe format as " + "it's too big.");
        }
        long totalSize = totalSizeInLongs * 8;

        final byte[] data = new byte[(int) totalSize];

        UNSAFE.putInt(data, (long) BYTE_ARRAY_BASE_OFFSET, length);
        UNSAFE.copyMemory(
                arr, offset, data, BYTE_ARRAY_BASE_OFFSET + headerInBytes, valueRegionInBytes);

        BinaryArray result = new BinaryArray();
        result.pointTo(MemorySegment.wrap(data), 0, (int) totalSize);
        return result;
    }

    public static BinaryArray fromLongArray(Long[] arr) {
        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, arr.length, 8);
        for (int i = 0; i < arr.length; i++) {
            Long v = arr[i];
            if (v == null) {
                writer.setNullLong(i);
            } else {
                writer.writeLong(i, v);
            }
        }
        writer.complete();
        return array;
    }

    public static BinaryArray fromLongArray(InternalArray arr) {
        if (arr instanceof BinaryArray) {
            return (BinaryArray) arr;
        }

        BinaryArray array = new BinaryArray();
        BinaryArrayWriter writer = new BinaryArrayWriter(array, arr.size(), 8);
        for (int i = 0; i < arr.size(); i++) {
            if (arr.isNullAt(i)) {
                writer.setNullLong(i);
            } else {
                writer.writeLong(i, arr.getLong(i));
            }
        }
        writer.complete();
        return array;
    }
}
