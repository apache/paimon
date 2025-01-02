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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.utils.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link ArrayType}.
 *
 * <p>Note: All elements of this data structure must be internal data structures and must be of the
 * same type. See {@link InternalRow} for more information about internal data structures.
 *
 * <p>{@link GenericArray} is a generic implementation of {@link InternalArray} which wraps regular
 * Java arrays.
 *
 * @since 0.4.0
 */
@Public
public final class GenericArray implements InternalArray, Serializable {

    private static final long serialVersionUID = 1L;

    private final Object array;
    private final int size;
    private final boolean isPrimitiveArray;

    /**
     * Creates an instance of {@link GenericArray} using the given Java array.
     *
     * <p>Note: All elements of the array must be internal data structures.
     */
    public GenericArray(Object[] array) {
        this(array, array.length, false);
    }

    public GenericArray(int[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(long[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(float[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(double[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(short[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(byte[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    public GenericArray(boolean[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    private GenericArray(Object array, int size, boolean isPrimitiveArray) {
        this.array = array;
        this.size = size;
        this.isPrimitiveArray = isPrimitiveArray;
    }

    /**
     * Returns true if this is a primitive array.
     *
     * <p>A primitive array is an array whose elements are of primitive type.
     */
    public boolean isPrimitiveArray() {
        return isPrimitiveArray;
    }

    /**
     * Converts this {@link GenericArray} into an array of Java {@link Object}.
     *
     * <p>The method will convert a primitive array into an object array. But it will not convert
     * internal data structures into external data structures (e.g. {@link BinaryString} to {@link
     * String}).
     */
    public Object[] toObjectArray() {
        if (isPrimitiveArray) {
            Class<?> arrayClass = array.getClass();
            if (int[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((int[]) array);
            } else if (long[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((long[]) array);
            } else if (float[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((float[]) array);
            } else if (double[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((double[]) array);
            } else if (short[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((short[]) array);
            } else if (byte[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((byte[]) array);
            } else if (boolean[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((boolean[]) array);
            }
            throw new RuntimeException("Unsupported primitive array: " + arrayClass);
        } else {
            return (Object[]) array;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isNullAt(int pos) {
        return !isPrimitiveArray && ((Object[]) array)[pos] == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericArray that = (GenericArray) o;
        return size == that.size
                && isPrimitiveArray == that.isPrimitiveArray
                && Objects.deepEquals(array, that.array);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(size, isPrimitiveArray);
        result = 31 * result + Arrays.deepHashCode(new Object[] {array});
        return result;
    }

    // ------------------------------------------------------------------------------------------
    // Read-only accessor methods
    // ------------------------------------------------------------------------------------------

    @Override
    public boolean getBoolean(int pos) {
        return isPrimitiveArray ? ((boolean[]) array)[pos] : (boolean) getObject(pos);
    }

    @Override
    public byte getByte(int pos) {
        return isPrimitiveArray ? ((byte[]) array)[pos] : (byte) getObject(pos);
    }

    @Override
    public short getShort(int pos) {
        return isPrimitiveArray ? ((short[]) array)[pos] : (short) getObject(pos);
    }

    @Override
    public int getInt(int pos) {
        return isPrimitiveArray ? ((int[]) array)[pos] : (int) getObject(pos);
    }

    @Override
    public long getLong(int pos) {
        return isPrimitiveArray ? ((long[]) array)[pos] : (long) getObject(pos);
    }

    @Override
    public float getFloat(int pos) {
        return isPrimitiveArray ? ((float[]) array)[pos] : (float) getObject(pos);
    }

    @Override
    public double getDouble(int pos) {
        return isPrimitiveArray ? ((double[]) array)[pos] : (double) getObject(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) getObject(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return (Variant) getObject(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return (BinaryString) getObject(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return (Decimal) getObject(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return (Timestamp) getObject(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return (InternalRow) getObject(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) getObject(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return (InternalMap) getObject(pos);
    }

    private Object getObject(int pos) {
        return ((Object[]) array)[pos];
    }

    // ------------------------------------------------------------------------------------------
    // Conversion Utilities
    // ------------------------------------------------------------------------------------------

    private boolean anyNull() {
        for (Object element : (Object[]) array) {
            if (element == null) {
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
        if (isPrimitiveArray) {
            return (boolean[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveBoolean((Object[]) array);
    }

    @Override
    public byte[] toByteArray() {
        if (isPrimitiveArray) {
            return (byte[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveByte((Object[]) array);
    }

    @Override
    public short[] toShortArray() {
        if (isPrimitiveArray) {
            return (short[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveShort((Object[]) array);
    }

    @Override
    public int[] toIntArray() {
        if (isPrimitiveArray) {
            return (int[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveInteger((Object[]) array);
    }

    @Override
    public long[] toLongArray() {
        if (isPrimitiveArray) {
            return (long[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveLong((Object[]) array);
    }

    @Override
    public float[] toFloatArray() {
        if (isPrimitiveArray) {
            return (float[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveFloat((Object[]) array);
    }

    @Override
    public double[] toDoubleArray() {
        if (isPrimitiveArray) {
            return (double[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveDouble((Object[]) array);
    }
}
