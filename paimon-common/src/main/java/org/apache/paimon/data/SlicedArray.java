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

import java.io.Serializable;

/**
 * An implementation of {@link InternalArray} which represents a sliced view of an {@link
 * InternalArray}.
 *
 * @since 2.0
 */
@Public
public final class SlicedArray implements InternalArray, Serializable {

    private static final long serialVersionUID = 1L;

    private final InternalArray array;
    private final int offset;
    private final int size;

    public SlicedArray(InternalArray array, int offset, int size) {
        if (offset < 0 || size < 0 || offset + size > array.size()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid slice offset %s and size %s for array size %s.",
                            offset, size, array.size()));
        }

        if (array instanceof SlicedArray) {
            SlicedArray slicedArray = (SlicedArray) array;
            this.array = slicedArray.array;
            this.offset = slicedArray.offset + offset;
        } else {
            this.array = array;
            this.offset = offset;
        }
        this.size = size;
    }

    public InternalArray array() {
        return array;
    }

    public int offset() {
        return offset;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isNullAt(int pos) {
        return array.isNullAt(toArrayPos(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return array.getBoolean(toArrayPos(pos));
    }

    @Override
    public byte getByte(int pos) {
        return array.getByte(toArrayPos(pos));
    }

    @Override
    public short getShort(int pos) {
        return array.getShort(toArrayPos(pos));
    }

    @Override
    public int getInt(int pos) {
        return array.getInt(toArrayPos(pos));
    }

    @Override
    public long getLong(int pos) {
        return array.getLong(toArrayPos(pos));
    }

    @Override
    public float getFloat(int pos) {
        return array.getFloat(toArrayPos(pos));
    }

    @Override
    public double getDouble(int pos) {
        return array.getDouble(toArrayPos(pos));
    }

    @Override
    public BinaryString getString(int pos) {
        return array.getString(toArrayPos(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return array.getDecimal(toArrayPos(pos), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return array.getTimestamp(toArrayPos(pos), precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return array.getBinary(toArrayPos(pos));
    }

    @Override
    public Variant getVariant(int pos) {
        return array.getVariant(toArrayPos(pos));
    }

    @Override
    public Blob getBlob(int pos) {
        return array.getBlob(toArrayPos(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return array.getArray(toArrayPos(pos));
    }

    @Override
    public InternalVector getVector(int pos) {
        return array.getVector(toArrayPos(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return array.getMap(toArrayPos(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return array.getRow(toArrayPos(pos), numFields);
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[size];
        System.arraycopy(array.toBooleanArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[size];
        System.arraycopy(array.toByteArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public short[] toShortArray() {
        short[] result = new short[size];
        System.arraycopy(array.toShortArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public int[] toIntArray() {
        int[] result = new int[size];
        System.arraycopy(array.toIntArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public long[] toLongArray() {
        long[] result = new long[size];
        System.arraycopy(array.toLongArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public float[] toFloatArray() {
        float[] result = new float[size];
        System.arraycopy(array.toFloatArray(), offset, result, 0, size);
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        double[] result = new double[size];
        System.arraycopy(array.toDoubleArray(), offset, result, 0, size);
        return result;
    }

    private int toArrayPos(int pos) {
        if (pos < 0 || pos >= size) {
            throw new IndexOutOfBoundsException(
                    String.format("Position %s is out of bounds for array size %s.", pos, size));
        }
        return offset + pos;
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "SlicedArray do not support equals, please compare fields one by one!");
    }
}
