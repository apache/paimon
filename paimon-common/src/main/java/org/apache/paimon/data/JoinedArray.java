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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link InternalArray} which is backed by concatenated {@link InternalArray}
 * instances.
 *
 * @since 2.0
 */
@Public
public class JoinedArray implements InternalArray {

    private final List<InternalArray> arrays = new ArrayList<>();
    private final List<Integer> offsets = new ArrayList<>();
    private int size;

    private int lastIndex = 0;
    private int currentArrayIndex = 0;
    private int currentElementIndex = 0;

    /**
     * Creates a new {@link JoinedArray} backed by array1 and array2.
     *
     * <p>If any backing array is also a {@link JoinedArray}, its backing arrays will be flattened
     * into this {@link JoinedArray}.
     *
     * @param array1 the first array
     * @param array2 the second array
     */
    public JoinedArray(InternalArray array1, InternalArray array2) {
        offsets.add(0);
        concat(array1);
        concat(array2);
    }

    public JoinedArray concat(InternalArray toConcat) {
        if (toConcat instanceof JoinedArray) {
            JoinedArray joinedArray = (JoinedArray) toConcat;
            for (InternalArray array : joinedArray.arrays) {
                addArray(array);
            }
        } else {
            addArray(toConcat);
        }
        return this;
    }

    public List<InternalArray> arrays() {
        return Collections.unmodifiableList(arrays);
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isNullAt(int pos) {
        InternalArray array = arrayAt(pos);
        return array.isNullAt(currentElementIndex);
    }

    @Override
    public boolean getBoolean(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getBoolean(currentElementIndex);
    }

    @Override
    public byte getByte(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getByte(currentElementIndex);
    }

    @Override
    public short getShort(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getShort(currentElementIndex);
    }

    @Override
    public int getInt(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getInt(currentElementIndex);
    }

    @Override
    public long getLong(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getLong(currentElementIndex);
    }

    @Override
    public float getFloat(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getFloat(currentElementIndex);
    }

    @Override
    public double getDouble(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getDouble(currentElementIndex);
    }

    @Override
    public BinaryString getString(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getString(currentElementIndex);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        InternalArray array = arrayAt(pos);
        return array.getDecimal(currentElementIndex, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        InternalArray array = arrayAt(pos);
        return array.getTimestamp(currentElementIndex, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getBinary(currentElementIndex);
    }

    @Override
    public Variant getVariant(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getVariant(currentElementIndex);
    }

    @Override
    public Blob getBlob(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getBlob(currentElementIndex);
    }

    @Override
    public InternalArray getArray(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getArray(currentElementIndex);
    }

    @Override
    public InternalVector getVector(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getVector(currentElementIndex);
    }

    @Override
    public InternalMap getMap(int pos) {
        InternalArray array = arrayAt(pos);
        return array.getMap(currentElementIndex);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        InternalArray array = arrayAt(pos);
        return array.getRow(currentElementIndex, numFields);
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            boolean[] elements = array.toBooleanArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            byte[] elements = array.toByteArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        short[] result = new short[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            short[] elements = array.toShortArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        int[] result = new int[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            int[] elements = array.toIntArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        long[] result = new long[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            long[] elements = array.toLongArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        float[] result = new float[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            float[] elements = array.toFloatArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        double[] result = new double[size];
        int offset = 0;
        for (InternalArray array : arrays) {
            double[] elements = array.toDoubleArray();
            System.arraycopy(elements, 0, result, offset, elements.length);
            offset += elements.length;
        }
        return result;
    }

    private InternalArray arrayAt(int pos) {
        if (pos < 0 || pos >= size) {
            throw new IndexOutOfBoundsException(
                    "Position " + pos + " is out of bounds for array size " + size);
        }

        if (pos == lastIndex) {
            return arrays.get(currentArrayIndex);
        }

        if (pos == lastIndex + 1) {
            if (pos < offsets.get(currentArrayIndex + 1)) {
                currentElementIndex++;
            } else {
                currentArrayIndex++;
                currentElementIndex = 0;
            }
            lastIndex = pos;
            return arrays.get(currentArrayIndex);
        }

        int head = 0, tail = offsets.size() - 1;
        while (head < tail) {
            int mid = (head + tail + 1) / 2;
            if (offsets.get(mid) <= pos) {
                head = mid;
            } else {
                tail = mid - 1;
            }
        }
        currentArrayIndex = head;
        currentElementIndex = pos - offsets.get(currentArrayIndex);
        lastIndex = pos;
        return arrays.get(currentArrayIndex);
    }

    private void addArray(InternalArray array) {
        int arraySize = array.size();
        if (arraySize == 0) {
            return;
        }
        arrays.add(array);
        size += arraySize;
        offsets.add(size);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinedArray that = (JoinedArray) o;
        return Objects.equals(arrays, that.arrays);
    }

    @Override
    public int hashCode() {
        return Objects.hash(arrays);
    }

    @Override
    public String toString() {
        return "JoinedArray{" + "arrays=" + arrays + '}';
    }
}
