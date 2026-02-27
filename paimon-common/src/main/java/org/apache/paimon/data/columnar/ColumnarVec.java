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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

import java.io.Serializable;

/** Columnar VectorType to support access to vector column data. */
public final class ColumnarVec implements InternalVector, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private final ColumnVector data;
    private final int offset;
    private final int numElements;

    private ColumnarVec(ColumnVector data, int offset, int numElements) {
        this.data = data;
        this.offset = offset;
        this.numElements = numElements;
    }

    public static final Factory DEFAULT_FACTORY = new Factory();

    /** A Factory is used to ensure that ColumnarVec does not contain null elements. */
    public static class Factory {
        public final ColumnarVec create(ColumnVector data, int offset, int numElements) {
            if (offset < 0) {
                throw new IllegalArgumentException("Offset must be non-negative.");
            }
            if (numElements <= 0) {
                throw new IllegalArgumentException("Number of elements must be positive.");
            }
            ensureNonNull(data, offset, numElements);
            return new ColumnarVec(data, offset, numElements);
        }

        protected void ensureNonNull(ColumnVector data, int offset, int numElements) {
            final int limit = offset + numElements;
            for (int pos = offset; pos < limit; ++pos) {
                if (data.isNullAt(pos)) {
                    throw new UnsupportedOperationException("ColumnarVec refuse null elements.");
                }
            }
        }
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public boolean isNullAt(int pos) {
        // Elements in vector must be not null.
        return false;
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean getBoolean(int pos) {
        return ((BooleanColumnVector) data).getBoolean(offset + pos);
    }

    @Override
    public byte getByte(int pos) {
        return ((ByteColumnVector) data).getByte(offset + pos);
    }

    @Override
    public short getShort(int pos) {
        return ((ShortColumnVector) data).getShort(offset + pos);
    }

    @Override
    public int getInt(int pos) {
        return ((IntColumnVector) data).getInt(offset + pos);
    }

    @Override
    public long getLong(int pos) {
        return ((LongColumnVector) data).getLong(offset + pos);
    }

    @Override
    public float getFloat(int pos) {
        return ((FloatColumnVector) data).getFloat(offset + pos);
    }

    @Override
    public double getDouble(int pos) {
        return ((DoubleColumnVector) data).getDouble(offset + pos);
    }

    @Override
    public BinaryString getString(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public Blob getBlob(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public InternalVector getVector(int pos) {
        return ((VecColumnVector) data).getVector(offset + pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] res = new boolean[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getBoolean(i);
        }
        return res;
    }

    @Override
    public byte[] toByteArray() {
        byte[] res = new byte[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getByte(i);
        }
        return res;
    }

    @Override
    public short[] toShortArray() {
        short[] res = new short[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getShort(i);
        }
        return res;
    }

    @Override
    public int[] toIntArray() {
        int[] res = new int[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getInt(i);
        }
        return res;
    }

    @Override
    public long[] toLongArray() {
        long[] res = new long[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getLong(i);
        }
        return res;
    }

    @Override
    public float[] toFloatArray() {
        float[] res = new float[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getFloat(i);
        }
        return res;
    }

    @Override
    public double[] toDoubleArray() {
        double[] res = new double[numElements];
        for (int i = 0; i < numElements; i++) {
            res[i] = getDouble(i);
        }
        return res;
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarVector do not support equals, please compare fields one by one!");
    }
}
