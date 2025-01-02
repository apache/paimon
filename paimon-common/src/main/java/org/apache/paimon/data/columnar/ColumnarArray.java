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
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.Variant;

import java.io.Serializable;
import java.util.Arrays;

/** Columnar array to support access to vector column data. */
public final class ColumnarArray implements InternalArray, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private final ColumnVector data;
    private final int offset;
    private final int numElements;

    public ColumnarArray(ColumnVector data, int offset, int numElements) {
        this.data = data;
        this.offset = offset;
        this.numElements = numElements;
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public boolean isNullAt(int pos) {
        return data.isNullAt(offset + pos);
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
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return ((DecimalColumnVector) data).getDecimal(offset + pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return ((TimestampColumnVector) data).getTimestamp(offset + pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        BytesColumnVector.Bytes byteArray = getByteArray(pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            return Arrays.copyOfRange(
                    byteArray.data, byteArray.offset, byteArray.offset + byteArray.len);
        }
    }

    @Override
    public Variant getVariant(int pos) {
        InternalRow row = getRow(pos, 2);
        byte[] value = row.getBinary(0);
        byte[] metadata = row.getBinary(1);
        return new GenericVariant(value, metadata);
    }

    @Override
    public InternalArray getArray(int pos) {
        return ((ArrayColumnVector) data).getArray(offset + pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return ((MapColumnVector) data).getMap(offset + pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return ((RowColumnVector) data).getRow(offset + pos);
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

    private BytesColumnVector.Bytes getByteArray(int pos) {
        return ((BytesColumnVector) data).getBytes(offset + pos);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarArray do not support equals, please compare fields one by one!");
    }
}
