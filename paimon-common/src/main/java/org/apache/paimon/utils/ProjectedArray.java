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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;

/**
 * An implementation of {@link InternalArray} which provides a projected view of the underlying
 * {@link InternalArray}.
 *
 * <p>Projection includes both reducing the accessible fields and reordering them.
 *
 * <p>Note: This class supports only top-level projections, not nested projections.
 */
public class ProjectedArray implements InternalArray {

    private final int[] indexMapping;

    private InternalArray array;

    private ProjectedArray(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    /**
     * Replaces the underlying {@link InternalArray} backing this {@link ProjectedArray}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public ProjectedArray replaceArray(InternalArray array) {
        this.array = array;
        return this;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int size() {
        return indexMapping.length;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (indexMapping[pos] < 0) {
            return true;
        }
        return array.isNullAt(indexMapping[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return array.getBoolean(indexMapping[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return array.getByte(indexMapping[pos]);
    }

    @Override
    public short getShort(int pos) {
        return array.getShort(indexMapping[pos]);
    }

    @Override
    public int getInt(int pos) {
        return array.getInt(indexMapping[pos]);
    }

    @Override
    public long getLong(int pos) {
        return array.getLong(indexMapping[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return array.getFloat(indexMapping[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return array.getDouble(indexMapping[pos]);
    }

    @Override
    public BinaryString getString(int pos) {
        return array.getString(indexMapping[pos]);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return array.getDecimal(indexMapping[pos], precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return array.getTimestamp(indexMapping[pos], precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return array.getBinary(indexMapping[pos]);
    }

    @Override
    public Variant getVariant(int pos) {
        return array.getVariant(indexMapping[pos]);
    }

    @Override
    public InternalArray getArray(int pos) {
        return array.getArray(indexMapping[pos]);
    }

    @Override
    public InternalMap getMap(int pos) {
        return array.getMap(indexMapping[pos]);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return array.getRow(indexMapping[pos], numFields);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException("Projected row data cannot be compared");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Projected row data cannot be hashed");
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("Projected row data cannot be toString");
    }

    /**
     * Create an empty {@link ProjectedArray} starting from a {@code projection} array.
     *
     * <p>The array represents the mapping of the fields of the original {@link DataType}. For
     * example, {@code [0, 2, 1]} specifies to include in the following order the 1st field, the 3rd
     * field and the 2nd field of the row.
     *
     * @see Projection
     * @see ProjectedArray
     */
    public static ProjectedArray from(int[] projection) {
        return new ProjectedArray(projection);
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
