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
import org.apache.paimon.types.VecType;

import java.io.Serializable;
import java.util.Objects;

/**
 * An internal data structure representing data of {@link VecType}.
 *
 * <p>Note: All elements of this data structure must be numeric.
 *
 * <p>{@link ArrayBasedVec} is an implementation of {@link InternalVec} which wraps {@link
 * InternalArray}.
 *
 * @since 2.0.0
 */
@Public
public final class ArrayBasedVec implements InternalVec, Serializable {

    private static final long serialVersionUID = 1L;
    private final InternalArray array;

    private ArrayBasedVec(InternalArray array) {
        this.array = array;
    }

    /** Creates an instance of {@link ArrayBasedVec} by {@link InternalArray}. */
    public static ArrayBasedVec from(InternalArray array) {
        return new ArrayBasedVec(array);
    }

    public InternalArray getInnerArray() {
        return array;
    }

    @Override
    public int size() {
        return array.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return array.isNullAt(pos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArrayBasedVec that = (ArrayBasedVec) o;
        return array.equals(that.array);
    }

    @Override
    public int hashCode() {
        return Objects.hash(array.size(), array);
    }

    @Override
    public boolean getBoolean(int pos) {
        return array.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return array.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return array.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return array.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return array.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return array.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return array.getDouble(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Binary.");
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Variant.");
    }

    @Override
    public Blob getBlob(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Blob.");
    }

    @Override
    public BinaryString getString(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support BinaryString.");
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Decimal.");
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Timestamp.");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Row.");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support nested Array.");
    }

    @Override
    public InternalVec getVec(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support nested VecType.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("ArrayBasedVec does not support Map.");
    }

    @Override
    public boolean[] toBooleanArray() {
        return array.toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return array.toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return array.toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return array.toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return array.toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return array.toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return array.toDoubleArray();
    }
}
