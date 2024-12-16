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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;

/**
 * An implementation of {@link InternalArray} which provides a casted view of the underlying {@link
 * InternalArray}.
 *
 * <p>It reads data from underlying {@link InternalArray} according to source logical type and casts
 * it with specific {@link CastExecutor}.
 */
public class CastedArray implements InternalArray {

    private final CastElementGetter castElementGetter;
    private InternalArray array;

    protected CastedArray(CastElementGetter castElementGetter) {
        this.castElementGetter = castElementGetter;
    }

    /**
     * Replaces the underlying {@link InternalArray} backing this {@link CastedArray}.
     *
     * <p>This method replaces the array in place and does not return a new object. This is done for
     * performance reasons.
     */
    public static CastedArray from(CastElementGetter castElementGetter) {
        return new CastedArray(castElementGetter);
    }

    public CastedArray replaceArray(InternalArray array) {
        this.array = array;
        return this;
    }

    @Override
    public int size() {
        return array.size();
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public short[] toShortArray() {
        short[] result = new short[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public int[] toIntArray() {
        int[] result = new int[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public long[] toLongArray() {
        long[] result = new long[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public float[] toFloatArray() {
        float[] result = new float[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public double[] toDoubleArray() {
        double[] result = new double[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    @Override
    public boolean isNullAt(int pos) {
        return castElementGetter.getElementOrNull(array, pos) == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public byte getByte(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public short getShort(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public int getInt(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public long getLong(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public float getFloat(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public double getDouble(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return castElementGetter.getElementOrNull(array, pos);
    }
}
