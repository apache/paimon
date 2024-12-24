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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;

import java.util.List;

/** HivePaimonArray for Array type. */
public class HivePaimonArray implements InternalArray {

    private final DataType elementType;
    private final List<Object> list;

    public HivePaimonArray(DataType elementType, List<Object> list) {
        this.list = list;
        this.elementType = elementType;
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isNullAt(int i) {
        return list.get(i) == null;
    }

    public List<Object> getList() {
        return list;
    }

    @SuppressWarnings("unchecked")
    private <T> T getAs(int i) {
        return (T) list.get(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return getAs(i);
    }

    @Override
    public byte getByte(int i) {
        return getAs(i);
    }

    @Override
    public short getShort(int i) {
        return getAs(i);
    }

    @Override
    public int getInt(int i) {
        return getAs(i);
    }

    @Override
    public long getLong(int i) {
        return getAs(i);
    }

    @Override
    public float getFloat(int i) {
        return getAs(i);
    }

    @Override
    public double getDouble(int i) {
        return getAs(i);
    }

    @Override
    public BinaryString getString(int i) {
        return getAs(i);
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        return getAs(i);
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        return getAs(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return getAs(i);
    }

    @Override
    public Variant getVariant(int pos) {
        return getAs(pos);
    }

    @Override
    public InternalArray getArray(int i) {
        return new HivePaimonArray(
                ((ArrayType) elementType).getElementType(),
                ((HivePaimonArray) this.getAs(i)).getList());
    }

    @Override
    public InternalMap getMap(int i) {
        return getAs(i);
    }

    @Override
    public InternalRow getRow(int i, int i1) {
        return getAs(i);
    }

    @Override
    public boolean[] toBooleanArray() {
        boolean[] res = new boolean[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getBoolean(i);
        }
        return res;
    }

    @Override
    public byte[] toByteArray() {
        byte[] res = new byte[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getByte(i);
        }
        return res;
    }

    @Override
    public short[] toShortArray() {
        short[] res = new short[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getShort(i);
        }
        return res;
    }

    @Override
    public int[] toIntArray() {
        int[] res = new int[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getInt(i);
        }
        return res;
    }

    @Override
    public long[] toLongArray() {
        long[] res = new long[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getLong(i);
        }
        return res;
    }

    @Override
    public float[] toFloatArray() {
        float[] res = new float[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getFloat(i);
        }
        return res;
    }

    @Override
    public double[] toDoubleArray() {
        double[] res = new double[size()];
        for (int i = 0; i < size(); i++) {
            res[i] = getDouble(i);
        }
        return res;
    }
}
