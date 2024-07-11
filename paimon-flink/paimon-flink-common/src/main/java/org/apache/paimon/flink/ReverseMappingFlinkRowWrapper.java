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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;

/**
 * Map the incomplete schema to the complete schema, replacing missing column positions with -1.
 *
 * <p>When the column position is -1, the overridden methods starting with 'get' will return the
 * default value or null for the corresponding data type.
 */
public class ReverseMappingFlinkRowWrapper extends FlinkRowWrapper {

    private final int[] indexMapping;

    public ReverseMappingFlinkRowWrapper(
            org.apache.flink.table.data.RowData row, int[] indexMapping) {
        super(row);
        this.indexMapping = indexMapping;
    }

    @Override
    public int getFieldCount() {
        return indexMapping.length;
    }

    /** When the column position is -1, return true. */
    @Override
    public boolean isNullAt(int pos) {
        return -1 == indexMapping[pos] || super.isNullAt(indexMapping[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return !isNullAt(pos) && super.getBoolean(indexMapping[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return isNullAt(pos) ? (byte) 0 : super.getByte(indexMapping[pos]);
    }

    @Override
    public short getShort(int pos) {
        return isNullAt(pos) ? (short) 0 : super.getShort(indexMapping[pos]);
    }

    @Override
    public int getInt(int pos) {
        return isNullAt(pos) ? 0 : super.getInt(indexMapping[pos]);
    }

    @Override
    public long getLong(int pos) {
        return isNullAt(pos) ? 0L : super.getLong(indexMapping[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return isNullAt(pos) ? 0.0f : super.getFloat(indexMapping[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return isNullAt(pos) ? 0.0d : super.getDouble(indexMapping[pos]);
    }

    @Override
    public BinaryString getString(int pos) {
        return isNullAt(pos) ? null : super.getString(indexMapping[pos]);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return isNullAt(pos) ? null : super.getDecimal(indexMapping[pos], precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return isNullAt(pos) ? null : super.getTimestamp(indexMapping[pos], precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return isNullAt(pos) ? null : super.getBinary(indexMapping[pos]);
    }

    @Override
    public InternalArray getArray(int pos) {
        return isNullAt(pos) ? null : super.getArray(indexMapping[pos]);
    }

    @Override
    public InternalMap getMap(int pos) {
        return isNullAt(pos) ? null : super.getMap(indexMapping[pos]);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return isNullAt(pos) ? null : super.getRow(pos, numFields);
    }
}
