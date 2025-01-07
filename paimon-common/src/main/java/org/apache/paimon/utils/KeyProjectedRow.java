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
import org.apache.paimon.types.RowKind;

import java.util.Arrays;

/** A {@link InternalRow} to project key fields with {@link RowKind#INSERT}. */
public class KeyProjectedRow implements InternalRow {

    private final int[] indexMapping;

    private InternalRow row;

    public KeyProjectedRow(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    public KeyProjectedRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    public int[] indexMapping() {
        return indexMapping;
    }

    @Override
    public int getFieldCount() {
        return indexMapping.length;
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind kind) {
        throw new UnsupportedOperationException("Key row data should always be insert only.");
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(indexMapping[pos]);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(indexMapping[pos]);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(indexMapping[pos]);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(indexMapping[pos]);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(indexMapping[pos]);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(indexMapping[pos]);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(indexMapping[pos]);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(indexMapping[pos]);
    }

    @Override
    public BinaryString getString(int pos) {
        return row.getString(indexMapping[pos]);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(indexMapping[pos], precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return row.getTimestamp(indexMapping[pos], precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(indexMapping[pos]);
    }

    @Override
    public Variant getVariant(int pos) {
        return row.getVariant(indexMapping[pos]);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(indexMapping[pos]);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(indexMapping[pos]);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return row.getRow(indexMapping[pos], numFields);
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
        return getRowKind().shortString()
                + "{"
                + "indexMapping="
                + Arrays.toString(indexMapping)
                + ", mutableRow="
                + row
                + '}';
    }
}
