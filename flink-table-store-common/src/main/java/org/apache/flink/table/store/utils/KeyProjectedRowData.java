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

package org.apache.flink.table.store.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

/** A {@link RowData} to project key fields with {@link RowKind#INSERT}. */
public class KeyProjectedRowData implements RowData {

    private final int[] indexMapping;

    private RowData row;

    public KeyProjectedRowData(int[] indexMapping) {
        this.indexMapping = indexMapping;
    }

    public KeyProjectedRowData replaceRow(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public int getArity() {
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
    public StringData getString(int pos) {
        return row.getString(indexMapping[pos]);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(indexMapping[pos], precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row.getTimestamp(indexMapping[pos], precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row.getRawValue(indexMapping[pos]);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(indexMapping[pos]);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row.getArray(indexMapping[pos]);
    }

    @Override
    public MapData getMap(int pos) {
        return row.getMap(indexMapping[pos]);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
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
