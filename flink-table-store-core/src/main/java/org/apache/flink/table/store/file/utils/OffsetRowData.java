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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/** A {@link RowData} to wrap row with offset. */
public class OffsetRowData implements RowData {

    private final int arity;

    private final int offset;

    private RowData row;

    public OffsetRowData(int arity, int offset) {
        this.arity = arity;
        this.offset = offset;
    }

    public OffsetRowData replace(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public int getArity() {
        return arity;
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(offset + pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(offset + pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(offset + pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(offset + pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(offset + pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(offset + pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(offset + pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(offset + pos);
    }

    @Override
    public StringData getString(int pos) {
        return row.getString(offset + pos);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return row.getDecimal(offset + pos, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return row.getTimestamp(offset + pos, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return row.getRawValue(offset + pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(offset + pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        return row.getArray(offset + pos);
    }

    @Override
    public MapData getMap(int pos) {
        return row.getMap(offset + pos);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return row.getRow(offset + pos, numFields);
    }
}
