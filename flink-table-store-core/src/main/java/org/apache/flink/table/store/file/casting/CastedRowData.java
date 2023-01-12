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

package org.apache.flink.table.store.file.casting;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/**
 * An implementation of {@link RowData} which provides a casted view of the underlying {@link
 * RowData}.
 *
 * <p>It reads data from underlying {@link RowData} according to source logical type and casts it
 * with specific {@link CastExecutor}.
 *
 * <p>Note: This class supports only top-level castings, not nested castings.
 */
public class CastedRowData implements RowData {

    private final FieldGetterCastExecutor[] castMapping;

    private RowData row;

    protected CastedRowData(FieldGetterCastExecutor[] castMapping) {
        this.castMapping = castMapping;
    }

    /**
     * Replaces the underlying {@link RowData} backing this {@link CastedRowData}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public CastedRowData replaceRow(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public int getArity() {
        return row.getArity();
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
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return castMapping == null ? row.getBoolean(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public byte getByte(int pos) {
        return castMapping == null ? row.getByte(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public short getShort(int pos) {
        return castMapping == null ? row.getShort(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public int getInt(int pos) {
        return castMapping == null ? row.getInt(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public long getLong(int pos) {
        return castMapping == null ? row.getLong(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public float getFloat(int pos) {
        return castMapping == null ? row.getFloat(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public double getDouble(int pos) {
        return castMapping == null ? row.getDouble(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public StringData getString(int pos) {
        return castMapping == null ? row.getString(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return castMapping == null
                ? row.getDecimal(pos, precision, scale)
                : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return castMapping == null
                ? row.getTimestamp(pos, precision)
                : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return castMapping == null ? row.getRawValue(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public byte[] getBinary(int pos) {
        return castMapping == null ? row.getBinary(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public ArrayData getArray(int pos) {
        return castMapping == null ? row.getArray(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public MapData getMap(int pos) {
        return castMapping == null ? row.getMap(pos) : castMapping[pos].getFieldOrNull(row);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return castMapping == null
                ? row.getRow(pos, numFields)
                : castMapping[pos].getFieldOrNull(row);
    }

    /**
     * Create an empty {@link CastedRowData} starting from a {@code casting} array.
     *
     * @see FieldGetterCastExecutor
     * @see CastedRowData
     */
    public static CastedRowData from(FieldGetterCastExecutor[] castMapping) {
        return new CastedRowData(castMapping);
    }
}
