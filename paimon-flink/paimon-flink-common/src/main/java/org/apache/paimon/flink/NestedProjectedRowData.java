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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * A {@link RowData} that provides a mapping view of the original {@link RowData} according to
 * projection information. Compared with {@link ProjectedRowData}, this class supports nested
 * projection.
 */
public class NestedProjectedRowData implements RowData, Serializable {
    private static final long serialVersionUID = 1L;

    private final RowType producedDataType;
    private final int[][] projectedFields;
    private final int[] lastProjectedFields;

    private final Object[] cachedFields;
    private final boolean[] isFieldsCached;

    private final boolean[] cachedNullAt;
    private final boolean[] isNullAtCached;

    private transient RowData row;

    NestedProjectedRowData(RowType producedDataType, int[][] projectedFields) {
        this.producedDataType = producedDataType;
        this.projectedFields = projectedFields;
        this.lastProjectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            this.lastProjectedFields[i] = projectedFields[i][projectedFields[i].length - 1];
        }

        this.cachedFields = new Object[projectedFields.length];
        this.isFieldsCached = new boolean[projectedFields.length];
        this.cachedNullAt = new boolean[projectedFields.length];
        this.isNullAtCached = new boolean[projectedFields.length];
    }

    public NestedProjectedRowData replaceRow(RowData row) {
        this.row = row;
        Arrays.fill(isFieldsCached, false);
        Arrays.fill(isNullAtCached, false);
        return this;
    }

    public static @Nullable NestedProjectedRowData copy(@Nullable NestedProjectedRowData rowData) {
        if (rowData == null) {
            return null;
        }
        return new NestedProjectedRowData(rowData.producedDataType, rowData.projectedFields);
    }

    @Override
    public int getArity() {
        return projectedFields.length;
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        row.setRowKind(rowKind);
    }

    @Override
    public boolean isNullAt(int pos) {
        if (isNullAtCached[pos]) {
            return cachedNullAt[pos];
        }

        RowData rowData = extractInternalRow(pos);
        boolean result;
        if (rowData == null) {
            result = true;
        } else {
            result = rowData.isNullAt(lastProjectedFields[pos]);
        }

        isNullAtCached[pos] = true;
        cachedNullAt[pos] = result;

        return result;
    }

    @Override
    public boolean getBoolean(int pos) {
        return getFieldAs(pos, RowData::getBoolean);
    }

    @Override
    public byte getByte(int pos) {
        return getFieldAs(pos, RowData::getByte);
    }

    @Override
    public short getShort(int pos) {
        return getFieldAs(pos, RowData::getShort);
    }

    @Override
    public int getInt(int pos) {
        return getFieldAs(pos, RowData::getInt);
    }

    @Override
    public long getLong(int pos) {
        return getFieldAs(pos, RowData::getLong);
    }

    @Override
    public float getFloat(int pos) {
        return getFieldAs(pos, RowData::getFloat);
    }

    @Override
    public double getDouble(int pos) {
        return getFieldAs(pos, RowData::getDouble);
    }

    @Override
    public StringData getString(int pos) {
        return getFieldAs(pos, RowData::getString);
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return getFieldAs(
                pos, (rowData, internalPos) -> rowData.getDecimal(internalPos, precision, scale));
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return getFieldAs(
                pos, (rowData, internalPos) -> rowData.getTimestamp(internalPos, precision));
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        return getFieldAs(pos, RowData::getRawValue);
    }

    @Override
    public byte[] getBinary(int pos) {
        return getFieldAs(pos, RowData::getBinary);
    }

    @Override
    public ArrayData getArray(int pos) {
        return getFieldAs(pos, RowData::getArray);
    }

    @Override
    public MapData getMap(int pos) {
        return getFieldAs(pos, RowData::getMap);
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return getFieldAs(pos, (rowData, internalPos) -> rowData.getRow(internalPos, numFields));
    }

    private @Nullable RowData extractInternalRow(int pos) {
        int[] projectedField = projectedFields[pos];
        RowData rowData = this.row;
        RowType dataType = producedDataType;
        for (int i = 0; i < projectedField.length - 1; i++) {
            dataType = (RowType) dataType.getTypeAt(projectedField[i]);
            if (rowData.isNullAt(projectedField[i])) {
                return null;
            }
            rowData = rowData.getRow(projectedField[i], dataType.getFieldCount());
        }
        return rowData;
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldAs(int pos, BiFunction<RowData, Integer, T> getter) {
        if (isFieldsCached[pos]) {
            return (T) cachedFields[pos];
        }

        RowData rowData = extractInternalRow(pos);
        T result;
        if (rowData == null) {
            isNullAtCached[pos] = true;
            cachedNullAt[pos] = true;
            isFieldsCached[pos] = true;
            cachedFields[pos] = null;
            result = null;
        } else {
            result = getter.apply(rowData, lastProjectedFields[pos]);
            isNullAtCached[pos] = true;
            cachedNullAt[pos] = result == null;
            isFieldsCached[pos] = true;
            cachedFields[pos] = result;
        }

        return result;
    }

    @VisibleForTesting
    public int[][] getProjectedFields() {
        return projectedFields;
    }

    @VisibleForTesting
    public RowType getRowType() {
        return producedDataType;
    }
}
