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
 * projection information.
 */
public class ProjectionRowData implements RowData, Serializable {
    private static final long serialVersionUID = 1L;

    private final RowType producedDataType;
    private final int[][] projectedFields;
    private final int[] lastProjectedFields;

    private final Object[] cachedFields;
    private final boolean[] isFieldsCached;

    private final boolean[] cachedNullAt;
    private final boolean[] isNullAtCached;

    private transient RowData row;

    ProjectionRowData(RowType producedDataType, int[][] projectedFields) {
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

    public ProjectionRowData replaceRow(RowData row) {
        this.row = row;
        Arrays.fill(isFieldsCached, false);
        Arrays.fill(isNullAtCached, false);
        return this;
    }

    public static @Nullable ProjectionRowData copy(@Nullable ProjectionRowData rowData) {
        if (rowData == null) {
            return null;
        }
        return new ProjectionRowData(rowData.producedDataType, rowData.projectedFields);
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
    public boolean isNullAt(int i) {
        if (isNullAtCached[i]) {
            return cachedNullAt[i];
        }

        RowData rowData = extractInternalRow(i);
        boolean result;
        if (rowData == null) {
            result = true;
        } else {
            result = rowData.isNullAt(lastProjectedFields[i]);
        }

        isNullAtCached[i] = true;
        cachedNullAt[i] = result;

        return result;
    }

    @Override
    public boolean getBoolean(int i) {
        return getFieldAs(i, RowData::getBoolean);
    }

    @Override
    public byte getByte(int i) {
        return getFieldAs(i, RowData::getByte);
    }

    @Override
    public short getShort(int i) {
        return getFieldAs(i, RowData::getShort);
    }

    @Override
    public int getInt(int i) {
        return getFieldAs(i, RowData::getInt);
    }

    @Override
    public long getLong(int i) {
        return getFieldAs(i, RowData::getLong);
    }

    @Override
    public float getFloat(int i) {
        return getFieldAs(i, RowData::getFloat);
    }

    @Override
    public double getDouble(int i) {
        return getFieldAs(i, RowData::getDouble);
    }

    @Override
    public StringData getString(int i) {
        return getFieldAs(i, RowData::getString);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        return getFieldAs(i, (rowData, j) -> rowData.getDecimal(j, i1, i2));
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        return getFieldAs(i, (rowData, j) -> rowData.getTimestamp(j, i1));
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return getFieldAs(i, RowData::getRawValue);
    }

    @Override
    public byte[] getBinary(int i) {
        return getFieldAs(i, RowData::getBinary);
    }

    @Override
    public ArrayData getArray(int i) {
        return getFieldAs(i, RowData::getArray);
    }

    @Override
    public MapData getMap(int i) {
        return getFieldAs(i, RowData::getMap);
    }

    @Override
    public RowData getRow(int i, int i1) {
        return getFieldAs(i, (rowData, j) -> rowData.getRow(j, i1));
    }

    private @Nullable RowData extractInternalRow(int i) {
        int[] projectedField = projectedFields[i];
        RowData rowData = this.row;
        RowType dataType = producedDataType;
        for (int j = 0; j < projectedField.length - 1; j++) {
            dataType = (RowType) dataType.getTypeAt(projectedField[j]);
            if (rowData.isNullAt(projectedField[j])) {
                return null;
            }
            rowData = rowData.getRow(projectedField[j], dataType.getFieldCount());
        }
        return rowData;
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldAs(int i, BiFunction<RowData, Integer, T> getter) {
        if (isFieldsCached[i]) {
            return (T) cachedFields[i];
        }

        RowData rowData = extractInternalRow(i);
        T result;
        if (rowData == null) {
            isNullAtCached[i] = true;
            cachedNullAt[i] = true;
            isFieldsCached[i] = true;
            cachedFields[i] = null;
            result = null;
        } else {
            result = getter.apply(rowData, lastProjectedFields[i]);
            isNullAtCached[i] = true;
            cachedNullAt[i] = result == null;
            isFieldsCached[i] = true;
            cachedFields[i] = result;
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
