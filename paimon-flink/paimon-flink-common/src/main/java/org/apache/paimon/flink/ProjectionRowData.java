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

import org.apache.paimon.types.RowType;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A {@link RowData} that provides a mapping view of the original {@link RowData} according to
 * projection information.
 */
public class ProjectionRowData implements RowData, Serializable {
    private final RowType producedDataType;
    private final int[][] projectedFields;
    private final int[] lastProjectedFields;
    private transient RowData row;

    ProjectionRowData(RowType producedDataType, int[][] projectedFields) {
        this.producedDataType = producedDataType;
        this.projectedFields = projectedFields;
        this.lastProjectedFields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            this.lastProjectedFields[i] = projectedFields[i][projectedFields[i].length - 1];
        }
    }

    public ProjectionRowData replaceRow(RowData row) {
        this.row = row;
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
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return true;
        }
        return rowData.isNullAt(lastProjectedFields[i]);
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

    @Override
    public boolean getBoolean(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getBoolean(lastProjectedFields[i]);
    }

    @Override
    public byte getByte(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getByte(lastProjectedFields[i]);
    }

    @Override
    public short getShort(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getShort(lastProjectedFields[i]);
    }

    @Override
    public int getInt(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getInt(lastProjectedFields[i]);
    }

    @Override
    public long getLong(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getLong(lastProjectedFields[i]);
    }

    @Override
    public float getFloat(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getFloat(lastProjectedFields[i]);
    }

    @Override
    public double getDouble(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            throw new NullPointerException();
        }
        return rowData.getDouble(lastProjectedFields[i]);
    }

    @Override
    public StringData getString(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getString(lastProjectedFields[i]);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getDecimal(lastProjectedFields[i], i1, i2);
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getTimestamp(lastProjectedFields[i], i1);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getRawValue(lastProjectedFields[i]);
    }

    @Override
    public byte[] getBinary(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getBinary(lastProjectedFields[i]);
    }

    @Override
    public ArrayData getArray(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getArray(lastProjectedFields[i]);
    }

    @Override
    public MapData getMap(int i) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getMap(lastProjectedFields[i]);
    }

    @Override
    public RowData getRow(int i, int i1) {
        RowData rowData = extractInternalRow(i);
        if (rowData == null) {
            return null;
        }
        return rowData.getRow(lastProjectedFields[i], i1);
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
