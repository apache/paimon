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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A {@link RowData} that provides a mapping view of the original {@link RowData} according to
 * projection information.
 */
public class ProjectionRowData implements RowData, Serializable {
    private static final long serialVersionUID = 1L;

    private final RowType producedDataType;
    private final int[][] projectedFields;
    private final FieldGetter[][] fieldGetters;

    private transient GenericRowData row;

    ProjectionRowData(RowType producedDataType, int[][] projectedFields) {
        this.producedDataType = producedDataType;
        this.projectedFields = projectedFields;
        this.fieldGetters = new FieldGetter[projectedFields.length][];
        for (int i = 0; i < projectedFields.length; i++) {
            this.fieldGetters[i] = new FieldGetter[projectedFields[i].length];
            LogicalType currentType = producedDataType;
            for (int j = 0; j < projectedFields[i].length; j++) {
                currentType = ((RowType) currentType).getTypeAt(projectedFields[i][j]);
                this.fieldGetters[i][j] =
                        RowData.createFieldGetter(currentType, projectedFields[i][j]);
            }
        }
    }

    public ProjectionRowData replaceRow(RowData inputRow) {
        if (this.row == null) {
            this.row = new GenericRowData(inputRow.getRowKind(), fieldGetters.length);
        }

        for (int i = 0; i < fieldGetters.length; i++) {
            Object currentRow = inputRow;
            for (int j = 0; j < fieldGetters[i].length; j++) {
                if (currentRow == null) {
                    break;
                }
                currentRow = this.fieldGetters[i][j].getFieldOrNull((RowData) currentRow);
            }
            this.row.setField(i, currentRow);
        }

        if (inputRow != null) {
            this.row.setRowKind(inputRow.getRowKind());
        }
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
        return this.row.getArity();
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
        return this.row.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return this.row.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return this.row.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return this.row.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return this.row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return this.row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return this.row.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return this.row.getDouble(i);
    }

    @Override
    public StringData getString(int i) {
        return this.row.getString(i);
    }

    @Override
    public DecimalData getDecimal(int i, int i1, int i2) {
        return this.row.getDecimal(i, i1, i2);
    }

    @Override
    public TimestampData getTimestamp(int i, int i1) {
        return this.row.getTimestamp(i, i1);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return this.row.getRawValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return this.row.getBinary(i);
    }

    @Override
    public ArrayData getArray(int i) {
        return this.row.getArray(i);
    }

    @Override
    public MapData getMap(int i) {
        return this.row.getMap(i);
    }

    @Override
    public RowData getRow(int i, int i1) {
        return this.row.getRow(i, i1);
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
