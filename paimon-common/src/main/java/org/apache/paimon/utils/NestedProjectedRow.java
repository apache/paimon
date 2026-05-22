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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A projected view of {@link InternalRow} that supports nested ROW field pruning.
 *
 * <p>Unlike {@link ProjectedRow} which only handles top-level projection, this class recursively
 * projects nested ROW fields. It maps each projected field to the corresponding position in the
 * data schema by name, and for ROW-typed fields, recursively applies sub-projections.
 */
public class NestedProjectedRow implements InternalRow {

    private final int[] indexMapping;
    @Nullable private final NestedProjectedRow[] nestedProjections;
    @Nullable private final int[] nestedArity;
    private InternalRow row;

    private NestedProjectedRow(
            int[] indexMapping,
            @Nullable NestedProjectedRow[] nestedProjections,
            @Nullable int[] nestedArity) {
        this.indexMapping = indexMapping;
        this.nestedProjections = nestedProjections;
        this.nestedArity = nestedArity;
    }

    public NestedProjectedRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    /**
     * Creates a {@link NestedProjectedRow} from the data schema and projected schema using field
     * IDs to match fields. Returns null if the two schemas are identical (no projection needed).
     *
     * @param dataSchema the full schema of the underlying row data
     * @param projectedSchema the projected schema to read
     */
    @Nullable
    public static NestedProjectedRow create(RowType dataSchema, RowType projectedSchema) {
        if (dataSchema.equals(projectedSchema)) {
            return null;
        }

        List<DataField> dataFields = dataSchema.getFields();
        List<DataField> projectedFields = projectedSchema.getFields();

        int projectedSize = projectedFields.size();
        int[] indexMapping = new int[projectedSize];
        NestedProjectedRow[] nestedProjections = null;
        int[] nestedArity = null;
        boolean hasNested = false;

        for (int i = 0; i < projectedSize; i++) {
            DataField projected = projectedFields.get(i);
            int dataIdx = dataSchema.getFieldIndexByFieldId(projected.id());
            DataField dataField = dataFields.get(dataIdx);
            Preconditions.checkArgument(
                    dataField.name().equals(projected.name()),
                    "Field name mismatch for field id %s: data schema has '%s' but projected schema has '%s'",
                    projected.id(),
                    dataField.name(),
                    projected.name());
            indexMapping[i] = dataIdx;

            if (projected.type().getTypeRoot() == DataTypeRoot.ROW) {
                RowType dataNestedType = (RowType) dataFields.get(dataIdx).type();
                RowType projectedNestedType = (RowType) projected.type();
                NestedProjectedRow sub = create(dataNestedType, projectedNestedType);
                if (sub != null) {
                    if (nestedProjections == null) {
                        nestedProjections = new NestedProjectedRow[projectedSize];
                        nestedArity = new int[projectedSize];
                    }
                    nestedProjections[i] = sub;
                    nestedArity[i] = dataNestedType.getFieldCount();
                    hasNested = true;
                }
            }
        }

        if (!hasNested) {
            return new NestedProjectedRow(indexMapping, null, null);
        }
        return new NestedProjectedRow(indexMapping, nestedProjections, nestedArity);
    }

    @Override
    public int getFieldCount() {
        return indexMapping.length;
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
    public Blob getBlob(int pos) {
        return row.getBlob(indexMapping[pos]);
    }

    @Override
    public InternalArray getArray(int pos) {
        return row.getArray(indexMapping[pos]);
    }

    @Override
    public InternalVector getVector(int pos) {
        return row.getVector(indexMapping[pos]);
    }

    @Override
    public InternalMap getMap(int pos) {
        return row.getMap(indexMapping[pos]);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (nestedProjections != null && nestedProjections[pos] != null) {
            InternalRow inner = row.getRow(indexMapping[pos], nestedArity[pos]);
            return nestedProjections[pos].replaceRow(inner);
        }
        return row.getRow(indexMapping[pos], numFields);
    }
}
