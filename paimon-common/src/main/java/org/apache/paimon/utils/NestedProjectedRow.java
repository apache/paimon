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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A projected view of {@link InternalRow} that supports nested ROW field pruning.
 *
 * <p>Unlike {@link ProjectedRow} which only handles top-level projection, this class recursively
 * projects nested ROW fields. It maps each projected field to the corresponding position in the
 * data schema by field ID, and for ROW-typed fields, recursively applies sub-projections. It also
 * handles projection through ARRAY, MAP, and MULTISET types at arbitrary nesting depth.
 */
public class NestedProjectedRow implements InternalRow {

    private final int[] indexMapping;
    @Nullable private final NestedProjectedRow[] nestedProjections;
    @Nullable private final int[] nestedArity;
    @Nullable private final ElementProjection[] elementProjections;
    private InternalRow row;

    private NestedProjectedRow(
            int[] indexMapping,
            @Nullable NestedProjectedRow[] nestedProjections,
            @Nullable int[] nestedArity,
            @Nullable ElementProjection[] elementProjections) {
        this.indexMapping = indexMapping;
        this.nestedProjections = nestedProjections;
        this.nestedArity = nestedArity;
        this.elementProjections = elementProjections;
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
        ElementProjection[] elementProjections = null;

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

            DataTypeRoot typeRoot = projected.type().getTypeRoot();
            if (typeRoot == DataTypeRoot.ROW) {
                RowType dataNestedType = (RowType) dataField.type();
                RowType projectedNestedType = (RowType) projected.type();
                NestedProjectedRow sub = create(dataNestedType, projectedNestedType);
                if (sub != null) {
                    if (nestedProjections == null) {
                        nestedProjections = new NestedProjectedRow[projectedSize];
                        nestedArity = new int[projectedSize];
                    }
                    nestedProjections[i] = sub;
                    nestedArity[i] = dataNestedType.getFieldCount();
                }
            } else {
                ElementProjection ep = createElementProjection(dataField.type(), projected.type());
                if (ep != null) {
                    if (elementProjections == null) {
                        elementProjections = new ElementProjection[projectedSize];
                    }
                    elementProjections[i] = ep;
                }
            }
        }

        return new NestedProjectedRow(
                indexMapping, nestedProjections, nestedArity, elementProjections);
    }

    @Nullable
    private static ElementProjection createElementProjection(
            DataType dataType, DataType projectedType) {
        if (dataType.equals(projectedType)) {
            return null;
        }
        DataTypeRoot typeRoot = projectedType.getTypeRoot();
        switch (typeRoot) {
            case ARRAY:
                DataType dataElement = ((ArrayType) dataType).getElementType();
                DataType projectedElement = ((ArrayType) projectedType).getElementType();
                return createCollectionElementProjection(dataElement, projectedElement);
            case MAP:
                return createMapProjection(
                        ((MapType) dataType).getKeyType(),
                        ((MapType) projectedType).getKeyType(),
                        ((MapType) dataType).getValueType(),
                        ((MapType) projectedType).getValueType());
            case MULTISET:
                DataType dataMultisetElement = ((MultisetType) dataType).getElementType();
                DataType projectedMultisetElement = ((MultisetType) projectedType).getElementType();
                return createMapProjection(
                        dataMultisetElement, projectedMultisetElement, null, null);
            default:
                return null;
        }
    }

    @Nullable
    private static ElementProjection createMapProjection(
            DataType dataKey,
            DataType projectedKey,
            @Nullable DataType dataValue,
            @Nullable DataType projectedValue) {
        ElementProjection keyProj = null;
        ElementProjection valueProj = null;

        if (dataKey != null && projectedKey != null && !dataKey.equals(projectedKey)) {
            keyProj = createCollectionElementProjection(dataKey, projectedKey);
        }

        if (dataValue != null && projectedValue != null && !dataValue.equals(projectedValue)) {
            valueProj = createCollectionElementProjection(dataValue, projectedValue);
        }

        if (keyProj == null && valueProj == null) {
            return null;
        }
        return new ElementProjection(null, 0, keyProj, valueProj);
    }

    @Nullable
    private static ElementProjection createCollectionElementProjection(
            DataType dataType, DataType projectedType) {
        if (dataType.equals(projectedType)) {
            return null;
        }
        if (projectedType.getTypeRoot() == DataTypeRoot.ROW) {
            RowType dataRow = (RowType) dataType;
            RowType projRow = (RowType) projectedType;
            NestedProjectedRow sub = create(dataRow, projRow);
            if (sub != null) {
                return new ElementProjection(sub, dataRow.getFieldCount(), null, null);
            }
            return null;
        }
        // Element is a collection type (ARRAY, MAP, MULTISET) — wrap one level deeper
        ElementProjection inner = createElementProjection(dataType, projectedType);
        if (inner != null) {
            return new ElementProjection(null, 0, inner, null);
        }
        return null;
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
        InternalArray array = row.getArray(indexMapping[pos]);
        if (elementProjections != null && elementProjections[pos] != null) {
            return elementProjections[pos].projectArray(array);
        }
        return array;
    }

    @Override
    public InternalVector getVector(int pos) {
        return row.getVector(indexMapping[pos]);
    }

    @Override
    public InternalMap getMap(int pos) {
        InternalMap map = row.getMap(indexMapping[pos]);
        if (elementProjections != null && elementProjections[pos] != null) {
            return elementProjections[pos].projectMap(map);
        }
        return map;
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (nestedProjections != null && nestedProjections[pos] != null) {
            InternalRow inner = row.getRow(indexMapping[pos], nestedArity[pos]);
            return nestedProjections[pos].replaceRow(inner);
        }
        return row.getRow(indexMapping[pos], numFields);
    }

    // ======================== ElementProjection ========================

    /**
     * Describes how to project elements within a collection type (ARRAY, MAP, MULTISET).
     * Recursively handles nested collections.
     */
    static class ElementProjection {
        @Nullable final NestedProjectedRow rowProjection;
        final int rowArity;
        @Nullable final ElementProjection keyOrElementProjection;
        @Nullable final ElementProjection valueProjection;

        ElementProjection(
                @Nullable NestedProjectedRow rowProjection,
                int rowArity,
                @Nullable ElementProjection keyOrElementProjection,
                @Nullable ElementProjection valueProjection) {
            this.rowProjection = rowProjection;
            this.rowArity = rowArity;
            this.keyOrElementProjection = keyOrElementProjection;
            this.valueProjection = valueProjection;
        }

        InternalArray projectArray(InternalArray array) {
            return new ProjectedInternalArray(array, this);
        }

        InternalMap projectMap(InternalMap map) {
            InternalArray keys = map.keyArray();
            InternalArray values = map.valueArray();
            InternalArray projectedKeys =
                    keyOrElementProjection != null
                            ? keyOrElementProjection.projectArray(keys)
                            : keys;
            InternalArray projectedValues =
                    valueProjection != null ? valueProjection.projectArray(values) : values;
            if (projectedKeys == keys && projectedValues == values) {
                return map;
            }
            return new ProjectedInternalMap(map.size(), projectedKeys, projectedValues);
        }
    }

    // ======================== ProjectedInternalArray ========================

    private static class ProjectedInternalArray implements InternalArray {

        private final InternalArray array;
        private final ElementProjection projection;

        ProjectedInternalArray(InternalArray array, ElementProjection projection) {
            this.array = array;
            this.projection = projection;
        }

        @Override
        public int size() {
            return array.size();
        }

        @Override
        public boolean isNullAt(int pos) {
            return array.isNullAt(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            if (projection.rowProjection != null) {
                InternalRow inner = array.getRow(pos, projection.rowArity);
                return projection.rowProjection.replaceRow(inner);
            }
            return array.getRow(pos, numFields);
        }

        @Override
        public InternalArray getArray(int pos) {
            InternalArray inner = array.getArray(pos);
            if (projection.keyOrElementProjection != null) {
                return projection.keyOrElementProjection.projectArray(inner);
            }
            return inner;
        }

        @Override
        public InternalMap getMap(int pos) {
            InternalMap inner = array.getMap(pos);
            if (projection.keyOrElementProjection != null) {
                return projection.keyOrElementProjection.projectMap(inner);
            }
            return inner;
        }

        @Override
        public boolean getBoolean(int pos) {
            return array.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return array.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return array.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return array.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return array.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return array.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return array.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return array.getString(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return array.getDecimal(pos, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return array.getTimestamp(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return array.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            return array.getVariant(pos);
        }

        @Override
        public Blob getBlob(int pos) {
            return array.getBlob(pos);
        }

        @Override
        public InternalVector getVector(int pos) {
            return array.getVector(pos);
        }

        @Override
        public boolean[] toBooleanArray() {
            return array.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return array.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return array.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return array.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return array.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return array.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return array.toDoubleArray();
        }
    }

    // ======================== ProjectedInternalMap ========================

    private static class ProjectedInternalMap implements InternalMap {

        private final int size;
        private final InternalArray keyArray;
        private final InternalArray valueArray;

        ProjectedInternalMap(int size, InternalArray keyArray, InternalArray valueArray) {
            this.size = size;
            this.keyArray = keyArray;
            this.valueArray = valueArray;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public InternalArray keyArray() {
            return keyArray;
        }

        @Override
        public InternalArray valueArray() {
            return valueArray;
        }
    }
}
