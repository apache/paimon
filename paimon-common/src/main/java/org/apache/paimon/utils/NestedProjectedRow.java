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
 * handles projection through ARRAY and MAP types whose elements contain ROW fields.
 */
public class NestedProjectedRow implements InternalRow {

    private final int[] indexMapping;
    @Nullable private final NestedProjectedRow[] nestedProjections;
    @Nullable private final int[] nestedArity;
    @Nullable private final NestedProjectedRow[] arrayElementProjections;
    @Nullable private final int[] arrayElementArity;
    @Nullable private final NestedProjectedRow[] mapKeyProjections;
    @Nullable private final int[] mapKeyArity;
    @Nullable private final NestedProjectedRow[] mapValueProjections;
    @Nullable private final int[] mapValueArity;
    private InternalRow row;

    private NestedProjectedRow(
            int[] indexMapping,
            @Nullable NestedProjectedRow[] nestedProjections,
            @Nullable int[] nestedArity,
            @Nullable NestedProjectedRow[] arrayElementProjections,
            @Nullable int[] arrayElementArity,
            @Nullable NestedProjectedRow[] mapKeyProjections,
            @Nullable int[] mapKeyArity,
            @Nullable NestedProjectedRow[] mapValueProjections,
            @Nullable int[] mapValueArity) {
        this.indexMapping = indexMapping;
        this.nestedProjections = nestedProjections;
        this.nestedArity = nestedArity;
        this.arrayElementProjections = arrayElementProjections;
        this.arrayElementArity = arrayElementArity;
        this.mapKeyProjections = mapKeyProjections;
        this.mapKeyArity = mapKeyArity;
        this.mapValueProjections = mapValueProjections;
        this.mapValueArity = mapValueArity;
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
        NestedProjectedRow[] arrayElementProjections = null;
        int[] arrayElementArity = null;
        NestedProjectedRow[] mapKeyProjections = null;
        int[] mapKeyArity = null;
        NestedProjectedRow[] mapValueProjections = null;
        int[] mapValueArity = null;

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
            } else if (typeRoot == DataTypeRoot.ARRAY) {
                DataType projectedElement = ((ArrayType) projected.type()).getElementType();
                DataType dataElement = ((ArrayType) dataField.type()).getElementType();
                if (projectedElement.getTypeRoot() == DataTypeRoot.ROW) {
                    RowType dataElementRow = (RowType) dataElement;
                    RowType projectedElementRow = (RowType) projectedElement;
                    NestedProjectedRow sub = create(dataElementRow, projectedElementRow);
                    if (sub != null) {
                        if (arrayElementProjections == null) {
                            arrayElementProjections = new NestedProjectedRow[projectedSize];
                            arrayElementArity = new int[projectedSize];
                        }
                        arrayElementProjections[i] = sub;
                        arrayElementArity[i] = dataElementRow.getFieldCount();
                    }
                }
            } else if (typeRoot == DataTypeRoot.MAP || typeRoot == DataTypeRoot.MULTISET) {
                MapType projectedMapType = (MapType) projected.type();
                MapType dataMapType = (MapType) dataField.type();
                DataType projectedKey = projectedMapType.getKeyType();
                DataType dataKey = dataMapType.getKeyType();
                if (projectedKey.getTypeRoot() == DataTypeRoot.ROW) {
                    RowType dataKeyRow = (RowType) dataKey;
                    RowType projectedKeyRow = (RowType) projectedKey;
                    NestedProjectedRow sub = create(dataKeyRow, projectedKeyRow);
                    if (sub != null) {
                        if (mapKeyProjections == null) {
                            mapKeyProjections = new NestedProjectedRow[projectedSize];
                            mapKeyArity = new int[projectedSize];
                        }
                        mapKeyProjections[i] = sub;
                        mapKeyArity[i] = dataKeyRow.getFieldCount();
                    }
                }
                DataType projectedValue = projectedMapType.getValueType();
                DataType dataValue = dataMapType.getValueType();
                if (projectedValue.getTypeRoot() == DataTypeRoot.ROW) {
                    RowType dataValueRow = (RowType) dataValue;
                    RowType projectedValueRow = (RowType) projectedValue;
                    NestedProjectedRow sub = create(dataValueRow, projectedValueRow);
                    if (sub != null) {
                        if (mapValueProjections == null) {
                            mapValueProjections = new NestedProjectedRow[projectedSize];
                            mapValueArity = new int[projectedSize];
                        }
                        mapValueProjections[i] = sub;
                        mapValueArity[i] = dataValueRow.getFieldCount();
                    }
                }
            }
        }

        return new NestedProjectedRow(
                indexMapping,
                nestedProjections,
                nestedArity,
                arrayElementProjections,
                arrayElementArity,
                mapKeyProjections,
                mapKeyArity,
                mapValueProjections,
                mapValueArity);
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
        if (arrayElementProjections != null && arrayElementProjections[pos] != null) {
            return new ProjectedInternalArray(
                    array, arrayElementProjections[pos], arrayElementArity[pos]);
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
        if ((mapKeyProjections != null && mapKeyProjections[pos] != null)
                || (mapValueProjections != null && mapValueProjections[pos] != null)) {
            NestedProjectedRow keyProj = mapKeyProjections != null ? mapKeyProjections[pos] : null;
            int keyAr = mapKeyArity != null ? mapKeyArity[pos] : 0;
            NestedProjectedRow valueProj =
                    mapValueProjections != null ? mapValueProjections[pos] : null;
            int valueAr = mapValueArity != null ? mapValueArity[pos] : 0;
            return new ProjectedInternalMap(map, keyProj, keyAr, valueProj, valueAr);
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

    // ======================== ProjectedInternalArray ========================

    private static class ProjectedInternalArray implements InternalArray {

        private final InternalArray array;
        private final NestedProjectedRow elementProjection;
        private final int elementArity;

        ProjectedInternalArray(
                InternalArray array, NestedProjectedRow elementProjection, int elementArity) {
            this.array = array;
            this.elementProjection = elementProjection;
            this.elementArity = elementArity;
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
            InternalRow inner = array.getRow(pos, elementArity);
            return elementProjection.replaceRow(inner);
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
        public InternalArray getArray(int pos) {
            return array.getArray(pos);
        }

        @Override
        public InternalVector getVector(int pos) {
            return array.getVector(pos);
        }

        @Override
        public InternalMap getMap(int pos) {
            return array.getMap(pos);
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

        private final InternalMap map;
        @Nullable private final NestedProjectedRow keyProjection;
        private final int keyArity;
        @Nullable private final NestedProjectedRow valueProjection;
        private final int valueArity;

        ProjectedInternalMap(
                InternalMap map,
                @Nullable NestedProjectedRow keyProjection,
                int keyArity,
                @Nullable NestedProjectedRow valueProjection,
                int valueArity) {
            this.map = map;
            this.keyProjection = keyProjection;
            this.keyArity = keyArity;
            this.valueProjection = valueProjection;
            this.valueArity = valueArity;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public InternalArray keyArray() {
            InternalArray keys = map.keyArray();
            if (keyProjection != null) {
                return new ProjectedInternalArray(keys, keyProjection, keyArity);
            }
            return keys;
        }

        @Override
        public InternalArray valueArray() {
            InternalArray values = map.valueArray();
            if (valueProjection != null) {
                return new ProjectedInternalArray(values, valueProjection, valueArity);
            }
            return values;
        }
    }
}
