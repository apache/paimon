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

package org.apache.paimon.data.shredding;

import org.apache.paimon.CoreOptions.MapSharedShreddingColumnPlacementPolicy;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Converts logical rows containing shared-shredding MAP fields into physical rows. */
public class MapSharedShreddingRowConverter {

    private final RowType logicalType;
    private final RowType physicalType;
    private final Map<String, ColumnContext> contextByFieldName;
    private final ColumnContext[] contextByFieldPos;
    private final List<String> shreddingFieldNames;

    public MapSharedShreddingRowConverter(
            RowType logicalType,
            Map<String, Integer> fieldToNumColumns,
            Map<String, MapSharedShreddingColumnPlacementPolicy> fieldToColumnPlacementPolicy) {
        this.logicalType = logicalType;
        this.physicalType =
                MapSharedShreddingUtils.logicalToPhysicalSchema(logicalType, fieldToNumColumns);
        this.contextByFieldName = new LinkedHashMap<>();
        this.contextByFieldPos = new ColumnContext[logicalType.getFieldCount()];
        this.shreddingFieldNames = new ArrayList<>();

        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            DataField field = logicalType.getFields().get(i);
            Integer numColumns = fieldToNumColumns.get(field.name());
            if (numColumns == null) {
                continue;
            }

            MapType mapType = (MapType) field.type();
            MapSharedShreddingColumnPlacementPolicy placementPolicy =
                    fieldToColumnPlacementPolicy.get(field.name());
            checkArgument(
                    placementPolicy != null,
                    "Missing column placement policy for shared-shredding field '%s'.",
                    field.name());
            ColumnContext context =
                    new ColumnContext(field.name(), numColumns, mapType, placementPolicy);
            contextByFieldName.put(field.name(), context);
            contextByFieldPos[i] = context;
            shreddingFieldNames.add(field.name());
        }
    }

    public RowType physicalType() {
        return physicalType;
    }

    public List<String> shreddingFieldNames() {
        return Collections.unmodifiableList(shreddingFieldNames);
    }

    public InternalRow convert(InternalRow logicalRow) {
        return new SharedShreddingRow(logicalRow);
    }

    public MapSharedShreddingFieldMeta buildFieldMeta(String fieldName) {
        ColumnContext context = contextByFieldName.get(fieldName);
        if (context == null) {
            throw new IllegalArgumentException(
                    "Cannot find shared-shredding field in row converter: " + fieldName);
        }
        return new MapSharedShreddingFieldMeta(
                context.dict.nameToId(),
                context.allocator.fieldToColumns(),
                context.allocator.overflowFieldSet(),
                context.allocator.numColumns(),
                context.allocator.maxRowWidth());
    }

    private InternalRow convertMap(InternalMap map, ColumnContext context) {
        int size = map.size();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();

        List<Integer> fieldIds = new ArrayList<>(size);
        Map<Integer, Integer> fieldIdToValueIndex = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            Object key = context.keyGetter.getElementOrNull(keys, i);
            if (key == null) {
                throw new IllegalArgumentException(
                        "Shared-shredding MAP keys cannot be null for field: " + context.fieldName);
            }
            int fieldId = context.dict.getOrAssign(key.toString());
            fieldIds.add(fieldId);
            fieldIdToValueIndex.put(fieldId, i);
        }

        MapSharedShreddingColumnAllocator.RowAllocation allocation =
                context.allocator.allocateRow(fieldIds);
        int[] colToField = allocation.colToField();
        GenericRow physicalStruct = new GenericRow(context.numColumns + 2);
        physicalStruct.setField(0, new GenericArray(colToField));
        for (int i = 0; i < context.numColumns; i++) {
            int fieldId = colToField[i];
            if (fieldId == -1) {
                physicalStruct.setField(1 + i, null);
            } else {
                physicalStruct.setField(
                        1 + i,
                        context.valueGetter.getElementOrNull(
                                values, fieldIdToValueIndex.get(fieldId)));
            }
        }

        if (allocation.overflowFields().isEmpty()) {
            physicalStruct.setField(1 + context.numColumns, null);
        } else {
            Map<Integer, Object> overflow = new LinkedHashMap<>();
            for (Integer fieldId : allocation.overflowFields()) {
                overflow.put(
                        fieldId,
                        context.valueGetter.getElementOrNull(
                                values, fieldIdToValueIndex.get(fieldId)));
            }
            physicalStruct.setField(1 + context.numColumns, new GenericMap(overflow));
        }
        return physicalStruct;
    }

    private class SharedShreddingRow implements InternalRow {

        private final InternalRow row;
        private final InternalRow[] convertedFields;
        private final boolean[] converted;

        private SharedShreddingRow(InternalRow row) {
            this.row = row;
            this.convertedFields = new InternalRow[contextByFieldPos.length];
            this.converted = new boolean[contextByFieldPos.length];
        }

        @Override
        public int getFieldCount() {
            return row.getFieldCount();
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
            return row.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return row.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return row.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return row.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return row.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return row.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return row.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return row.getString(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return row.getDecimal(pos, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return row.getTimestamp(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return row.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            return row.getVariant(pos);
        }

        @Override
        public Blob getBlob(int pos) {
            return row.getBlob(pos);
        }

        @Override
        public InternalArray getArray(int pos) {
            return row.getArray(pos);
        }

        @Override
        public InternalVector getVector(int pos) {
            return row.getVector(pos);
        }

        @Override
        public InternalMap getMap(int pos) {
            return row.getMap(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            ColumnContext context = contextByFieldPos[pos];
            if (context == null) {
                return row.getRow(pos, numFields);
            }
            if (row.isNullAt(pos)) {
                return null;
            }
            if (!converted[pos]) {
                convertedFields[pos] = convertMap(row.getMap(pos), context);
                converted[pos] = true;
            }
            return convertedFields[pos];
        }
    }

    private static class ColumnContext {

        private final String fieldName;
        private final int numColumns;
        private final InternalArray.ElementGetter keyGetter;
        private final InternalArray.ElementGetter valueGetter;
        private final MapSharedShreddingFieldDict dict;
        private final MapSharedShreddingColumnAllocator allocator;

        private ColumnContext(
                String fieldName,
                int numColumns,
                MapType mapType,
                MapSharedShreddingColumnPlacementPolicy placementPolicy) {
            this.fieldName = fieldName;
            this.numColumns = numColumns;
            this.keyGetter = InternalArray.createElementGetter(mapType.getKeyType());
            DataType valueType = mapType.getValueType();
            this.valueGetter = InternalArray.createElementGetter(valueType);
            this.dict = new MapSharedShreddingFieldDict();
            this.allocator = createAllocator(numColumns, placementPolicy);
        }

        private static MapSharedShreddingColumnAllocator createAllocator(
                int numColumns, MapSharedShreddingColumnPlacementPolicy placementPolicy) {
            switch (placementPolicy) {
                case PLAIN:
                    return new PlainMapSharedShreddingColumnAllocator(numColumns);
                case SEQUENTIAL:
                    return new SequentialMapSharedShreddingColumnAllocator(numColumns);
                case LRU:
                    return new LruMapSharedShreddingColumnAllocator(numColumns);
                default:
                    throw new IllegalArgumentException(
                            "Unknown shared-shredding column placement policy: " + placementPolicy);
            }
        }
    }
}
