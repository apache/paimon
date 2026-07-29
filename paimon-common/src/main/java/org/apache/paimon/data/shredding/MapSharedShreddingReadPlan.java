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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnVectorUtils;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.RowToColumnConverter;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.heap.CastedMapColumnVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Read plan that rebuilds logical MAP values from shared-shredding physical ROW values. */
public class MapSharedShreddingReadPlan implements ShreddingReadPlan {

    private static final int FIELD_MAPPING_POSITION = 0;

    private final RowType logicalType;
    private final RowType physicalType;
    private final Map<Integer, SharedShreddingContext> contextByFieldIndex;

    public MapSharedShreddingReadPlan(
            RowType logicalType, Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        this.logicalType = logicalType;
        this.physicalType = MapSharedShreddingUtils.buildPhysicalReadType(logicalType, fieldMetas);
        this.contextByFieldIndex = createContexts(logicalType, physicalType, fieldMetas);
    }

    @Override
    public RowType logicalRowType() {
        return logicalType;
    }

    @Override
    public RowType physicalRowType() {
        return physicalType;
    }

    @Override
    public boolean isIdentity() {
        return contextByFieldIndex.isEmpty();
    }

    @Override
    public ShreddingBatchAssembler batchAssembler() {
        return new MapSharedShreddingBatchAssembler();
    }

    private static Map<Integer, SharedShreddingContext> createContexts(
            RowType logicalType,
            RowType physicalType,
            Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        Map<Integer, SharedShreddingContext> contexts = new LinkedHashMap<>();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            DataField field = logicalType.getFields().get(i);
            MapSharedShreddingFieldMeta fieldMeta = fieldMetas.get(field.name());
            if (fieldMeta == null) {
                continue;
            }

            RowType physicalStructType = (RowType) physicalType.getTypeAt(i);
            SharedPhysicalContext physicalContext =
                    new SharedPhysicalContext(fieldMeta, physicalStructType);
            if (field.type() instanceof MapType) {
                contexts.put(i, new FullMapContext(physicalContext, (MapType) field.type()));
            } else if (MapSelectedKeysMetadataUtils.isMapSelectedKeysField(field)) {
                contexts.put(
                        i,
                        new SelectedKeysContext(
                                physicalContext,
                                fieldMeta,
                                (RowType) field.type(),
                                field.description()));
            }
        }
        return contexts;
    }

    private class MapSharedShreddingBatchAssembler implements ShreddingBatchAssembler {

        @Override
        public VectorizedColumnBatch assemble(VectorizedColumnBatch physicalBatch) {
            ColumnVector[] logicalVectors = new ColumnVector[logicalType.getFieldCount()];
            for (int i = 0; i < logicalVectors.length; i++) {
                SharedShreddingContext context = contextByFieldIndex.get(i);
                if (context == null) {
                    logicalVectors[i] = physicalBatch.columns[i];
                } else {
                    logicalVectors[i] =
                            context.materialize(
                                    (RowColumnVector) physicalBatch.columns[i],
                                    physicalBatch.getNumRows());
                }
            }
            return physicalBatch.copy(logicalVectors);
        }
    }

    private static CastedMapColumnVector materializeLogicalMapVector(
            RowColumnVector physicalVector, FullMapContext context, int rowCount) {
        ColumnVector[] physicalChildren = physicalChildren(physicalVector);
        int totalElements =
                countLogicalMapElements(physicalVector, physicalChildren, context, rowCount);

        WritableColumnVector keyVector =
                ColumnVectorUtils.createWritableColumnVector(
                        totalElements, context.mapType.getKeyType());
        WritableColumnVector valueVector =
                ColumnVectorUtils.createWritableColumnVector(
                        totalElements, context.mapType.getValueType());
        HeapMapVector mapVector = new HeapMapVector(rowCount, keyVector, valueVector);

        int offset = 0;
        for (int row = 0; row < rowCount; row++) {
            if (physicalVector.isNullAt(row)) {
                mapVector.setNullAt(row);
                mapVector.putOffsetLength(row, offset, 0);
                continue;
            }

            int elementCount =
                    appendLogicalMapElements(
                            physicalChildren, row, context, keyVector, valueVector);
            mapVector.putOffsetLength(row, offset, elementCount);
            offset += elementCount;
        }
        mapVector.addElementsAppended(rowCount);

        return new CastedMapColumnVector(
                mapVector,
                ColumnVectorUtils.createReadableColumnVectors(
                        Arrays.asList(context.mapType.getKeyType(), context.mapType.getValueType()),
                        new WritableColumnVector[] {keyVector, valueVector}));
    }

    private static ColumnVector materializeSelectedKeysRowVector(
            RowColumnVector physicalVector, SelectedKeysContext context, int rowCount) {
        ColumnVector[] physicalChildren = physicalChildren(physicalVector);
        RowType selectedKeysType = context.selectedKeysType;
        WritableColumnVector[] selectedKeyVectors =
                new WritableColumnVector[selectedKeysType.getFieldCount()];
        for (int i = 0; i < selectedKeyVectors.length; i++) {
            selectedKeyVectors[i] =
                    ColumnVectorUtils.createWritableColumnVector(
                            rowCount, selectedKeysType.getTypeAt(i));
        }
        HeapRowVector rowVector = new HeapRowVector(rowCount, selectedKeyVectors);

        for (int row = 0; row < rowCount; row++) {
            if (physicalVector.isNullAt(row)) {
                rowVector.appendNull();
                continue;
            }

            InternalArray fieldMapping = fieldMapping(physicalChildren, row, context.physical);
            InternalMap overflow = null;
            for (int ordinal = 0; ordinal < selectedKeyVectors.length; ordinal++) {
                if (context.selectedKeyOverflow[ordinal] && overflow == null) {
                    overflow = overflowMap(physicalChildren, row, context.physical);
                }
                appendSelectedKeyValue(
                        physicalChildren,
                        fieldMapping,
                        context.selectedKeyOverflow[ordinal] ? overflow : null,
                        row,
                        context,
                        ordinal,
                        selectedKeyVectors[ordinal]);
            }
            rowVector.appendRow();
        }

        return ColumnVectorUtils.createReadableColumnVector(selectedKeysType, rowVector);
    }

    private static ColumnVector[] physicalChildren(RowColumnVector physicalVector) {
        ColumnVector[] physicalChildren = physicalVector.getChildren();
        if (physicalChildren != null) {
            return physicalChildren;
        }
        return physicalVector.getBatch().columns;
    }

    private static int countLogicalMapElements(
            RowColumnVector physicalVector,
            ColumnVector[] physicalChildren,
            FullMapContext context,
            int rowCount) {
        int totalElements = 0;
        for (int row = 0; row < rowCount; row++) {
            if (!physicalVector.isNullAt(row)) {
                totalElements += countLogicalMapElements(physicalChildren, row, context);
            }
        }
        return totalElements;
    }

    private static int countLogicalMapElements(
            ColumnVector[] physicalChildren, int row, FullMapContext context) {
        int count = 0;
        InternalArray fieldMapping = fieldMapping(physicalChildren, row, context.physical);
        for (int column = 0; column < context.physical.numColumns; column++) {
            if (logicalFieldName(fieldMapping, column, context.physical) != null) {
                count++;
            }
        }

        InternalMap overflow = overflowMap(physicalChildren, row, context.physical);
        if (overflow != null) {
            InternalArray keys = overflow.keyArray();
            for (int i = 0; i < overflow.size(); i++) {
                if (context.physical.nameById.containsKey(keys.getInt(i))) {
                    count++;
                }
            }
        }
        return count;
    }

    private static int appendLogicalMapElements(
            ColumnVector[] physicalChildren,
            int row,
            FullMapContext context,
            WritableColumnVector keyVector,
            WritableColumnVector valueVector) {
        int count = 0;
        InternalArray fieldMapping = fieldMapping(physicalChildren, row, context.physical);
        for (int column = 0; column < context.physical.numColumns; column++) {
            BinaryString fieldName = logicalFieldName(fieldMapping, column, context.physical);
            if (fieldName == null) {
                continue;
            }

            context.keyConverter.append(fieldName, keyVector);
            ColumnVector valueColumn = physicalColumn(physicalChildren, column, context.physical);
            appendValue(context.valueConverter, valueColumn, row, valueVector);
            count++;
        }

        InternalMap overflow = overflowMap(physicalChildren, row, context.physical);
        if (overflow != null) {
            InternalArray keys = overflow.keyArray();
            InternalArray values = overflow.valueArray();
            for (int i = 0; i < overflow.size(); i++) {
                BinaryString fieldName = context.physical.nameById.get(keys.getInt(i));
                if (fieldName != null) {
                    context.keyConverter.append(fieldName, keyVector);
                    appendValue(context.valueConverter, values, i, valueVector);
                    count++;
                }
            }
        }
        return count;
    }

    private static void appendSelectedKeyValue(
            ColumnVector[] physicalChildren,
            InternalArray fieldMapping,
            @Nullable InternalMap overflow,
            int row,
            SelectedKeysContext context,
            int ordinal,
            WritableColumnVector valueVector) {
        int fieldId = context.selectedKeyFieldIds[ordinal];
        if (fieldId < 0) {
            valueVector.appendNull();
            return;
        }

        int[] candidateColumns = context.selectedKeyColumns[ordinal];
        for (int i = 0; i < candidateColumns.length; i++) {
            int column = candidateColumns[i];
            if (mappedFieldId(fieldMapping, column) == fieldId) {
                appendValue(
                        context.selectedValueConverters[ordinal],
                        physicalColumn(physicalChildren, column, context.physical),
                        row,
                        valueVector);
                return;
            }
        }

        if (overflow != null) {
            InternalArray keys = overflow.keyArray();
            InternalArray values = overflow.valueArray();
            for (int i = 0; i < overflow.size(); i++) {
                if (!keys.isNullAt(i) && keys.getInt(i) == fieldId) {
                    appendValue(context.selectedValueConverters[ordinal], values, i, valueVector);
                    return;
                }
            }
        }

        valueVector.appendNull();
    }

    private static void appendValue(
            RowToColumnConverter.ElementConverter converter,
            ColumnVector source,
            int row,
            WritableColumnVector valueVector) {
        if (source.isNullAt(row)) {
            valueVector.appendNull();
        } else {
            converter.append(source, row, valueVector);
        }
    }

    private static void appendValue(
            RowToColumnConverter.ElementConverter converter,
            InternalArray source,
            int pos,
            WritableColumnVector valueVector) {
        if (source.isNullAt(pos)) {
            valueVector.appendNull();
        } else {
            converter.append(source, pos, valueVector);
        }
    }

    private static InternalArray fieldMapping(
            ColumnVector[] physicalChildren, int row, SharedPhysicalContext context) {
        InternalArray fieldMapping =
                ((ArrayColumnVector) physicalChildren[FIELD_MAPPING_POSITION]).getArray(row);
        checkArgument(
                fieldMapping.size() == context.numColumns,
                "Shared-shredding field mapping size %s does not match metadata num columns %s.",
                fieldMapping.size(),
                context.numColumns);
        for (int column = 0; column < fieldMapping.size(); column++) {
            checkArgument(
                    !fieldMapping.isNullAt(column),
                    "Shared-shredding field mapping must not contain null at column %s.",
                    column);
        }
        return fieldMapping;
    }

    private static BinaryString logicalFieldName(
            InternalArray fieldMapping, int column, SharedPhysicalContext context) {
        int fieldId = mappedFieldId(fieldMapping, column);
        return fieldId < 0 ? null : context.nameById.get(fieldId);
    }

    private static int mappedFieldId(InternalArray fieldMapping, int column) {
        return fieldMapping.getInt(column);
    }

    private static ColumnVector physicalColumn(
            ColumnVector[] physicalChildren, int column, SharedPhysicalContext context) {
        int position = context.physicalColumnPositions[column];
        checkArgument(
                position >= 0,
                "Shared-shredding physical column %s was not included in the read schema.",
                MapSharedShreddingDefine.physicalColumnName(column));
        return physicalChildren[position];
    }

    private static InternalMap overflowMap(
            ColumnVector[] physicalChildren, int row, SharedPhysicalContext context) {
        if (context.overflowPosition < 0 || context.overflowPosition >= physicalChildren.length) {
            return null;
        }

        ColumnVector overflowVector = physicalChildren[context.overflowPosition];
        return overflowVector.isNullAt(row) ? null : ((MapColumnVector) overflowVector).getMap(row);
    }

    private interface SharedShreddingContext {

        ColumnVector materialize(RowColumnVector physicalVector, int rowCount);
    }

    private static class SharedPhysicalContext {

        private final Map<Integer, BinaryString> nameById;
        private final int numColumns;
        private final int[] physicalColumnPositions;
        private final int overflowPosition;

        private SharedPhysicalContext(
                MapSharedShreddingFieldMeta fieldMeta, RowType physicalStructType) {
            this.nameById = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : fieldMeta.nameToId().entrySet()) {
                this.nameById.put(entry.getValue(), BinaryString.fromString(entry.getKey()));
            }
            this.numColumns = fieldMeta.numColumns();
            checkArgument(
                    physicalStructType.getFieldCount() > 0
                            && MapSharedShreddingDefine.FIELD_MAPPING.equals(
                                    physicalStructType.getFieldNames().get(FIELD_MAPPING_POSITION)),
                    "Shared-shredding physical struct must start with %s.",
                    MapSharedShreddingDefine.FIELD_MAPPING);
            this.physicalColumnPositions = physicalColumnPositions(physicalStructType, numColumns);
            this.overflowPosition = overflowPosition(physicalStructType);
        }

        private static int[] physicalColumnPositions(RowType physicalStructType, int numColumns) {
            int[] positions = new int[numColumns];
            Arrays.fill(positions, -1);
            int physicalColumnEnd = overflowPosition(physicalStructType);
            if (physicalColumnEnd < 0) {
                physicalColumnEnd = physicalStructType.getFieldCount();
            }
            for (int i = FIELD_MAPPING_POSITION + 1; i < physicalColumnEnd; i++) {
                String fieldName = physicalStructType.getFieldNames().get(i);
                int column = physicalColumnIndex(fieldName);
                checkArgument(
                        column >= 0,
                        "Unexpected shared-shredding physical field %s at position %s.",
                        fieldName,
                        i);
                checkArgument(
                        column < numColumns,
                        "Shared-shredding physical column %s exceeds metadata num columns %s.",
                        fieldName,
                        numColumns);
                positions[column] = i;
            }
            return positions;
        }

        private static int overflowPosition(RowType physicalStructType) {
            int lastPosition = physicalStructType.getFieldCount() - 1;
            if (lastPosition <= FIELD_MAPPING_POSITION) {
                return -1;
            }
            return MapSharedShreddingDefine.OVERFLOW.equals(
                            physicalStructType.getFieldNames().get(lastPosition))
                    ? lastPosition
                    : -1;
        }

        private static int physicalColumnIndex(String fieldName) {
            String prefix = "__col_";
            if (!fieldName.startsWith(prefix)) {
                return -1;
            }
            return Integer.parseInt(fieldName.substring(prefix.length()));
        }
    }

    private static class FullMapContext implements SharedShreddingContext {

        private final SharedPhysicalContext physical;
        private final MapType mapType;
        private final RowToColumnConverter.ElementConverter keyConverter;
        private final RowToColumnConverter.ElementConverter valueConverter;

        private FullMapContext(SharedPhysicalContext physical, MapType mapType) {
            this.physical = physical;
            this.mapType = mapType;
            this.keyConverter = RowToColumnConverter.createElementConverter(mapType.getKeyType());
            this.valueConverter =
                    RowToColumnConverter.createElementConverter(mapType.getValueType());
        }

        @Override
        public ColumnVector materialize(RowColumnVector physicalVector, int rowCount) {
            return materializeLogicalMapVector(physicalVector, this, rowCount);
        }
    }

    private static class SelectedKeysContext implements SharedShreddingContext {

        private final SharedPhysicalContext physical;
        private final RowType selectedKeysType;
        private final int[] selectedKeyFieldIds;
        private final int[][] selectedKeyColumns;
        private final boolean[] selectedKeyOverflow;
        private final RowToColumnConverter.ElementConverter[] selectedValueConverters;

        private SelectedKeysContext(
                SharedPhysicalContext physical,
                MapSharedShreddingFieldMeta fieldMeta,
                RowType selectedKeysType,
                String selectedKeysMetadata) {
            this.physical = physical;
            this.selectedKeysType = selectedKeysType;
            List<String> selectedKeys =
                    MapSelectedKeysMetadataUtils.selectedKeys(selectedKeysMetadata);
            checkArgument(
                    selectedKeys.size() == selectedKeysType.getFieldCount(),
                    "Selected-key metadata size %s does not match selected ROW field count %s.",
                    selectedKeys.size(),
                    selectedKeysType.getFieldCount());
            this.selectedKeyFieldIds = new int[selectedKeys.size()];
            this.selectedKeyColumns = new int[selectedKeys.size()][];
            this.selectedKeyOverflow = new boolean[selectedKeys.size()];
            this.selectedValueConverters =
                    new RowToColumnConverter.ElementConverter[selectedKeys.size()];
            for (int i = 0; i < selectedKeys.size(); i++) {
                Integer fieldId = fieldMeta.nameToId().get(selectedKeys.get(i));
                this.selectedKeyFieldIds[i] = fieldId == null ? -1 : fieldId;
                this.selectedKeyColumns[i] =
                        fieldId == null ? new int[0] : candidateColumns(fieldMeta, fieldId);
                this.selectedKeyOverflow[i] =
                        fieldId != null && fieldMeta.overflowFieldSet().contains(fieldId);
                this.selectedValueConverters[i] =
                        RowToColumnConverter.createElementConverter(selectedKeysType.getTypeAt(i));
            }
        }

        @Override
        public ColumnVector materialize(RowColumnVector physicalVector, int rowCount) {
            return materializeSelectedKeysRowVector(physicalVector, this, rowCount);
        }

        private static int[] candidateColumns(MapSharedShreddingFieldMeta fieldMeta, int fieldId) {
            List<Integer> columns = fieldMeta.fieldToColumns().get(fieldId);
            if (columns == null || columns.isEmpty()) {
                return new int[0];
            }
            int[] result = new int[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                result[i] = columns.get(i);
            }
            return result;
        }
    }
}
