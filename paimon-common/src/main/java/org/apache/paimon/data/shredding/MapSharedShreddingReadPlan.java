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
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Read plan that rebuilds logical MAP values from shared-shredding physical ROW values. */
public class MapSharedShreddingReadPlan implements ShreddingReadPlan {

    private final RowType logicalType;
    private final RowType physicalType;
    private final Map<Integer, SharedShreddingContext> contextByFieldIndex;

    public MapSharedShreddingReadPlan(
            RowType logicalType, Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        this.logicalType = logicalType;
        this.physicalType = MapSharedShreddingUtils.buildPhysicalReadType(logicalType, fieldMetas);
        this.contextByFieldIndex = createContexts(logicalType, fieldMetas);
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
            RowType logicalType, Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        Map<Integer, SharedShreddingContext> contexts = new LinkedHashMap<>();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            DataField field = logicalType.getFields().get(i);
            MapSharedShreddingFieldMeta fieldMeta = fieldMetas.get(field.name());
            if (fieldMeta != null && field.type() instanceof MapType) {
                contexts.put(i, new SharedShreddingContext(fieldMeta, field.type()));
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
                            materializeLogicalMapVector(
                                    (RowColumnVector) physicalBatch.columns[i],
                                    context,
                                    physicalBatch.getNumRows());
                }
            }
            return physicalBatch.copy(logicalVectors);
        }
    }

    private static CastedMapColumnVector materializeLogicalMapVector(
            RowColumnVector physicalVector, SharedShreddingContext context, int rowCount) {
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
            SharedShreddingContext context,
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
            ColumnVector[] physicalChildren, int row, SharedShreddingContext context) {
        int count = 0;
        InternalArray fieldMapping = fieldMapping(physicalChildren, row, context);
        for (int column = 0; column < context.numColumns; column++) {
            if (logicalFieldName(fieldMapping, column, context) != null) {
                count++;
            }
        }

        InternalMap overflow = overflowMap(physicalChildren, row, context);
        if (overflow != null) {
            InternalArray keys = overflow.keyArray();
            for (int i = 0; i < overflow.size(); i++) {
                if (context.nameById.containsKey(keys.getInt(i))) {
                    count++;
                }
            }
        }
        return count;
    }

    private static int appendLogicalMapElements(
            ColumnVector[] physicalChildren,
            int row,
            SharedShreddingContext context,
            WritableColumnVector keyVector,
            WritableColumnVector valueVector) {
        int count = 0;
        InternalArray fieldMapping = fieldMapping(physicalChildren, row, context);
        for (int column = 0; column < context.numColumns; column++) {
            BinaryString fieldName = logicalFieldName(fieldMapping, column, context);
            if (fieldName == null) {
                continue;
            }

            context.keyConverter.append(fieldName, keyVector);
            ColumnVector valueColumn = physicalChildren[column + 1];
            appendValue(context, valueColumn, row, valueVector);
            count++;
        }

        InternalMap overflow = overflowMap(physicalChildren, row, context);
        if (overflow != null) {
            InternalArray keys = overflow.keyArray();
            InternalArray values = overflow.valueArray();
            for (int i = 0; i < overflow.size(); i++) {
                BinaryString fieldName = context.nameById.get(keys.getInt(i));
                if (fieldName != null) {
                    context.keyConverter.append(fieldName, keyVector);
                    appendValue(context, values, i, valueVector);
                    count++;
                }
            }
        }
        return count;
    }

    private static void appendValue(
            SharedShreddingContext context,
            ColumnVector source,
            int row,
            WritableColumnVector valueVector) {
        if (source.isNullAt(row)) {
            valueVector.appendNull();
        } else {
            context.valueConverter.append(source, row, valueVector);
        }
    }

    private static void appendValue(
            SharedShreddingContext context,
            InternalArray source,
            int pos,
            WritableColumnVector valueVector) {
        if (source.isNullAt(pos)) {
            valueVector.appendNull();
        } else {
            context.valueConverter.append(source, pos, valueVector);
        }
    }

    private static InternalArray fieldMapping(
            ColumnVector[] physicalChildren, int row, SharedShreddingContext context) {
        InternalArray fieldMapping = ((ArrayColumnVector) physicalChildren[0]).getArray(row);
        checkArgument(
                fieldMapping.size() == context.numColumns,
                "Shared-shredding field mapping size %s does not match metadata num columns %s.",
                fieldMapping.size(),
                context.numColumns);
        return fieldMapping;
    }

    private static BinaryString logicalFieldName(
            InternalArray fieldMapping, int column, SharedShreddingContext context) {
        int fieldId = fieldMapping.isNullAt(column) ? -1 : fieldMapping.getInt(column);
        return fieldId < 0 ? null : context.nameById.get(fieldId);
    }

    private static InternalMap overflowMap(
            ColumnVector[] physicalChildren, int row, SharedShreddingContext context) {
        if (context.overflowPosition >= physicalChildren.length) {
            return null;
        }

        ColumnVector overflowVector = physicalChildren[context.overflowPosition];
        return overflowVector.isNullAt(row) ? null : ((MapColumnVector) overflowVector).getMap(row);
    }

    private static class SharedShreddingContext {

        private final MapType mapType;
        private final Map<Integer, BinaryString> nameById;
        private final RowToColumnConverter.ElementConverter keyConverter;
        private final RowToColumnConverter.ElementConverter valueConverter;
        private final int numColumns;
        private final int overflowPosition;

        private SharedShreddingContext(MapSharedShreddingFieldMeta fieldMeta, DataType fieldType) {
            this.mapType = (MapType) fieldType;
            this.nameById = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : fieldMeta.nameToId().entrySet()) {
                this.nameById.put(entry.getValue(), BinaryString.fromString(entry.getKey()));
            }
            this.keyConverter =
                    RowToColumnConverter.createElementConverter(this.mapType.getKeyType());
            this.valueConverter =
                    RowToColumnConverter.createElementConverter(this.mapType.getValueType());
            this.numColumns = fieldMeta.numColumns();
            this.overflowPosition = fieldMeta.numColumns() + 1;
        }
    }
}
