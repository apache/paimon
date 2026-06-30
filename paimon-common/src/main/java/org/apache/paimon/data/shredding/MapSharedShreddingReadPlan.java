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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

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

    private static InternalMap rebuildLogicalMap(
            InternalRow physicalRow, SharedShreddingContext context) {
        InternalArray fieldMapping = physicalRow.getArray(0);
        checkArgument(
                fieldMapping.size() == context.numColumns,
                "Shared-shredding field mapping size %s does not match metadata num columns %s.",
                fieldMapping.size(),
                context.numColumns);
        Map<Object, Object> result = new LinkedHashMap<>();
        for (int column = 0; column < context.numColumns; column++) {
            int fieldId = fieldMapping.isNullAt(column) ? -1 : fieldMapping.getInt(column);
            if (fieldId < 0) {
                continue;
            }
            BinaryString fieldName = context.nameById.get(fieldId);
            if (fieldName == null) {
                continue;
            }
            Object value = null;
            int valuePosition = column + 1;
            if (valuePosition < physicalRow.getFieldCount()
                    && !physicalRow.isNullAt(valuePosition)) {
                value = context.valueGetters[column].getFieldOrNull(physicalRow);
            }
            result.put(fieldName, value);
        }
        if (context.overflowPosition < physicalRow.getFieldCount()
                && !physicalRow.isNullAt(context.overflowPosition)) {
            InternalMap overflow = physicalRow.getMap(context.overflowPosition);
            InternalArray keys = overflow.keyArray();
            InternalArray values = overflow.valueArray();
            for (int i = 0; i < overflow.size(); i++) {
                int fieldId = keys.getInt(i);
                BinaryString fieldName = context.nameById.get(fieldId);
                if (fieldName != null) {
                    result.put(fieldName, context.overflowValueGetter.getElementOrNull(values, i));
                }
            }
        }
        return new GenericMap(result);
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
                            new SharedShreddingMapColumnVector(
                                    (RowColumnVector) physicalBatch.columns[i], context);
                }
            }
            return physicalBatch.copy(logicalVectors);
        }
    }

    private static class SharedShreddingMapColumnVector implements MapColumnVector {

        private final RowColumnVector physicalVector;
        private final SharedShreddingContext context;

        private SharedShreddingMapColumnVector(
                RowColumnVector physicalVector, SharedShreddingContext context) {
            this.physicalVector = physicalVector;
            this.context = context;
        }

        @Override
        public InternalMap getMap(int i) {
            if (physicalVector.isNullAt(i)) {
                return null;
            }
            // TODO (xinyu.lxy): Move this per-row map assemble path to a real batch-level
            // physical-to-logical conversion.
            return rebuildLogicalMap(physicalVector.getRow(i), context);
        }

        @Override
        public boolean isNullAt(int i) {
            return physicalVector.isNullAt(i);
        }

        @Override
        public int getCapacity() {
            return physicalVector.getCapacity();
        }

        @Override
        public ColumnVector[] getChildren() {
            // This is a lazy row-access wrapper. It rebuilds maps through getMap(rowId) and does
            // not expose contiguous key/value child vectors until the assemble path is moved to a
            // real batch-level conversion.
            return null;
        }
    }

    private static class SharedShreddingContext {

        private final Map<Integer, BinaryString> nameById;
        private final InternalRow.FieldGetter[] valueGetters;
        private final InternalArray.ElementGetter overflowValueGetter;
        private final int numColumns;
        private final int overflowPosition;

        private SharedShreddingContext(MapSharedShreddingFieldMeta fieldMeta, DataType fieldType) {
            MapType mapType = (MapType) fieldType;
            this.nameById = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : fieldMeta.nameToId().entrySet()) {
                this.nameById.put(entry.getValue(), BinaryString.fromString(entry.getKey()));
            }
            this.valueGetters = new InternalRow.FieldGetter[fieldMeta.numColumns()];
            for (int i = 0; i < fieldMeta.numColumns(); i++) {
                this.valueGetters[i] = InternalRow.createFieldGetter(mapType.getValueType(), i + 1);
            }
            this.overflowValueGetter = InternalArray.createElementGetter(mapType.getValueType());
            this.numColumns = fieldMeta.numColumns();
            this.overflowPosition = fieldMeta.numColumns() + 1;
        }
    }
}
