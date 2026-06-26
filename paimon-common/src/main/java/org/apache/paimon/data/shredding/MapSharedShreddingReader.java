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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A reader wrapper that rebuilds logical MAP values from shared-shredding physical ROW values.
 *
 * <p>The wrapped format reader reads the physical schema stored in a file. This reader presents the
 * original logical schema to upper layers by lazily converting only shared-shredding MAP fields
 * when {@link InternalRow#getMap(int)} is called.
 */
public class MapSharedShreddingReader implements FileRecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> reader;
    private final RowType logicalType;
    private final Map<Integer, SharedShreddingContext> contextByFieldIndex;

    public MapSharedShreddingReader(
            FileRecordReader<InternalRow> reader,
            RowType logicalType,
            Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        this.reader = reader;
        this.logicalType = logicalType;
        this.contextByFieldIndex = createContexts(logicalType, fieldMetas);
    }

    public static Map<String, MapSharedShreddingFieldMeta> readSharedShreddingMetas(
            Map<String, Map<String, String>> fieldMetadata) {
        Map<String, MapSharedShreddingFieldMeta> metas = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : fieldMetadata.entrySet()) {
            if (MapSharedShreddingUtils.hasShreddingMetadata(entry.getValue())) {
                metas.put(
                        entry.getKey(),
                        MapSharedShreddingUtils.deserializeMetadata(
                                entry.getValue(),
                                MapSharedShreddingDefine.DEFAULT_DICT_COMPRESSION));
            }
        }
        return metas;
    }

    private static Map<Integer, SharedShreddingContext> createContexts(
            RowType logicalType, Map<String, MapSharedShreddingFieldMeta> fieldMetas) {
        Map<Integer, SharedShreddingContext> contexts = new LinkedHashMap<>();
        for (int i = 0; i < logicalType.getFieldCount(); i++) {
            DataField field = logicalType.getFields().get(i);
            MapSharedShreddingFieldMeta fieldMeta = fieldMetas.get(field.name());
            if (fieldMeta != null) {
                contexts.put(i, new SharedShreddingContext(fieldMeta, field.type()));
            }
        }
        return contexts;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        return new SharedShreddingIterator(iterator);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private class SharedShreddingIterator implements FileRecordIterator<InternalRow> {

        private final FileRecordIterator<InternalRow> iterator;

        private SharedShreddingIterator(FileRecordIterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        @Override
        public long returnedPosition() {
            return iterator.returnedPosition();
        }

        @Override
        public Path filePath() {
            return iterator.filePath();
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            InternalRow row = iterator.next();
            if (row == null) {
                return null;
            }
            return new SharedShreddingRow(row);
        }

        @Override
        public void releaseBatch() {
            iterator.releaseBatch();
        }
    }

    private class SharedShreddingRow implements InternalRow {

        private final InternalRow row;

        private SharedShreddingRow(InternalRow row) {
            this.row = row;
        }

        @Override
        public int getFieldCount() {
            return logicalType.getFieldCount();
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
            SharedShreddingContext context = contextByFieldIndex.get(pos);
            if (context == null) {
                return row.getMap(pos);
            }
            if (row.isNullAt(pos)) {
                return null;
            }
            InternalRow physicalRow = row.getRow(pos, context.numPhysicalFields);
            return rebuildLogicalMap(physicalRow, context);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return row.getRow(pos, numFields);
        }
    }

    private static InternalMap rebuildLogicalMap(
            InternalRow physicalRow, SharedShreddingContext context) {
        if (physicalRow.isNullAt(0)) {
            throw new IllegalArgumentException(
                    "Shared-shredding field mapping cannot be null in a non-null physical row.");
        }
        InternalArray fieldMapping = physicalRow.getArray(0);
        Map<Object, Object> result = new LinkedHashMap<>();
        if (fieldMapping.size() != context.numColumns) {
            throw new IllegalArgumentException(
                    "Shared-shredding field mapping size "
                            + fieldMapping.size()
                            + " does not match metadata num columns "
                            + context.numColumns
                            + ".");
        }
        for (int column = 0; column < context.numColumns; column++) {
            if (fieldMapping.isNullAt(column)) {
                throw new IllegalArgumentException(
                        "Shared-shredding field mapping element cannot be null.");
            }
            int fieldId = fieldMapping.getInt(column);
            if (fieldId < 0) {
                continue;
            }
            BinaryString fieldName = context.nameById.get(fieldId);
            if (fieldName == null) {
                throw new IllegalArgumentException(
                        "Cannot find shared-shredding field id " + fieldId + " in metadata.");
            }
            int valuePosition = column + 1;
            if (valuePosition >= physicalRow.getFieldCount()) {
                throw new IllegalArgumentException(
                        "Cannot find shared-shredding physical column "
                                + MapSharedShreddingDefine.physicalColumnName(column)
                                + ".");
            }
            // TODO(lisizhuo.lsz): Support rebuilding in the user requested selected-key order once key-level
            // projection is pushed down. Full map reads currently follow the physical/metadata
            // layout order.
            result.put(fieldName, context.valueGetters[column].getFieldOrNull(physicalRow));
        }
        if (context.overflowPosition < physicalRow.getFieldCount()
                && !physicalRow.isNullAt(context.overflowPosition)) {
            InternalMap overflow = physicalRow.getMap(context.overflowPosition);
            InternalArray keys = overflow.keyArray();
            InternalArray values = overflow.valueArray();
            for (int i = 0; i < overflow.size(); i++) {
                if (keys.isNullAt(i)) {
                    throw new IllegalArgumentException(
                            "Shared-shredding overflow field id cannot be null.");
                }
                int fieldId = keys.getInt(i);
                BinaryString fieldName = context.nameById.get(fieldId);
                if (fieldName == null) {
                    throw new IllegalArgumentException(
                            "Cannot find shared-shredding field id " + fieldId + " in metadata.");
                }
                result.put(fieldName, context.overflowValueGetter.getElementOrNull(values, i));
            }
        }
        return new GenericMap(result);
    }

    private static class SharedShreddingContext {

        private final Map<Integer, BinaryString> nameById;
        private final InternalRow.FieldGetter[] valueGetters;
        private final InternalArray.ElementGetter overflowValueGetter;
        private final int numColumns;
        private final int overflowPosition;
        private final int numPhysicalFields;

        private SharedShreddingContext(MapSharedShreddingFieldMeta fieldMeta, DataType fieldType) {
            MapType mapType = (MapType) fieldType;
            this.nameById = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : fieldMeta.nameToId().entrySet()) {
                // ordered by dict
                this.nameById.put(entry.getValue(), BinaryString.fromString(entry.getKey()));
            }
            this.valueGetters = new InternalRow.FieldGetter[fieldMeta.numColumns()];
            for (int i = 0; i < fieldMeta.numColumns(); i++) {
                // plus 1 to skip __field_mapping
                this.valueGetters[i] =
                        InternalRow.createFieldGetter(mapType.getValueType(), i + 1);
            }
            this.overflowValueGetter = InternalArray.createElementGetter(mapType.getValueType());
            this.numColumns = fieldMeta.numColumns();
            this.overflowPosition = fieldMeta.numColumns() + 1;
            this.numPhysicalFields =
                    fieldMeta.overflowFieldSet().isEmpty()
                            ? fieldMeta.numColumns() + 1
                            : fieldMeta.numColumns() + 2;
        }
    }
}
