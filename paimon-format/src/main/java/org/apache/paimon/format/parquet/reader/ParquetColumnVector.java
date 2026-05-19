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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.ArrayColumnVector;
import org.apache.paimon.data.columnar.BooleanColumnVector;
import org.apache.paimon.data.columnar.ByteColumnVector;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.data.columnar.DoubleColumnVector;
import org.apache.paimon.data.columnar.FloatColumnVector;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.MapColumnVector;
import org.apache.paimon.data.columnar.RowColumnVector;
import org.apache.paimon.data.columnar.ShortColumnVector;
import org.apache.paimon.data.columnar.TimestampColumnVector;
import org.apache.paimon.data.columnar.heap.AbstractArrayBasedVector;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.heap.HeapArrayVector;
import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapMapVector;
import org.apache.paimon.data.columnar.heap.HeapRowVector;
import org.apache.paimon.data.columnar.writable.WritableBooleanVector;
import org.apache.paimon.data.columnar.writable.WritableByteVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableDoubleVector;
import org.apache.paimon.data.columnar.writable.WritableFloatVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;
import org.apache.paimon.data.columnar.writable.WritableTimestampVector;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.PaimonShreddingUtils.FieldToExtract;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantSchema;
import org.apache.paimon.format.parquet.type.MapShreddingField;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetGroupField;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.format.parquet.reader.ParquetReaderUtil.createReadableColumnVector;

/** Parquet Column tree. */
public class ParquetColumnVector {
    private final ParquetField column;
    private final List<ParquetColumnVector> children;
    private final WritableColumnVector vector;

    // Describes the file schema of the Parquet variant column. When it is not null, `children`
    // contains only one child that reads the underlying file content. This `ParquetColumnVector`
    // should assemble variant values from the file content.
    private VariantSchema variantSchema;
    // Only meaningful if `variantSchema` is not null. See `PaimonShreddingUtils.getFieldsToExtract`
    // for its meaning.
    private FieldToExtract[] fieldsToExtract;

    /**
     * Repetition & Definition levels These are allocated only for leaf columns; for non-leaf
     * columns, they simply maintain references to that of the former.
     */
    private HeapIntVector repetitionLevels;

    private HeapIntVector definitionLevels;

    /** Whether this column is primitive (i.e., leaf column). */
    private final boolean isPrimitive;

    /** Reader for this column - only set if 'isPrimitive' is true. */
    private VectorizedColumnReader columnReader;

    ParquetColumnVector(
            ParquetField column,
            WritableColumnVector vector,
            int capacity,
            Set<ParquetField> missingColumns,
            boolean isTopLevel) {
        this.column = column;
        this.vector = vector;
        this.children = new ArrayList<>();
        this.isPrimitive = column.isPrimitive();

        if (missingColumns.contains(column)) {
            vector.setAllNull();
            return;
        }

        if (column.variantFileType().isPresent()) {
            ParquetField fileContentCol = column.variantFileType().get();
            WritableColumnVector fileContent =
                    ParquetReaderUtil.createWritableColumnVector(
                            capacity, fileContentCol.getType());
            ParquetColumnVector contentVector =
                    new ParquetColumnVector(
                            fileContentCol, fileContent, capacity, missingColumns, false);
            children.add(contentVector);
            variantSchema =
                    PaimonShreddingUtils.buildVariantSchema((RowType) fileContentCol.getType());
            fieldsToExtract =
                    PaimonShreddingUtils.getFieldsToExtract(column.getType(), variantSchema);
            repetitionLevels = contentVector.repetitionLevels;
            definitionLevels = contentVector.definitionLevels;
        } else if (column instanceof MapShreddingField) {
            MapShreddingField mapField = (MapShreddingField) column;
            WritableColumnVector residualMap =
                    ParquetReaderUtil.createWritableColumnVector(capacity, column.getType());
            ParquetColumnVector residualVector =
                    new ParquetColumnVector(
                            mapField.residualMapField(),
                            residualMap,
                            capacity,
                            missingColumns,
                            false);
            children.add(residualVector);
            for (ParquetField sidecarField : mapField.sidecarFields()) {
                WritableColumnVector sidecar =
                        ParquetReaderUtil.createWritableColumnVector(
                                capacity, sidecarField.getType());
                children.add(
                        new ParquetColumnVector(
                                sidecarField, sidecar, capacity, missingColumns, true));
            }
            repetitionLevels = residualVector.repetitionLevels;
            definitionLevels = residualVector.definitionLevels;
        } else if (isPrimitive) {
            if (column.getRepetitionLevel() > 0) {
                repetitionLevels = new HeapIntVector(capacity);
            }
            // We don't need to create and store definition levels if the column is top-level.
            if (!isTopLevel) {
                definitionLevels = new HeapIntVector(capacity);
            }
        } else {
            ParquetGroupField groupField = (ParquetGroupField) column;
            Preconditions.checkArgument(
                    groupField.getChildren().size() == vector.getChildren().length);
            boolean allChildrenAreMissing = true;

            for (int i = 0; i < groupField.getChildren().size(); i++) {
                ParquetColumnVector childCv =
                        new ParquetColumnVector(
                                groupField.getChildren().get(i),
                                (WritableColumnVector) vector.getChildren()[i],
                                capacity,
                                missingColumns,
                                false);
                children.add(childCv);

                // Only use levels from non-missing child, this can happen if only some but not all
                // fields of a struct are missing.
                if (!childCv.vector.isAllNull()) {
                    allChildrenAreMissing = false;
                    this.repetitionLevels = childCv.repetitionLevels;
                    this.definitionLevels = childCv.definitionLevels;
                }
            }

            // This can happen if all the fields of a struct are missing, in which case we should
            // mark
            // the struct itself as a missing column
            if (allChildrenAreMissing) {
                vector.setAllNull();
            }
        }
    }

    /** Returns all the children of this column. */
    List<ParquetColumnVector> getChildren() {
        return children;
    }

    /** Returns all the leaf columns in depth-first order. */
    List<ParquetColumnVector> getLeaves() {
        List<ParquetColumnVector> result = new ArrayList<>();
        getLeavesHelper(this, result);
        return result;
    }

    private static void getLeavesHelper(
            ParquetColumnVector vector, List<ParquetColumnVector> coll) {
        if (vector.isPrimitive) {
            coll.add(vector);
        } else {
            for (ParquetColumnVector child : vector.children) {
                getLeavesHelper(child, coll);
            }
        }
    }

    /**
     * Assembles this column and calculate collection offsets recursively. This is a no-op for
     * primitive columns.
     */
    void assemble() {
        if (variantSchema != null) {
            assert column.variantFileType().isPresent();
            children.get(0).assemble();
            CastedRowColumnVector fileContent =
                    (CastedRowColumnVector)
                            createReadableColumnVector(
                                    column.variantFileType().get().getType(),
                                    children.get(0).getValueVector());
            if (fieldsToExtract == null) {
                PaimonShreddingUtils.assembleVariantBatch(fileContent, vector, variantSchema);
            } else {
                PaimonShreddingUtils.assembleVariantStructBatch(
                        fileContent, vector, variantSchema, fieldsToExtract, column.getType());
            }
            return;
        }

        if (column instanceof MapShreddingField) {
            assembleMapShredding((MapShreddingField) column);
            return;
        }

        // nothing to do if the column itself is missing
        if (vector.isAllNull()) {
            return;
        }

        DataTypeRoot type = column.getType().getTypeRoot();
        if (type == DataTypeRoot.ARRAY
                || type == DataTypeRoot.MAP
                || type == DataTypeRoot.MULTISET) {
            for (ParquetColumnVector child : children) {
                child.assemble();
            }
            assembleCollection();
        } else if (type == DataTypeRoot.ROW || type == DataTypeRoot.VARIANT) {
            for (ParquetColumnVector child : children) {
                child.assemble();
            }
            assembleStruct();
        }
    }

    /**
     * Resets this Parquet column vector, which includes resetting all the writable column vectors
     * (used to store values, definition levels, and repetition levels) for this and all its
     * children.
     */
    void reset() {
        // nothing to do if the column itself is missing
        if (vector.isAllNull()) {
            return;
        }

        vector.reset();
        if (repetitionLevels != null) {
            repetitionLevels.reset();
        }
        if (definitionLevels != null) {
            definitionLevels.reset();
        }
        for (ParquetColumnVector child : children) {
            child.reset();
        }
    }

    /** Returns the {@link ParquetField} of this column vector. */
    ParquetField getColumn() {
        return this.column;
    }

    /** Returns the writable column vector used to store values. */
    WritableColumnVector getValueVector() {
        return this.vector;
    }

    /** Returns the writable column vector used to store repetition levels. */
    WritableIntVector getRepetitionLevelVector() {
        return this.repetitionLevels;
    }

    /** Returns the writable column vector used to store definition levels. */
    WritableIntVector getDefinitionLevelVector() {
        return this.definitionLevels;
    }

    /** Returns the column reader for reading a Parquet column. */
    VectorizedColumnReader getColumnReader() {
        return this.columnReader;
    }

    /**
     * Sets the column vector to 'reader'. Note this can only be called on a primitive Parquet
     * column.
     */
    void setColumnReader(VectorizedColumnReader reader) {
        if (!isPrimitive) {
            throw new IllegalStateException("Can't set reader for non-primitive column");
        }
        this.columnReader = reader;
    }

    /** Assemble collections, e.g., array, map. */
    private void assembleCollection() {
        int maxDefinitionLevel = column.getDefinitionLevel();
        int maxElementRepetitionLevel = column.getRepetitionLevel();

        AbstractArrayBasedVector arrayVector = (AbstractArrayBasedVector) vector;

        // There are 4 cases when calculating definition levels:
        //   1. definitionLevel == maxDefinitionLevel
        //     ==> value is defined and not null
        //   2. definitionLevel == maxDefinitionLevel - 1
        //     ==> value is null
        //   3. definitionLevel < maxDefinitionLevel - 1
        //     ==> value doesn't exist since one of its optional parents is null
        //   4. definitionLevel > maxDefinitionLevel
        //     ==> value is a nested element within an array or map
        //
        // `i` is the index over all leaf elements of this array, while `offset` is the index over
        // all top-level elements of this array.
        int rowId = 0;
        for (int i = 0, offset = 0;
                i < definitionLevels.getElementsAppended();
                i = getNextCollectionStart(maxElementRepetitionLevel, i)) {
            arrayVector.reserve(rowId + 1);
            int definitionLevel = definitionLevels.getInt(i);
            if (definitionLevel <= maxDefinitionLevel) {
                // This means the value is not an array element, but a collection that is either
                // null or
                // empty. In this case, we should increase offset to skip it when returning an array
                // starting from the offset.
                //
                // For instance, considering an array of strings with 3 elements like the following:
                //  null, [], [a, b, c]
                // the child array (which is of String type) in this case will be:
                //  null:   1 1 0 0 0
                //  length: 0 0 1 1 1
                //  offset: 0 0 0 1 2
                // and the array itself will be:
                //  null:   1 0 0
                //  length: 0 0 3
                //  offset: 0 1 2
                //
                // It's important that for the third element `[a, b, c]`, the offset in the array
                // (not the elements) starts from 2 since otherwise we'd include the first & second
                // null
                // element from child array in the result.
                offset += 1;
            }
            if (definitionLevel <= maxDefinitionLevel - 1) {
                // Collection is null or one of its optional parents is null
                arrayVector.setNullAt(rowId++);
            } else if (definitionLevel == maxDefinitionLevel) {
                arrayVector.putOffsetLength(rowId, offset, 0);
                rowId++;
            } else if (definitionLevel > maxDefinitionLevel) {
                int length = getCollectionSize(maxElementRepetitionLevel, i);
                arrayVector.putOffsetLength(rowId, offset, length);
                offset += length;
                rowId++;
            }
        }
        vector.addElementsAppended(rowId);
    }

    private void assembleStruct() {
        int maxRepetitionLevel = column.getRepetitionLevel();
        int maxDefinitionLevel = column.getDefinitionLevel();

        vector.reserve(definitionLevels.getElementsAppended());

        int rowId = 0;
        boolean hasRepetitionLevels =
                repetitionLevels != null && repetitionLevels.getElementsAppended() > 0;
        for (int i = 0; i < definitionLevels.getElementsAppended(); i++) {
            // If repetition level > maxRepetitionLevel, the value is a nested element (e.g., an
            // array
            // element in struct<array<int>>), and we should skip the definition level since it
            // doesn't
            // represent with the struct.
            if (!hasRepetitionLevels || repetitionLevels.getInt(i) <= maxRepetitionLevel) {
                if (definitionLevels.getInt(i) <= maxDefinitionLevel - 1) {
                    // Struct is null
                    vector.setNullAt(rowId);
                    rowId++;
                } else if (definitionLevels.getInt(i) >= maxDefinitionLevel) {
                    rowId++;
                }
            }
        }
        vector.addElementsAppended(rowId);
    }

    private void assembleMapShredding(MapShreddingField mapField) {
        for (ParquetColumnVector child : children) {
            child.assemble();
        }

        HeapMapVector residualMapVector = (HeapMapVector) children.get(0).getValueVector();
        HeapMapVector targetMapVector = (HeapMapVector) vector;
        HeapBytesVector targetKeys = (HeapBytesVector) targetMapVector.getKeys();
        WritableColumnVector targetValues = (WritableColumnVector) targetMapVector.getValues();
        MapType mapType = (MapType) mapField.getType();

        int rowCount = residualMapVector.getElementsAppended();
        for (int rowId = 0; rowId < rowCount; rowId++) {
            InternalMap residualMap = residualMapVector.getMap(rowId);
            int sidecarValueCount = sidecarValueCount(rowId);
            if (residualMapVector.isNullAt(rowId) && sidecarValueCount == 0) {
                targetMapVector.appendNull();
                continue;
            }

            int residualSize = residualMapVector.isNullAt(rowId) ? 0 : residualMap.size();
            targetMapVector.appendArray(residualSize + sidecarValueCount);
            if (residualSize > 0) {
                appendResidualMap(residualMap, mapType, targetKeys, targetValues);
            }
            appendSidecars(rowId, mapField, mapType, targetKeys, targetValues);
        }
    }

    private int sidecarValueCount(int rowId) {
        int count = 0;
        for (int i = 1; i < children.size(); i++) {
            if (!children.get(i).getValueVector().isNullAt(rowId)) {
                count++;
            }
        }
        return count;
    }

    private void appendResidualMap(
            InternalMap residualMap,
            MapType mapType,
            HeapBytesVector targetKeys,
            WritableColumnVector targetValues) {
        InternalArray keyArray = residualMap.keyArray();
        InternalArray valueArray = residualMap.valueArray();
        for (int i = 0; i < residualMap.size(); i++) {
            appendString(targetKeys, keyArray.getString(i));
            appendInternalArrayValue(valueArray, i, mapType.getValueType(), targetValues);
        }
    }

    private void appendSidecars(
            int rowId,
            MapShreddingField mapField,
            MapType mapType,
            HeapBytesVector targetKeys,
            WritableColumnVector targetValues) {
        for (int i = 1; i < children.size(); i++) {
            WritableColumnVector sidecarVector = children.get(i).getValueVector();
            if (!sidecarVector.isNullAt(rowId)) {
                appendString(targetKeys, mapField.sidecarKeys().get(i - 1));
                appendColumnVectorValue(sidecarVector, rowId, mapType.getValueType(), targetValues);
            }
        }
    }

    private static void appendString(HeapBytesVector vector, BinaryString value) {
        byte[] bytes = value.toBytes();
        vector.appendByteArray(bytes, 0, bytes.length);
    }

    private static void appendColumnVectorValue(
            ColumnVector source, int sourcePos, DataType type, WritableColumnVector target) {
        if (source.isNullAt(sourcePos)) {
            target.appendNull();
            return;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                ((WritableBooleanVector) target)
                        .appendBoolean(((BooleanColumnVector) source).getBoolean(sourcePos));
                return;
            case TINYINT:
                ((WritableByteVector) target)
                        .appendByte(((ByteColumnVector) source).getByte(sourcePos));
                return;
            case SMALLINT:
                ((WritableShortVector) target)
                        .appendShort(((ShortColumnVector) source).getShort(sourcePos));
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ((WritableIntVector) target)
                        .appendInt(((IntColumnVector) source).getInt(sourcePos));
                return;
            case BIGINT:
                ((WritableLongVector) target)
                        .appendLong(((LongColumnVector) source).getLong(sourcePos));
                return;
            case FLOAT:
                ((WritableFloatVector) target)
                        .appendFloat(((FloatColumnVector) source).getFloat(sourcePos));
                return;
            case DOUBLE:
                ((WritableDoubleVector) target)
                        .appendDouble(((DoubleColumnVector) source).getDouble(sourcePos));
                return;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case BLOB:
                BytesColumnVector.Bytes bytes = ((BytesColumnVector) source).getBytes(sourcePos);
                ((WritableBytesVector) target).appendByteArray(bytes.data, bytes.offset, bytes.len);
                return;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                if (source instanceof DecimalColumnVector) {
                    appendDecimal(
                            ((DecimalColumnVector) source)
                                    .getDecimal(
                                            sourcePos,
                                            decimalType.getPrecision(),
                                            decimalType.getScale()),
                            decimalType,
                            target);
                } else if (decimalType.getPrecision() <= 9) {
                    ((WritableIntVector) target)
                            .appendInt(((IntColumnVector) source).getInt(sourcePos));
                } else if (decimalType.getPrecision() <= 18) {
                    ((WritableLongVector) target)
                            .appendLong(((LongColumnVector) source).getLong(sourcePos));
                } else {
                    BytesColumnVector.Bytes decimalBytes =
                            ((BytesColumnVector) source).getBytes(sourcePos);
                    ((WritableBytesVector) target)
                            .appendByteArray(
                                    decimalBytes.data, decimalBytes.offset, decimalBytes.len);
                }
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int precision = DataTypeChecks.getPrecision(type);
                if (source instanceof TimestampColumnVector) {
                    appendTimestamp(
                            type,
                            ((TimestampColumnVector) source).getTimestamp(sourcePos, precision),
                            target);
                } else if (precision <= 6 && source instanceof LongColumnVector) {
                    ((WritableLongVector) target)
                            .appendLong(((LongColumnVector) source).getLong(sourcePos));
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported timestamp vector for map shredding: " + source);
                }
                return;
            case ARRAY:
                appendInternalValue(((ArrayColumnVector) source).getArray(sourcePos), type, target);
                return;
            case MAP:
            case MULTISET:
                appendInternalValue(((MapColumnVector) source).getMap(sourcePos), type, target);
                return;
            case ROW:
                appendInternalValue(((RowColumnVector) source).getRow(sourcePos), type, target);
                return;
            case VARIANT:
                InternalRow variantRow = ((RowColumnVector) source).getRow(sourcePos);
                appendInternalValue(
                        new GenericVariant(variantRow.getBinary(0), variantRow.getBinary(1)),
                        type,
                        target);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported map shredding value type: " + type);
        }
    }

    private static void appendInternalArrayValue(
            InternalArray array, int sourcePos, DataType type, WritableColumnVector target) {
        if (array.isNullAt(sourcePos)) {
            target.appendNull();
            return;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                ((WritableBooleanVector) target).appendBoolean(array.getBoolean(sourcePos));
                return;
            case TINYINT:
                ((WritableByteVector) target).appendByte(array.getByte(sourcePos));
                return;
            case SMALLINT:
                ((WritableShortVector) target).appendShort(array.getShort(sourcePos));
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ((WritableIntVector) target).appendInt(array.getInt(sourcePos));
                return;
            case BIGINT:
                ((WritableLongVector) target).appendLong(array.getLong(sourcePos));
                return;
            case FLOAT:
                ((WritableFloatVector) target).appendFloat(array.getFloat(sourcePos));
                return;
            case DOUBLE:
                ((WritableDoubleVector) target).appendDouble(array.getDouble(sourcePos));
                return;
            case CHAR:
            case VARCHAR:
                appendString((HeapBytesVector) target, array.getString(sourcePos));
                return;
            case BINARY:
            case VARBINARY:
            case BLOB:
                byte[] bytes = array.getBinary(sourcePos);
                ((WritableBytesVector) target).appendByteArray(bytes, 0, bytes.length);
                return;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                appendDecimal(
                        array.getDecimal(
                                sourcePos, decimalType.getPrecision(), decimalType.getScale()),
                        decimalType,
                        target);
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                appendTimestamp(
                        type,
                        array.getTimestamp(sourcePos, DataTypeChecks.getPrecision(type)),
                        target);
                return;
            case ARRAY:
                appendInternalValue(array.getArray(sourcePos), type, target);
                return;
            case MAP:
            case MULTISET:
                appendInternalValue(array.getMap(sourcePos), type, target);
                return;
            case ROW:
                appendInternalValue(
                        array.getRow(sourcePos, ((RowType) type).getFieldCount()), type, target);
                return;
            case VARIANT:
                appendInternalValue(array.getVariant(sourcePos), type, target);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported map shredding value type: " + type);
        }
    }

    private static void appendInternalValue(
            Object value, DataType type, WritableColumnVector target) {
        if (value == null) {
            target.appendNull();
            return;
        }

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                ((WritableBooleanVector) target).appendBoolean((boolean) value);
                return;
            case TINYINT:
                ((WritableByteVector) target).appendByte((byte) value);
                return;
            case SMALLINT:
                ((WritableShortVector) target).appendShort((short) value);
                return;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ((WritableIntVector) target).appendInt((int) value);
                return;
            case BIGINT:
                ((WritableLongVector) target).appendLong((long) value);
                return;
            case FLOAT:
                ((WritableFloatVector) target).appendFloat((float) value);
                return;
            case DOUBLE:
                ((WritableDoubleVector) target).appendDouble((double) value);
                return;
            case CHAR:
            case VARCHAR:
                appendString((HeapBytesVector) target, (BinaryString) value);
                return;
            case BINARY:
            case VARBINARY:
            case BLOB:
                byte[] bytes = (byte[]) value;
                ((WritableBytesVector) target).appendByteArray(bytes, 0, bytes.length);
                return;
            case DECIMAL:
                appendDecimal((Decimal) value, (DecimalType) type, target);
                return;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                appendTimestamp(type, (Timestamp) value, target);
                return;
            case ARRAY:
                appendInternalArray(
                        (InternalArray) value, (ArrayType) type, (HeapArrayVector) target);
                return;
            case MAP:
                appendInternalMap((InternalMap) value, (MapType) type, (HeapMapVector) target);
                return;
            case MULTISET:
                appendInternalMap(
                        (InternalMap) value,
                        new MapType(((MultisetType) type).getElementType(), new IntType(false)),
                        (HeapMapVector) target);
                return;
            case ROW:
                appendInternalRow((InternalRow) value, (RowType) type, (HeapRowVector) target);
                return;
            case VARIANT:
                appendVariant((Variant) value, (HeapRowVector) target);
                return;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported map shredding value type: " + type);
        }
    }

    private static void appendInternalArray(
            InternalArray array, ArrayType type, HeapArrayVector target) {
        WritableColumnVector child = (WritableColumnVector) target.getChildren()[0];
        target.appendArray(array.size());
        for (int i = 0; i < array.size(); i++) {
            appendInternalArrayValue(array, i, type.getElementType(), child);
        }
    }

    private static void appendInternalMap(InternalMap map, MapType type, HeapMapVector target) {
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        WritableColumnVector keyVector = (WritableColumnVector) target.getKeys();
        WritableColumnVector valueVector = (WritableColumnVector) target.getValues();
        target.appendArray(map.size());
        for (int i = 0; i < map.size(); i++) {
            appendInternalArrayValue(keyArray, i, type.getKeyType(), keyVector);
            appendInternalArrayValue(valueArray, i, type.getValueType(), valueVector);
        }
    }

    private static void appendInternalRow(InternalRow row, RowType type, HeapRowVector target) {
        target.appendRow();
        for (int i = 0; i < type.getFieldCount(); i++) {
            WritableColumnVector child = (WritableColumnVector) target.getChildren()[i];
            DataType fieldType = type.getTypeAt(i);
            if (row.isNullAt(i)) {
                child.appendNull();
            } else {
                appendInternalValue(
                        InternalRow.createFieldGetter(fieldType, i).getFieldOrNull(row),
                        fieldType,
                        child);
            }
        }
    }

    private static void appendVariant(Variant variant, HeapRowVector target) {
        target.appendRow();
        byte[] value = variant.value();
        byte[] metadata = variant.metadata();
        ((WritableBytesVector) target.getChildren()[0]).appendByteArray(value, 0, value.length);
        ((WritableBytesVector) target.getChildren()[1])
                .appendByteArray(metadata, 0, metadata.length);
    }

    private static void appendDecimal(
            Decimal decimal, DecimalType type, WritableColumnVector target) {
        if (decimal == null) {
            target.appendNull();
        } else if (type.getPrecision() <= 9) {
            ((WritableIntVector) target).appendInt((int) decimal.toUnscaledLong());
        } else if (type.getPrecision() <= 18) {
            ((WritableLongVector) target).appendLong(decimal.toUnscaledLong());
        } else {
            byte[] bytes = decimal.toUnscaledBytes();
            ((WritableBytesVector) target).appendByteArray(bytes, 0, bytes.length);
        }
    }

    private static void appendTimestamp(
            DataType type, Timestamp timestamp, WritableColumnVector target) {
        if (timestamp == null) {
            target.appendNull();
        } else if (target instanceof WritableTimestampVector) {
            ((WritableTimestampVector) target).appendTimestamp(timestamp);
        } else if (DataTypeChecks.getPrecision(type) <= 3) {
            ((WritableLongVector) target).appendLong(timestamp.getMillisecond());
        } else if (DataTypeChecks.getPrecision(type) <= 6) {
            ((WritableLongVector) target).appendLong(timestamp.toMicros());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported timestamp vector for map shredding: " + target);
        }
    }

    /**
     * For a collection (i.e., array or map) element at index 'idx', returns the starting index of
     * the next collection after it.
     *
     * @param maxRepetitionLevel the maximum repetition level for the elements in this collection
     * @param idx the index of this collection in the Parquet column
     * @return the starting index of the next collection
     */
    private int getNextCollectionStart(int maxRepetitionLevel, int idx) {
        idx += 1;
        for (; idx < repetitionLevels.getElementsAppended(); idx++) {
            if (repetitionLevels.getInt(idx) <= maxRepetitionLevel) {
                break;
            }
        }
        return idx;
    }

    /**
     * Gets the size of a collection (i.e., array or map) element, starting at 'idx'.
     *
     * @param maxRepetitionLevel the maximum repetition level for the elements in this collection
     * @param idx the index of this collection in the Parquet column
     * @return the size of this collection
     */
    private int getCollectionSize(int maxRepetitionLevel, int idx) {
        int size = 1;
        for (idx += 1; idx < repetitionLevels.getElementsAppended(); idx++) {
            if (repetitionLevels.getInt(idx) <= maxRepetitionLevel) {
                break;
            } else if (repetitionLevels.getInt(idx) <= maxRepetitionLevel + 1) {
                // Only count elements which belong to the current collection
                // For instance, suppose we have the following Parquet schema:
                //
                // message schema {                        max rl   max dl
                //   optional group col (LIST) {              0        1
                //     repeated group list {                  1        2
                //       optional group element (LIST) {      1        3
                //         repeated group list {              2        4
                //           required int32 element;          2        4
                //         }
                //       }
                //     }
                //   }
                // }
                //
                // For a list such as: [[[0, 1], [2, 3]], [[4, 5], [6, 7]]], the repetition &
                // definition
                // levels would be:
                //
                // repetition levels: [0, 2, 1, 2, 0, 2, 1, 2]
                // definition levels: [2, 2, 2, 2, 2, 2, 2, 2]
                //
                // When calculating collection size for the outer array, we should only count
                // repetition
                // levels whose value is <= 1 (which is the max repetition level for the inner
                // array)
                size++;
            }
        }
        return size;
    }
}
