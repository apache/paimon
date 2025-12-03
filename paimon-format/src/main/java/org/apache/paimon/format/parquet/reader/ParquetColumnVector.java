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

import org.apache.paimon.data.columnar.heap.AbstractArrayBasedVector;
import org.apache.paimon.data.columnar.heap.CastedRowColumnVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.variant.PaimonShreddingUtils;
import org.apache.paimon.data.variant.PaimonShreddingUtils.FieldToExtract;
import org.apache.paimon.data.variant.VariantSchema;
import org.apache.paimon.format.parquet.type.ParquetField;
import org.apache.paimon.format.parquet.type.ParquetGroupField;
import org.apache.paimon.types.DataTypeRoot;
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
                    PaimonShreddingUtils.getFieldsToExtract(column.variantFields(), variantSchema);
            repetitionLevels = contentVector.repetitionLevels;
            definitionLevels = contentVector.definitionLevels;
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
