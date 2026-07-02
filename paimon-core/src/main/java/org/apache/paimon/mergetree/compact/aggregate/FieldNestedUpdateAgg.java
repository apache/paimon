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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Used to update a field which representing a nested table. The data type of nested table field is
 * {@code ARRAY<ROW>}.
 */
public class FieldNestedUpdateAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int nestedFields;

    @Nullable private final Projection keyProjection;
    @Nullable private final RecordEqualiser elementEqualiser;

    @Nullable private final Projection sequenceProjection;
    @Nullable private final RecordComparator sequenceComparator;
    private final boolean hasSequenceField;

    private final CoreOptions.NestedKeyNullStrategy nestedKeyNullStrategy;
    private final int countLimit;

    public FieldNestedUpdateAgg(
            String name,
            ArrayType dataType,
            List<String> nestedKey,
            CoreOptions.NestedKeyNullStrategy nestedKeyNullStrategy,
            List<String> nestedSequenceField,
            int countLimit) {
        super(name, dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        if (nestedKey.isEmpty()) {
            this.keyProjection = null;
            this.elementEqualiser = newRecordEqualiser(nestedType.getFieldTypes());
        } else {
            this.keyProjection = newProjection(nestedType, nestedKey);
            this.elementEqualiser = null;
        }

        checkArgument(
                nestedKeyNullStrategy == null || this.keyProjection != null,
                "Option 'fields.<field-name>.nested-key-null-strategy' requires "
                        + "'fields.<field-name>.nested-key' to be configured.");

        // Default to MERGE to preserve the previous behavior.
        this.nestedKeyNullStrategy =
                nestedKeyNullStrategy == null
                        ? CoreOptions.NestedKeyNullStrategy.MERGE
                        : nestedKeyNullStrategy;

        // If nestedSequenceField is set, we need to compare sequence fields to determine
        // whether to update. Only update when the new sequence is greater than the old one.
        if (!nestedSequenceField.isEmpty()) {
            checkArgument(
                    this.keyProjection != null,
                    "Option 'fields.<field-name>.nested-sequence-field' requires "
                            + "'fields.<field-name>.nested-key' to be configured.");
            this.sequenceProjection = newProjection(nestedType, nestedSequenceField);
            this.hasSequenceField = true;

            // Extract the data types of the sequence fields to generate a native record comparator
            int sequenceFields = nestedSequenceField.size();
            List<DataType> seqTypes = new ArrayList<>(sequenceFields);
            int[] sortFields = new int[sequenceFields];
            for (int i = 0; i < sequenceFields; i++) {
                String fieldName = nestedSequenceField.get(i);
                seqTypes.add(nestedType.getTypeAt(nestedType.getFieldIndex(fieldName)));
                sortFields[i] = i;
            }
            this.sequenceComparator = newRecordComparator(seqTypes, sortFields);
        } else {
            this.sequenceProjection = null;
            this.sequenceComparator = null;
            this.hasSequenceField = false;
        }

        // If deduplicate key is set, we don't guarantee that the result is exactly right
        this.countLimit = countLimit;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (inputField == null) {
            return accumulator;
        }

        InternalArray input = (InternalArray) inputField;

        if (keyProjection == null) {
            if (accumulator == null) {
                List<InternalRow> rows = new ArrayList<>(input.size());
                addNonNullRows(input, rows, countLimit);
                return new GenericArray(rows.toArray());
            }

            InternalArray acc = (InternalArray) accumulator;
            if (acc.size() >= countLimit) {
                return accumulator;
            }

            int remainCount = countLimit - acc.size();

            List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
            addNonNullRows(acc, rows);
            addNonNullRows(input, rows, remainCount);
            return new GenericArray(rows.toArray());
        }

        Map<BinaryRow, InternalRow> map = new HashMap<>();
        if (accumulator != null) {
            addNestedRows((InternalArray) accumulator, map, false);
        }
        addNestedRows(input, map, true);
        return new GenericArray(new ArrayList<>(map.values()).toArray());
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null || retractField == null) {
            return accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray retract = (InternalArray) retractField;

        if (keyProjection == null) {
            checkNotNull(elementEqualiser);
            List<InternalRow> rows = new ArrayList<>();
            addNonNullRows(acc, rows);
            for (int i = 0; i < retract.size(); i++) {
                if (retract.isNullAt(i)) {
                    continue;
                }
                InternalRow retractRow = retract.getRow(i, nestedFields);
                rows.removeIf(next -> elementEqualiser.equals(next, retractRow));
            }
            return new GenericArray(rows.toArray());
        } else {
            Map<BinaryRow, InternalRow> map = new HashMap<>();

            for (int i = 0; i < acc.size(); i++) {
                if (acc.isNullAt(i)) {
                    continue;
                }
                InternalRow row = acc.getRow(i, nestedFields);
                BinaryRow key = keyProjection.apply(row).copy();
                if (!applyNestedKeyNullStrategy(key)) {
                    continue;
                }
                map.put(key, row);
            }

            for (int i = 0; i < retract.size(); i++) {
                if (retract.isNullAt(i)) {
                    continue;
                }
                BinaryRow key = keyProjection.apply(retract.getRow(i, nestedFields)).copy();
                if (!applyNestedKeyNullStrategy(key)) {
                    continue;
                }
                map.remove(key);
            }

            return new GenericArray(new ArrayList<>(map.values()).toArray());
        }
    }

    private int compareSequence(InternalRow newRow, InternalRow oldRow) {
        checkNotNull(
                sequenceComparator,
                "sequenceComparator should not be null when hasSequenceField is true.");
        checkNotNull(
                sequenceProjection,
                "sequenceProjection should not be null when hasSequenceField is true.");

        // Project the rows into sub-rows containing only sequence fields
        BinaryRow newSeqRow = sequenceProjection.apply(newRow).copy();
        BinaryRow oldSeqRow = sequenceProjection.apply(oldRow).copy();

        // Triggers native CodeGen comparison (Nulls First by default)
        return sequenceComparator.compare(newSeqRow, oldSeqRow);
    }

    private void addNonNullRows(InternalArray array, List<InternalRow> rows) {
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }
            rows.add(array.getRow(i, nestedFields));
        }
    }

    private void addNonNullRows(InternalArray array, List<InternalRow> rows, int remainSize) {
        int count = 0;
        for (int i = 0; i < array.size(); i++) {
            if (count >= remainSize) {
                return;
            }
            if (array.isNullAt(i)) {
                continue;
            }
            rows.add(array.getRow(i, nestedFields));
            count++;
        }
    }

    private void addNestedRows(
            InternalArray array, Map<BinaryRow, InternalRow> rows, boolean limitNewKeys) {
        checkNotNull(keyProjection);

        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }

            InternalRow row = array.getRow(i, nestedFields);
            BinaryRow key = keyProjection.apply(row).copy();

            if (!applyNestedKeyNullStrategy(key)) {
                continue;
            }

            InternalRow existing = rows.get(key);
            if (existing != null) {
                if (!hasSequenceField || compareSequence(row, existing) >= 0) {
                    rows.put(key, row);
                }
            } else if (!limitNewKeys || rows.size() < countLimit) {
                rows.put(key, row);
            }
        }
    }

    private boolean applyNestedKeyNullStrategy(BinaryRow key) {
        if (!key.anyNull()) {
            // The nested-key satisfies primary key semantics.
            return true;
        }
        switch (nestedKeyNullStrategy) {
            case MERGE:
                // Preserve the previous behavior.
                return true;
            case IGNORE:
                return false;
            case ERROR:
                throw new IllegalArgumentException(
                        "Nested key contains null values. Primary key fields must not be null.");
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported nested-key-null-strategy '%s'. Supported values are: %s.",
                                nestedKeyNullStrategy,
                                Arrays.toString(CoreOptions.NestedKeyNullStrategy.values())));
        }
    }
}
