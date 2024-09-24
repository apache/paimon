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

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Used to update a field which representing a nested table. The data type of nested table field is
 * {@code ARRAY<ROW>}.
 */
public class FieldNestedUpdateAgg extends FieldAggregator {

    public static final String NAME = "nested_update";

    private static final long serialVersionUID = 1L;

    private final int nestedFields;

    @Nullable private final Projection keyProjection;
    @Nullable private final RecordEqualiser elementEqualiser;

    public FieldNestedUpdateAgg(ArrayType dataType, List<String> nestedKey) {
        super(dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        if (nestedKey.isEmpty()) {
            this.keyProjection = null;
            this.elementEqualiser = newRecordEqualiser(nestedType.getFieldTypes());
        } else {
            this.keyProjection = newProjection(nestedType, nestedKey);
            this.elementEqualiser = null;
        }
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null) {
            return inputField;
        }
        if (inputField == null) {
            return accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray input = (InternalArray) inputField;

        List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
        addNonNullRows(acc, rows);
        addNonNullRows(input, rows);

        if (keyProjection != null) {
            Map<BinaryRow, InternalRow> map = new HashMap<>();
            for (InternalRow row : rows) {
                BinaryRow key = keyProjection.apply(row).copy();
                map.put(key, row);
            }

            rows = new ArrayList<>(map.values());
        }

        return new GenericArray(rows.toArray());
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
                map.put(keyProjection.apply(row).copy(), row);
            }

            for (int i = 0; i < retract.size(); i++) {
                if (retract.isNullAt(i)) {
                    continue;
                }
                map.remove(keyProjection.apply(retract.getRow(i, nestedFields)));
            }

            return new GenericArray(new ArrayList<>(map.values()).toArray());
        }
    }

    private void addNonNullRows(InternalArray array, List<InternalRow> rows) {
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }
            rows.add(array.getRow(i, nestedFields));
        }
    }
}
