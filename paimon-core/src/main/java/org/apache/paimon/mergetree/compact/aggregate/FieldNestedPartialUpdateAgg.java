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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Used to partial update a field which representing a nested table. The data type of nested table
 * field is {@code ARRAY<ROW>}.
 */
public class FieldNestedPartialUpdateAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int nestedFields;
    private final Projection keyProjection;
    private final FieldGetter[] fieldGetters;

    public FieldNestedPartialUpdateAgg(String name, ArrayType dataType, List<String> nestedKey) {
        super(name, dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        checkArgument(!nestedKey.isEmpty());
        this.keyProjection = newProjection(nestedType, nestedKey);
        this.fieldGetters = new FieldGetter[nestedFields];
        for (int i = 0; i < nestedFields; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(nestedType.getTypeAt(i), i);
        }
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray input = (InternalArray) inputField;

        List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
        addNonNullRows(acc, rows);
        addNonNullRows(input, rows);

        if (keyProjection != null) {
            Map<BinaryRow, GenericRow> map = new HashMap<>();
            for (InternalRow row : rows) {
                BinaryRow key = keyProjection.apply(row).copy();
                GenericRow toUpdate = map.computeIfAbsent(key, k -> new GenericRow(nestedFields));
                partialUpdate(toUpdate, row);
            }

            rows = new ArrayList<>(map.values());
        }

        return new GenericArray(rows.toArray());
    }

    private void addNonNullRows(InternalArray array, List<InternalRow> rows) {
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue;
            }
            rows.add(array.getRow(i, nestedFields));
        }
    }

    private void partialUpdate(GenericRow toUpdate, InternalRow input) {
        for (int i = 0; i < fieldGetters.length; i++) {
            FieldGetter fieldGetter = fieldGetters[i];
            Object field = fieldGetter.getFieldOrNull(input);
            if (field != null) {
                toUpdate.setField(i, field);
            }
        }
    }
}
