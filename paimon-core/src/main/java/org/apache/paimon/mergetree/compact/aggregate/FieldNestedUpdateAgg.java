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

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Used to update a field which representing a nested table. The data type of nested table field is
 * {@code ARRAY<ROW>}.
 */
public class FieldNestedUpdateAgg extends FieldAggregator {

    public static final String NAME = "nested-update";

    private final InternalArray.ElementGetter rowGetter;
    private final List<InternalRow.FieldGetter> keyGetters;

    public FieldNestedUpdateAgg(ArrayType dataType, List<String> nestedKeys) {
        super(dataType);
        RowType rowType = (RowType) dataType.getElementType();
        this.rowGetter = InternalArray.createElementGetter(rowType);

        this.keyGetters = new ArrayList<>(nestedKeys.size());
        List<DataField> dataFields = rowType.getFields();
        for (int i = 0; i < dataFields.size(); i++) {
            DataField dataField = dataFields.get(i);
            if (nestedKeys.contains(dataField.name())) {
                keyGetters.add(InternalRow.createFieldGetter(dataField.type(), i));
            }
        }
        checkArgument(keyGetters.size() == nestedKeys.size(), "You have set wrong nested keys.");
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return inputField == null ? accumulator : inputField;
        }

        InternalArray acc = (InternalArray) accumulator;
        Object[] nestedRows = new Object[acc.size() + 1];
        for (int i = 0; i < acc.size(); i++) {
            nestedRows[i] = rowGetter.getElementOrNull(acc, i);
        }

        InternalArray input = (InternalArray) inputField;
        checkArgument(input.size() == 1);
        nestedRows[acc.size()] = rowGetter.getElementOrNull(input, 0);

        return new GenericArray(nestedRows);
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null || retractField == null) {
            return null;
        }

        InternalArray array = (InternalArray) retractField;
        checkArgument(array.size() == 1);
        Object retract = rowGetter.getElementOrNull(array, 0);
        List<Object> keys = getKeys(retract);

        Map<List<Object>, Object> unnested = unnest(accumulator);
        unnested.remove(keys);

        return new GenericArray(unnested.values().toArray(new Object[0]));
    }

    private Map<List<Object>, Object> unnest(Object accumulator) {
        Map<List<Object>, Object> unnested = new HashMap<>();
        InternalArray array = (InternalArray) accumulator;
        for (int i = 0; i < array.size(); i++) {
            Object row = rowGetter.getElementOrNull(array, i);
            unnested.put(getKeys(rowGetter.getElementOrNull(array, i)), row);
        }
        return unnested;
    }

    private List<Object> getKeys(Object row) {
        return keyGetters.stream()
                .map(g -> g.getFieldOrNull((InternalRow) row))
                .collect(Collectors.toList());
    }
}
