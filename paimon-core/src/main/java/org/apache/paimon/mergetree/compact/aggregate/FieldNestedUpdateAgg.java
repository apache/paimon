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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Used to update a field which representing a nested table. The data type of nested table field is
 * {@code ARRAY<ROW>}.
 */
public class FieldNestedUpdateAgg extends FieldAggregator {

    public static final String NAME = "nested-update";

    private final BiFunction<InternalArray, Integer, InternalRow> rowGetter;
    private final List<InternalRow.FieldGetter> keyGetters;

    public FieldNestedUpdateAgg(ArrayType dataType, List<String> nestedKeys) {
        super(dataType);
        RowType rowType = (RowType) dataType.getElementType();

        InternalArray.ElementGetter objectGetter = InternalArray.createElementGetter(rowType);
        this.rowGetter = (array, pos) -> (InternalRow) objectGetter.getElementOrNull(array, pos);

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
            return accumulator == null ? inputField : accumulator;
        }

        Map<List<Object>, InternalRow> unnestedAcc = unnest(accumulator);
        InternalArray inputs = (InternalArray) inputField;

        for (int i = 0; i < inputs.size(); i++) {
            InternalRow row = rowGetter.apply(inputs, i);
            List<Object> keys = getKeys(row);
            // update by nested keys
            unnestedAcc.put(keys, row);
        }

        return new GenericArray(unnestedAcc.values().toArray());
    }

    private Map<List<Object>, InternalRow> unnest(@Nullable Object accumulator) {
        Map<List<Object>, InternalRow> unnested = new HashMap<>();
        if (accumulator != null) {
            InternalArray array = (InternalArray) accumulator;
            for (int i = 0; i < array.size(); i++) {
                InternalRow row = rowGetter.apply(array, i);
                List<Object> keys = getKeys(row);
                unnested.put(keys, row);
            }
        }

        return unnested;
    }

    private List<Object> getKeys(InternalRow row) {
        return keyGetters.stream().map(g -> g.getFieldOrNull(row)).collect(Collectors.toList());
    }
}
