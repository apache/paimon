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

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;

/** Collect elements into an ARRAY. */
public class FieldCollectAgg extends FieldAggregator {

    public static final String NAME = "collect";

    private final boolean distinct;
    private final InternalArray.ElementGetter elementGetter;
    @Nullable private final BiFunction<Object, Object, Boolean> equaliser;

    public FieldCollectAgg(ArrayType dataType, boolean distinct) {
        super(dataType);
        this.distinct = distinct;
        this.elementGetter = InternalArray.createElementGetter(dataType.getElementType());

        if (distinct
                && dataType.getElementType()
                        .getTypeRoot()
                        .getFamilies()
                        .contains(DataTypeFamily.CONSTRUCTED)) {
            DataType elementType = dataType.getElementType();
            List<DataType> fieldTypes =
                    elementType instanceof RowType
                            ? ((RowType) elementType).getFieldTypes()
                            : Collections.singletonList(elementType);
            RecordEqualiser elementEqualiser = newRecordEqualiser(fieldTypes);
            this.equaliser =
                    (o1, o2) -> {
                        InternalRow row1, row2;
                        if (elementType instanceof RowType) {
                            row1 = (InternalRow) o1;
                            row2 = (InternalRow) o2;
                        } else {
                            row1 = GenericRow.of(o1);
                            row2 = GenericRow.of(o2);
                        }
                        return elementEqualiser.equals(row1, row2);
                    };
        } else {
            equaliser = null;
        }
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null && inputField == null) {
            return null;
        }

        if ((accumulator == null || inputField == null) && !distinct) {
            return accumulator == null ? inputField : accumulator;
        }

        if (equaliser != null) {
            List<Object> collection = new ArrayList<>();
            collectWithEqualiser(collection, accumulator);
            collectWithEqualiser(collection, inputField);
            return new GenericArray(collection.toArray());
        } else {
            Collection<Object> collection = distinct ? new HashSet<>() : new ArrayList<>();
            collect(collection, accumulator);
            collect(collection, inputField);
            return new GenericArray(collection.toArray());
        }
    }

    private void collect(Collection<Object> collection, @Nullable Object data) {
        if (data == null) {
            return;
        }

        InternalArray array = (InternalArray) data;
        for (int i = 0; i < array.size(); i++) {
            collection.add(elementGetter.getElementOrNull(array, i));
        }
    }

    private void collectWithEqualiser(List<Object> list, Object data) {
        if (data == null) {
            return;
        }

        InternalArray array = (InternalArray) data;
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            if (!contains(list, element)) {
                list.add(element);
            }
        }
    }

    private boolean contains(List<Object> list, @Nullable Object element) {
        if (element == null) {
            return list.contains(null);
        }

        for (Object o : list) {
            if (equaliser.apply(o, element)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null) {
            return null;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray retract = (InternalArray) retractField;

        List<Object> retractedElements = new ArrayList<>();
        for (int i = 0; i < retract.size(); i++) {
            retractedElements.add(elementGetter.getElementOrNull(retract, i));
        }

        List<Object> accElements = new ArrayList<>();
        for (int i = 0; i < acc.size(); i++) {
            Object candidate = elementGetter.getElementOrNull(acc, i);
            if (!retract(retractedElements, candidate)) {
                accElements.add(candidate);
            }
        }
        return new GenericArray(accElements.toArray());
    }

    private boolean retract(List<Object> list, Object element) {
        Iterator<Object> iterator = list.iterator();
        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (equals(o, element)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private boolean equals(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else {
            return equaliser == null ? a.equals(b) : equaliser.apply(a, b);
        }
    }
}
