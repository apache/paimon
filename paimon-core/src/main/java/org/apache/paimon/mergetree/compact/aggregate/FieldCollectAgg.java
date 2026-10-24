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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;

/** Collect elements into an ARRAY. */
public class FieldCollectAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final boolean distinct;
    private final int countLimit;
    private final InternalArray.ElementGetter elementGetter;
    @Nullable private final BiFunction<Object, Object, Boolean> equaliser;

    public FieldCollectAgg(String name, ArrayType dataType, boolean distinct, int countLimit) {
        super(name, dataType);
        this.distinct = distinct;
        this.countLimit = countLimit;
        this.elementGetter = InternalArray.createElementGetter(dataType.getElementType());

        // The equaliser is built from the element type alone: retract() needs it whether or not
        // the array is distinct, and agg() gates de-duplication on 'distinct' separately.
        if (needsEqualiser(dataType.getElementType())) {
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

    /**
     * Whether elements of this type need the generated equaliser rather than {@link Object#equals}.
     *
     * <p>Constructed types need it because two rows holding the same values are not necessarily
     * equal objects. Binary types need it for a blunter reason: an element of {@code BINARY},
     * {@code VARBINARY}, {@code GEOMETRY} or {@code GEOGRAPHY} is a {@code byte[]}, which inherits
     * identity equality from {@link Object}, so two arrays with the same content never compare
     * equal and never share a hash bucket.
     */
    private static boolean needsEqualiser(DataType elementType) {
        return elementType.is(DataTypeFamily.CONSTRUCTED) || BinaryMapKeys.isBinary(elementType);
    }

    @Override
    public Object aggReversed(Object accumulator, Object inputField) {
        return agg(inputField, true, accumulator, false);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        return agg(accumulator, false, inputField, true);
    }

    private Object agg(
            Object accumulator,
            boolean accumulatorHasDuplicate,
            Object inputField,
            boolean inputHasDuplicate) {
        InternalArray accumulatorArray = (InternalArray) accumulator;
        InternalArray inputArray = (InternalArray) inputField;

        if (accumulatorArray == null && inputArray == null) {
            return null;
        } else if (inputArray == null
                && !distinct
                && accumulatorArray.size() <= Math.abs(countLimit)) {
            return accumulatorArray;
        } else if (accumulatorArray == null
                && !distinct
                && inputArray.size() <= Math.abs(countLimit)) {
            return inputArray;
        }

        Collection<Object> collection =
                distinct && equaliser == null ? new LinkedHashSet<>() : new ArrayList<>();
        if (countLimit >= 0) {
            collect(
                    collection,
                    accumulatorArray,
                    checkDuplicateUntil(accumulatorHasDuplicate, collection));
            collect(collection, inputArray, checkDuplicateUntil(inputHasDuplicate, collection));
            return new GenericArray(collection.toArray());
        } else {
            collect(collection, inputArray, checkDuplicateUntil(inputHasDuplicate, collection));
            collect(
                    collection,
                    accumulatorArray,
                    checkDuplicateUntil(accumulatorHasDuplicate, collection));
            List<Object> result =
                    collection instanceof List
                            ? (List<Object>) collection
                            : new ArrayList<>(collection);
            Collections.reverse(result);
            return new GenericArray(result.toArray());
        }
    }

    private int checkDuplicateUntil(boolean hasDuplicate, Collection<Object> collection) {
        if (distinct && equaliser != null) {
            if (hasDuplicate) {
                return Integer.MAX_VALUE;
            } else {
                return collection.size();
            }
        } else {
            return 0;
        }
    }

    private void collect(
            Collection<Object> collection, @Nullable InternalArray array, int checkDuplicateUntil) {
        if (array == null) {
            return;
        }

        long limit = Math.abs((long) countLimit);
        for (int i = 0; i < array.size() && collection.size() < limit; i++) {
            Object element =
                    elementGetter.getElementOrNull(
                            array, countLimit < 0 ? array.size() - 1 - i : i);
            if (!contains(collection, element, checkDuplicateUntil)) {
                collection.add(element);
            }
        }
    }

    private boolean contains(
            Collection<Object> list, @Nullable Object element, int checkDuplicateUntil) {
        if (checkDuplicateUntil <= 0) {
            return false;
        }

        for (Object o : list) {
            if (equals(o, element)) {
                return true;
            }
            checkDuplicateUntil--;
            if (checkDuplicateUntil <= 0) {
                break;
            }
        }
        return false;
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        // it's hard to mark the input is retracted without accumulator
        if (accumulator == null) {
            return null;
        }

        // nothing to be retracted
        if (retractField == null) {
            return accumulator;
        }
        InternalArray retract = (InternalArray) retractField;
        if (retract.size() == 0) {
            return accumulator;
        }

        List<Object> retractedElements = new ArrayList<>();
        for (int i = 0; i < retract.size(); i++) {
            retractedElements.add(elementGetter.getElementOrNull(retract, i));
        }

        InternalArray acc = (InternalArray) accumulator;
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
