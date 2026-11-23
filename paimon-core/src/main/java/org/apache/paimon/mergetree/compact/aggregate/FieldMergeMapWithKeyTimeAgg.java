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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.HashMap;
import java.util.Map;

/** Aggregator for merging maps with key and timestamp. */
public class FieldMergeMapWithKeyTimeAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter keyGetter;
    private final InternalArray.ElementGetter valueGetter;
    private final int timestampFieldIndex;
    private final InternalRow.FieldGetter timestampFieldGetter;
    private final DataTypeRoot timestampFieldTypeRoot;
    private final boolean overwriteOnEqualTs;

    /** See {@link BinaryMapKeys}: a binary key has no value equality of its own. */
    private final boolean binaryKey;

    public FieldMergeMapWithKeyTimeAgg(String name, MapType dataType, int timestampFieldIndex) {
        this(name, dataType, timestampFieldIndex, false);
    }

    public FieldMergeMapWithKeyTimeAgg(
            String name, MapType dataType, int timestampFieldIndex, boolean overwriteOnEqualTs) {
        super(name, dataType);
        this.keyGetter = InternalArray.createElementGetter(dataType.getKeyType());
        this.valueGetter = InternalArray.createElementGetter(dataType.getValueType());
        this.timestampFieldIndex = timestampFieldIndex;
        RowType rowType = (RowType) dataType.getValueType();
        this.timestampFieldGetter =
                InternalRow.createFieldGetter(
                        rowType.getTypeAt(timestampFieldIndex), timestampFieldIndex);
        this.timestampFieldTypeRoot = rowType.getTypeAt(timestampFieldIndex).getTypeRoot();
        this.overwriteOnEqualTs = overwriteOnEqualTs;
        this.binaryKey = BinaryMapKeys.isBinary(dataType.getKeyType());
    }

    private Object hashKey(Object key) {
        return BinaryMapKeys.hashKey(binaryKey, key);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null) {
            return inputField;
        }
        if (inputField == null) {
            return accumulator;
        }

        InternalMap accMap = (InternalMap) accumulator;
        InternalMap inputMap = (InternalMap) inputField;

        Map<Object, Object> resultMap = new HashMap<>();
        putToMap(resultMap, accMap);

        mergeInputMap(resultMap, inputMap);

        return BinaryMapKeys.toGenericMap(binaryKey, resultMap);
    }

    private void putToMap(Map<Object, Object> map, InternalMap data) {
        InternalArray keyArray = data.keyArray();
        InternalArray valueArray = data.valueArray();
        for (int i = 0; i < keyArray.size(); i++) {
            Object key = hashKey(keyGetter.getElementOrNull(keyArray, i));
            Object value = valueGetter.getElementOrNull(valueArray, i);
            map.put(key, value);
        }
    }

    private void mergeInputMap(Map<Object, Object> resultMap, InternalMap inputMap) {
        InternalArray keyArray = inputMap.keyArray();
        InternalArray valueArray = inputMap.valueArray();

        for (int i = 0; i < keyArray.size(); i++) {
            Object key = hashKey(keyGetter.getElementOrNull(keyArray, i));
            InternalRow newRow = (InternalRow) valueGetter.getElementOrNull(valueArray, i);

            if (newRow == null) {
                resultMap.remove(key);
                continue;
            }

            if (newRow.isNullAt(timestampFieldIndex)) {
                continue;
            }

            Object existingValue = resultMap.get(key);
            if (existingValue == null) {
                resultMap.put(key, newRow);
            } else {
                InternalRow existingRow = (InternalRow) existingValue;
                if (existingRow.isNullAt(timestampFieldIndex)) {
                    resultMap.put(key, newRow);
                } else {
                    Object newTs = timestampFieldGetter.getFieldOrNull(newRow);
                    Object existingTs = timestampFieldGetter.getFieldOrNull(existingRow);
                    int comparison =
                            InternalRowUtils.compare(newTs, existingTs, timestampFieldTypeRoot);
                    if (comparison > 0 || (comparison == 0 && overwriteOnEqualTs)) {
                        resultMap.put(key, newRow);
                    }
                }
            }
        }
    }
}
