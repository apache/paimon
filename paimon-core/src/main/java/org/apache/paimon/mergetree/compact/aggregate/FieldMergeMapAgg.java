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

import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.MapType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Merge two maps. */
public class FieldMergeMapAgg extends FieldAggregator {

    public static final String NAME = "merge_map";

    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter keyGetter;
    private final InternalArray.ElementGetter valueGetter;

    public FieldMergeMapAgg(MapType dataType) {
        super(dataType);

        this.keyGetter = InternalArray.createElementGetter(dataType.getKeyType());
        this.valueGetter = InternalArray.createElementGetter(dataType.getValueType());
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

        Map<Object, Object> resultMap = new HashMap<>();
        putToMap(resultMap, accumulator);
        putToMap(resultMap, inputField);

        return new GenericMap(resultMap);
    }

    private void putToMap(Map<Object, Object> map, Object data) {
        InternalMap mapData = (InternalMap) data;
        InternalArray keyArray = mapData.keyArray();
        InternalArray valueArray = mapData.valueArray();
        for (int i = 0; i < keyArray.size(); i++) {
            map.put(
                    keyGetter.getElementOrNull(keyArray, i),
                    valueGetter.getElementOrNull(valueArray, i));
        }
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (accumulator == null) {
            return null;
        }

        InternalMap acc = (InternalMap) accumulator;
        InternalMap retract = (InternalMap) retractField;

        InternalArray retractKeyArray = retract.keyArray();
        Set<Object> retractKeys = new HashSet<>();
        for (int i = 0; i < retractKeyArray.size(); i++) {
            retractKeys.add(keyGetter.getElementOrNull(retractKeyArray, i));
        }

        Map<Object, Object> resultMap = new HashMap<>();
        InternalArray accKeyArray = acc.keyArray();
        InternalArray accValueArray = acc.valueArray();
        for (int i = 0; i < accKeyArray.size(); i++) {
            Object accKey = keyGetter.getElementOrNull(accKeyArray, i);
            if (!retractKeys.contains(accKey)) {
                resultMap.put(accKey, valueGetter.getElementOrNull(accValueArray, i));
            }
        }

        return new GenericMap(resultMap);
    }
}
