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
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.MapType;
import org.apache.paimon.utils.ByteArrayKey;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Merge two maps. */
public class FieldMergeMapAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter keyGetter;
    private final InternalArray.ElementGetter valueGetter;

    /**
     * A key of type {@code BINARY} or {@code VARBINARY} arrives as a {@code byte[]}, which inherits
     * identity equality from {@link Object}. Used directly, two keys with the same content occupy
     * two entries and a retraction never matches, so such keys are held in a {@link ByteArrayKey}
     * while they are in a hash collection and unwrapped again on the way out.
     */
    private final boolean binaryKey;

    public FieldMergeMapAgg(String name, MapType dataType) {
        super(name, dataType);

        this.keyGetter = InternalArray.createElementGetter(dataType.getKeyType());
        this.valueGetter = InternalArray.createElementGetter(dataType.getValueType());
        this.binaryKey =
                dataType.getKeyType()
                        .getTypeRoot()
                        .getFamilies()
                        .contains(DataTypeFamily.BINARY_STRING);
    }

    private Object hashKey(Object key) {
        return binaryKey && key != null ? new ByteArrayKey((byte[]) key) : key;
    }

    private Object originalKey(Object key) {
        return key instanceof ByteArrayKey ? ((ByteArrayKey) key).bytes() : key;
    }

    private GenericMap toGenericMap(Map<Object, Object> map) {
        if (!binaryKey) {
            return new GenericMap(map);
        }
        Map<Object, Object> unwrapped = new HashMap<>(map.size());
        map.forEach((k, v) -> unwrapped.put(originalKey(k), v));
        return new GenericMap(unwrapped);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        Map<Object, Object> resultMap = new HashMap<>();
        putToMap(resultMap, accumulator);
        putToMap(resultMap, inputField);

        return toGenericMap(resultMap);
    }

    private void putToMap(Map<Object, Object> map, Object data) {
        InternalMap mapData = (InternalMap) data;
        InternalArray keyArray = mapData.keyArray();
        InternalArray valueArray = mapData.valueArray();
        for (int i = 0; i < keyArray.size(); i++) {
            map.put(
                    hashKey(keyGetter.getElementOrNull(keyArray, i)),
                    valueGetter.getElementOrNull(valueArray, i));
        }
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
        InternalMap retract = (InternalMap) retractField;
        if (retract.size() == 0) {
            return accumulator;
        }

        InternalArray retractKeyArray = retract.keyArray();
        Set<Object> retractKeys = new HashSet<>();
        for (int i = 0; i < retractKeyArray.size(); i++) {
            retractKeys.add(hashKey(keyGetter.getElementOrNull(retractKeyArray, i)));
        }

        InternalMap acc = (InternalMap) accumulator;
        Map<Object, Object> resultMap = new HashMap<>();
        InternalArray accKeyArray = acc.keyArray();
        InternalArray accValueArray = acc.valueArray();
        for (int i = 0; i < accKeyArray.size(); i++) {
            Object accKey = hashKey(keyGetter.getElementOrNull(accKeyArray, i));
            if (!retractKeys.contains(accKey)) {
                resultMap.put(accKey, valueGetter.getElementOrNull(accValueArray, i));
            }
        }

        return toGenericMap(resultMap);
    }
}
