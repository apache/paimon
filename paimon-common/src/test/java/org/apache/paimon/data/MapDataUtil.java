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

package org.apache.paimon.data;

import org.apache.paimon.types.DataType;

import java.util.HashMap;
import java.util.Map;

/** Utilities for {@link InternalMap}. */
public final class MapDataUtil {

    /**
     * Converts a {@link InternalMap} into Java {@link Map}, the keys and values of the Java map
     * still holds objects of internal data structures.
     */
    public static Map<Object, Object> convertToJavaMap(
            InternalMap map, DataType keyType, DataType valueType) {
        InternalArray keyArray = map.keyArray();
        InternalArray valueArray = map.valueArray();
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(keyType);
        InternalArray.ElementGetter valueGetter = InternalArray.createElementGetter(valueType);
        for (int i = 0; i < map.size(); i++) {
            Object key = keyGetter.getElementOrNull(keyArray, i);
            Object value = valueGetter.getElementOrNull(valueArray, i);
            javaMap.put(key, value);
        }
        return javaMap;
    }
}
