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

package org.apache.paimon.hive.objectinspector;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link MapObjectInspector} for {@link InternalMap}.
 *
 * <p>Behaviors of this class when input is null are compatible with {@link
 * org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector}.
 */
public class PaimonMapObjectInspector implements MapObjectInspector {

    private final ObjectInspector keyObjectInspector;
    private final ObjectInspector valueObjectInspector;
    private final InternalArray.ElementGetter keyGetter;
    private final InternalArray.ElementGetter valueGetter;

    public PaimonMapObjectInspector(DataType keyType, DataType valueType) {
        this.keyObjectInspector = PaimonObjectInspectorFactory.create(keyType);
        this.valueObjectInspector = PaimonObjectInspectorFactory.create(valueType);
        this.keyGetter = InternalArray.createElementGetter(keyType);
        this.valueGetter = InternalArray.createElementGetter(valueType);
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
        return keyObjectInspector;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
        return valueObjectInspector;
    }

    @Override
    public Object getMapValueElement(Object o, Object key) {
        if (o == null || key == null) {
            return null;
        }
        InternalMap mapData = (InternalMap) o;
        InternalArray keyArrayData = mapData.keyArray();
        InternalArray valueArrayData = mapData.valueArray();
        for (int i = 0; i < mapData.size(); i++) {
            Object k = keyGetter.getElementOrNull(keyArrayData, i);
            Object normalizedSearchKey = key;
            if (keyObjectInspector instanceof WriteableObjectInspector) {
                normalizedSearchKey = ((WriteableObjectInspector) keyObjectInspector).convert(key);
            }
            if (Objects.equals(k, normalizedSearchKey)) {
                return valueGetter.getElementOrNull(valueArrayData, i);
            }
        }
        return null;
    }

    @Override
    public Map<?, ?> getMap(Object o) {
        if (o == null) {
            return null;
        }
        InternalMap mapData = (InternalMap) o;
        InternalArray keyArrayData = mapData.keyArray();
        InternalArray valueArrayData = mapData.valueArray();
        Map<Object, Object> result = new HashMap<>();
        for (int i = 0; i < mapData.size(); i++) {
            Object k = keyGetter.getElementOrNull(keyArrayData, i);
            Object v = valueGetter.getElementOrNull(valueArrayData, i);
            result.put(k, v);
        }
        return result;
    }

    @Override
    public int getMapSize(Object o) {
        if (o == null) {
            return -1;
        }
        return ((InternalMap) o).size();
    }

    @Override
    public String getTypeName() {
        return serdeConstants.MAP_TYPE_NAME
                + "<"
                + keyObjectInspector.getTypeName()
                + ","
                + valueObjectInspector.getTypeName()
                + ">";
    }

    @Override
    public Category getCategory() {
        return Category.MAP;
    }
}
