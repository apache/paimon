/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive.objectinspector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link MapObjectInspector} for {@link MapData}.
 *
 * <p>Behaviors of this class when input is null are compatible with {@link
 * org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector}.
 */
public class TableStoreMapObjectInspector implements MapObjectInspector {

    private final ObjectInspector keyObjectInspector;
    private final ObjectInspector valueObjectInspector;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    public TableStoreMapObjectInspector(LogicalType keyType, LogicalType valueType) {
        this.keyObjectInspector = TableStoreObjectInspectorFactory.create(keyType);
        this.valueObjectInspector = TableStoreObjectInspectorFactory.create(valueType);
        this.keyGetter = ArrayData.createElementGetter(keyType);
        this.valueGetter = ArrayData.createElementGetter(valueType);
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
        MapData mapData = (MapData) o;
        ArrayData keyArrayData = mapData.keyArray();
        ArrayData valueArrayData = mapData.valueArray();
        for (int i = 0; i < mapData.size(); i++) {
            Object k = keyGetter.getElementOrNull(keyArrayData, i);
            if (Objects.equals(k, key)) {
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
        MapData mapData = (MapData) o;
        ArrayData keyArrayData = mapData.keyArray();
        ArrayData valueArrayData = mapData.valueArray();
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
        return ((MapData) o).size();
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
