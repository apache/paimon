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
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ListObjectInspector} for {@link ArrayData}.
 *
 * <p>Behaviors of this class when input is null are compatible with {@link
 * org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector}.
 */
public class TableStoreListObjectInspector implements ListObjectInspector {

    private final ObjectInspector elementObjectInspector;
    private final ArrayData.ElementGetter elementGetter;

    public TableStoreListObjectInspector(LogicalType elementType) {
        this.elementObjectInspector = TableStoreObjectInspectorFactory.create(elementType);
        this.elementGetter = ArrayData.createElementGetter(elementType);
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
        return elementObjectInspector;
    }

    @Override
    public Object getListElement(Object o, int i) {
        if (o == null) {
            return null;
        }
        return elementGetter.getElementOrNull((ArrayData) o, i);
    }

    @Override
    public int getListLength(Object o) {
        if (o == null) {
            return -1;
        }
        return ((ArrayData) o).size();
    }

    @Override
    public List<?> getList(Object o) {
        if (o == null) {
            return null;
        }
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < getListLength(o); i++) {
            result.add(getListElement(o, i));
        }
        return result;
    }

    @Override
    public String getTypeName() {
        return serdeConstants.LIST_TYPE_NAME + "<" + elementObjectInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
        return Category.LIST;
    }
}
