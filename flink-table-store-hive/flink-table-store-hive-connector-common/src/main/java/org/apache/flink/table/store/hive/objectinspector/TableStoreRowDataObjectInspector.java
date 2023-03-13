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

import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.hive.HiveTypeUtils;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.utils.RowDataUtils;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** {@link StructObjectInspector} for {@link InternalRow}. */
public class TableStoreRowDataObjectInspector extends StructObjectInspector {

    private final List<TableStoreStructField> structFields;
    private final Map<String, TableStoreStructField> structFieldMap;
    private final String typeName;

    public TableStoreRowDataObjectInspector(
            List<String> fieldNames, List<DataType> fieldTypes, List<String> fieldComments) {
        this.structFields = new ArrayList<>();
        this.structFieldMap = new HashMap<>();
        StringBuilder typeNameBuilder = new StringBuilder("struct<");

        for (int i = 0; i < fieldNames.size(); i++) {
            String name = fieldNames.get(i);
            DataType logicalType = fieldTypes.get(i);
            TableStoreStructField structField =
                    new TableStoreStructField(
                            name,
                            TableStoreObjectInspectorFactory.create(logicalType),
                            i,
                            RowDataUtils.createNullCheckingFieldGetter(logicalType, i),
                            fieldComments.get(i));
            structFields.add(structField);
            structFieldMap.put(name, structField);

            if (i > 0) {
                typeNameBuilder.append(",");
            }
            typeNameBuilder
                    .append(name)
                    .append(":")
                    .append(HiveTypeUtils.logicalTypeToTypeInfo(logicalType).getTypeName());
        }

        typeNameBuilder.append(">");
        this.typeName = typeNameBuilder.toString();
    }

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
        return structFields;
    }

    @Override
    public StructField getStructFieldRef(String name) {
        return structFieldMap.get(name);
    }

    @Override
    public Object getStructFieldData(Object o, StructField structField) {
        InternalRow rowData = (InternalRow) o;
        return ((TableStoreStructField) structField).fieldGetter.getFieldOrNull(rowData);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
        InternalRow rowData = (InternalRow) o;
        return structFields.stream()
                .map(f -> f.fieldGetter.getFieldOrNull(rowData))
                .collect(Collectors.toList());
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public Category getCategory() {
        return Category.STRUCT;
    }

    private static class TableStoreStructField implements StructField {

        private final String name;
        private final ObjectInspector objectInspector;
        private final int idx;
        private final InternalRow.FieldGetter fieldGetter;
        private final String comment;

        private TableStoreStructField(
                String name,
                ObjectInspector objectInspector,
                int idx,
                InternalRow.FieldGetter fieldGetter,
                String comment) {
            this.name = name;
            this.objectInspector = objectInspector;
            this.idx = idx;
            this.fieldGetter = fieldGetter;
            this.comment = comment;
        }

        @Override
        public String getFieldName() {
            return name;
        }

        @Override
        public ObjectInspector getFieldObjectInspector() {
            return objectInspector;
        }

        @Override
        public int getFieldID() {
            return idx;
        }

        @Override
        public String getFieldComment() {
            return comment;
        }
    }
}
