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

package org.apache.paimon.flink.action.cdc.mysql.schema;

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Pair;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class to load MySQL table schema with JDBC. */
public class MySqlSchema {

    private final LinkedHashMap<String, Pair<DataType, String>> fields;
    private final List<String> primaryKeys;

    private MySqlSchema(
            LinkedHashMap<String, Pair<DataType, String>> fields, List<String> primaryKeys) {
        this.fields = fields;
        this.primaryKeys = primaryKeys;
    }

    public static MySqlSchema buildSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String tableName,
            TypeMapping typeMapping)
            throws SQLException {
        LinkedHashMap<String, Pair<DataType, String>> fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String fieldType = rs.getString("TYPE_NAME");
                String fieldComment = rs.getString("REMARKS");

                Integer precision = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    precision = null;
                }

                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                DataType paimonType =
                        MySqlTypeUtils.toDataType(fieldType, precision, scale, typeMapping);

                fields.put(fieldName, Pair.of(paimonType, fieldComment));
            }
        }

        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }

        return new MySqlSchema(fields, primaryKeys);
    }

    public LinkedHashMap<String, Pair<DataType, String>> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public LinkedHashMap<String, DataType> typeMapping() {
        LinkedHashMap<String, DataType> typeMapping = new LinkedHashMap<>();
        fields.forEach((name, pair) -> typeMapping.put(name, pair.getLeft()));
        return typeMapping;
    }

    public List<String> comments() {
        List<String> comments = new ArrayList<>();
        fields.forEach((name, pair) -> comments.add(pair.getRight()));
        return comments;
    }

    public MySqlSchema merge(String currentTable, String otherTable, MySqlSchema other) {
        for (Map.Entry<String, Pair<DataType, String>> entry : other.fields.entrySet()) {
            String fieldName = entry.getKey();
            DataType newType = entry.getValue().getLeft();
            if (fields.containsKey(fieldName)) {
                DataType oldType = fields.get(fieldName).getLeft();
                switch (UpdatedDataFieldsProcessFunction.canConvert(oldType, newType)) {
                    case CONVERT:
                        fields.put(fieldName, Pair.of(newType, entry.getValue().getRight()));
                        break;
                    case EXCEPTION:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Column %s have different types when merging schemas.\n"
                                                + "Current table '%s' fields: %s\n"
                                                + "To be merged table '%s' fields: %s",
                                        fieldName,
                                        currentTable,
                                        fieldsToString(),
                                        otherTable,
                                        other.fieldsToString()));
                }
            } else {
                fields.put(fieldName, Pair.of(newType, entry.getValue().getRight()));
            }
        }

        if (!primaryKeys.equals(other.primaryKeys)) {
            primaryKeys.clear();
        }
        return this;
    }

    private String fieldsToString() {
        return "["
                + fields.entrySet().stream()
                        .map(e -> String.format("%s %s", e.getKey(), e.getValue().getLeft()))
                        .collect(Collectors.joining(","))
                + "]";
    }
}
