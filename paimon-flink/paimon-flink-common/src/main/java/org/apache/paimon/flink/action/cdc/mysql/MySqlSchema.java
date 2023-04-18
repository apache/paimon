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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.types.DataType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utility class to load MySQL table schema with JDBC. */
public class MySqlSchema {

    // used for retrieving metadata and throwing error, do not convert to case-insensitive form
    private final String databaseName;
    private final String originalTableName;
    // might be converted to case-insensitive form
    private final String tableName;

    private final LinkedHashMap<String, DataType> fields;
    private final List<String> primaryKeys;

    public MySqlSchema(
            DatabaseMetaData metaData, String databaseName, String tableName, boolean caseSensitive)
            throws Exception {
        this.databaseName = databaseName;
        this.originalTableName = tableName;
        this.tableName = caseSensitive ? tableName : tableName.toLowerCase();

        fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(null, databaseName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");
                if (rs.wasNull()) {
                    precision = null;
                }
                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }
                if (!caseSensitive) {
                    fieldName = fieldName.toLowerCase();
                    checkArgument(
                            !fields.containsKey(fieldName),
                            "Duplicate key appears when converting fields map keys to case-insensitive form.");
                }
                fields.put(fieldName, MySqlTypeUtils.toDataType(fieldType, precision, scale));
            }
        }

        primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(null, databaseName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                if (!caseSensitive) {
                    fieldName = fieldName.toLowerCase();
                }
                primaryKeys.add(fieldName);
            }
        }
    }

    public String originalTableName() {
        return originalTableName;
    }

    public String tableName() {
        return tableName;
    }

    public Map<String, DataType> fields() {
        return fields;
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public MySqlSchema merge(MySqlSchema other) {
        for (Map.Entry<String, DataType> entry : other.fields.entrySet()) {
            String fieldName = entry.getKey();
            DataType newType = entry.getValue();
            if (fields.containsKey(fieldName)) {
                DataType oldType = fields.get(fieldName);
                switch (UpdatedDataFieldsProcessFunction.canConvert(oldType, newType)) {
                    case CONVERT:
                        fields.put(fieldName, newType);
                        break;
                    case EXCEPTION:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Column %s have different types in table %s.%s and table %s.%s",
                                        fieldName,
                                        databaseName,
                                        tableName,
                                        other.databaseName,
                                        other.tableName));
                }
            } else {
                fields.put(fieldName, newType);
            }
        }
        if (!primaryKeys.equals(other.primaryKeys)) {
            primaryKeys.clear();
        }
        return this;
    }
}
