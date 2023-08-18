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

import org.apache.paimon.flink.action.cdc.DataTypeMapMode;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunctionBase;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Utility class to load MySQL table schema with JDBC. */
public class MySqlSchema {

    private final List<MySqlColumn> columns;
    private final List<String> primaryKeys;

    private MySqlSchema(List<MySqlColumn> columns, List<String> primaryKeys) {
        this.columns = columns;
        this.primaryKeys = primaryKeys;
    }

    public static MySqlSchema buildSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String tableName,
            boolean convertTinyintToBool,
            DataTypeMapMode dataTypeMapMode)
            throws SQLException {
        List<MySqlColumn> columns = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(databaseName, null, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String defaultValue = rs.getString("COLUMN_DEF");
                String fieldComment = rs.getString("REMARKS");
                DataType paimonType;

                switch (dataTypeMapMode) {
                    case IDENTITY:
                        String fieldType = rs.getString("TYPE_NAME");

                        Integer precision = rs.getInt("COLUMN_SIZE");
                        if (rs.wasNull()) {
                            precision = null;
                        }

                        Integer scale = rs.getInt("DECIMAL_DIGITS");
                        if (rs.wasNull()) {
                            scale = null;
                        }
                        paimonType =
                                MySqlTypeUtils.toDataType(
                                        fieldType, precision, scale, convertTinyintToBool);
                        break;
                    case ALL_TO_STRING:
                        paimonType = DataTypes.STRING();
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unsupported data type map mode: " + dataTypeMapMode);
                }
                columns.add(new MySqlColumn(fieldName, paimonType, defaultValue, fieldComment));
            }
        }

        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, null, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }

        return new MySqlSchema(columns, primaryKeys);
    }

    public List<MySqlColumn> columns(boolean caseSensitive) {
        if (caseSensitive) {
            return columns;
        } else {
            List<MySqlColumn> lowerCaseColumns =
                    columns.stream().map(MySqlColumn::toLowerCaseCopy).collect(Collectors.toList());

            List<String> columnNames =
                    lowerCaseColumns.stream().map(MySqlColumn::name).collect(Collectors.toList());

            Set<String> duplicateColumns =
                    columnNames.stream()
                            .filter(columName -> Collections.frequency(columnNames, columName) > 1)
                            .collect(Collectors.toSet());

            checkState(
                    duplicateColumns.isEmpty(),
                    "There are duplicate fields [%s] after converted to lower case.",
                    duplicateColumns);

            return lowerCaseColumns;
        }
    }

    public List<String> primaryKeys() {
        return primaryKeys;
    }

    public List<String> primaryKeys(boolean caseSensitive) {
        if (caseSensitive) {
            return primaryKeys;
        } else {
            return primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
        }
    }

    public Map<String, DataType> typeMapping() {
        return columns.stream().collect(Collectors.toMap(MySqlColumn::name, MySqlColumn::type));
    }

    public MySqlSchema merge(String currentTable, String otherTable, MySqlSchema other) {
        LinkedHashMap<String, MySqlColumn> columnMapping = new LinkedHashMap<>();
        columns.forEach(column -> columnMapping.put(column.name(), column));
        for (MySqlColumn newColumn : other.columns) {
            String columnName = newColumn.name();
            DataType newType = newColumn.type();
            if (columnMapping.containsKey(columnName)) {
                MySqlColumn oldColumn = columnMapping.get(columnName);
                DataType oldType = oldColumn.type();
                switch (UpdatedDataFieldsProcessFunctionBase.canConvert(oldType, newType)) {
                    case CONVERT:
                        columnMapping.put(columnName, oldColumn.merge(newColumn));
                        break;
                    case EXCEPTION:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Column %s have different types when merging schemas.\n"
                                                + "Current table '%s' columns: %s\n"
                                                + "To be merged table '%s' columns: %s",
                                        columnName,
                                        currentTable,
                                        columnsToString(),
                                        otherTable,
                                        other.columnsToString()));
                }
            } else {
                columnMapping.put(columnName, newColumn.copy());
            }
        }

        List<String> pks =
                primaryKeys.equals(other.primaryKeys) ? primaryKeys : Collections.emptyList();

        return new MySqlSchema(new ArrayList<>(columnMapping.values()), pks);
    }

    public String columnsToString() {
        return "["
                + columns.stream()
                        .map(c -> String.format("%s %s", c.name(), c.type()))
                        .collect(Collectors.joining(","))
                + "]";
    }
}
