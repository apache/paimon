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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/** The PostgreSQL database schema. */
public class PostgreSqlSchema {

    private final String databaseName;

    private final String schemaName;

    private final String tableName;

    private Schema schema;

    public PostgreSqlSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment)
            throws Exception {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        Schema.Builder builder = Schema.newBuilder();
        try (ResultSet rs = metaData.getColumns(null, schemaName, tableName, null)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                String fieldType = rs.getString("TYPE_NAME");
                Integer precision = rs.getInt("COLUMN_SIZE");
                String fieldComment = rs.getString("REMARKS");

                Integer scale = rs.getInt("DECIMAL_DIGITS");
                if (rs.wasNull()) {
                    scale = null;
                }

                DataType paimonType = PostgreSqlTypeUtils.toDataType(fieldType, precision, scale);

                builder.column(fieldName, paimonType, fieldComment);
            }
        }

        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
        builder.primaryKey(primaryKeys);

        builder.comment(tableComment);

        schema = builder.build();
    }

    public String databaseName() {
        return databaseName;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public Schema schema() {
        return schema;
    }

    public PostgreSqlSchema merge(PostgreSqlSchema other) {
        LinkedHashMap<String, DataField> currentFields = new LinkedHashMap<>();
        schema.fields().forEach(field -> currentFields.put(field.name(), field));
        for (DataField newField : other.schema.fields()) {
            DataField dataField = currentFields.get(newField.name());
            if (Objects.nonNull(dataField)) {
                DataType oldType = dataField.type();
                switch (UpdatedDataFieldsProcessFunction.canConvert(oldType, newField.type())) {
                    case CONVERT:
                        currentFields.put(newField.name(), newField);
                        break;
                    case EXCEPTION:
                        throw new IllegalArgumentException(
                                String.format(
                                        "Column %s have different types when merging schemas.\n"
                                                + "Current table '%s' field: %s\n"
                                                + "To be merged table '%s' field: %s",
                                        newField.name(),
                                        tableName,
                                        dataField,
                                        other.tableName,
                                        newField));
                }
            } else {
                currentFields.put(newField.name(), newField);
            }
        }

        Schema.Builder builder = Schema.newBuilder();
        if (!schema.primaryKeys().equals(other.schema.primaryKeys())) {
            schema.primaryKeys().clear();
        }
        builder.comment(schema.comment());
        builder.options(schema.options());
        currentFields.forEach(
                ((name, dataField) ->
                        builder.column(
                                dataField.name(), dataField.type(), dataField.description())));
        schema = builder.build();
        return this;
    }
}
