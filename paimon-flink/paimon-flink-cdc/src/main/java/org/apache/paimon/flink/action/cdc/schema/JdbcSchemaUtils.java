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

package org.apache.paimon.flink.action.cdc.schema;

import org.apache.paimon.flink.action.cdc.JdbcToPaimonTypeVisitor;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;

/** Utility class to load table schema with JDBC. */
public class JdbcSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSchemaUtils.class);

    public static Schema buildSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String tableName,
            String tableComment,
            TypeMapping typeMapping,
            JdbcToPaimonTypeVisitor jdbcToPaimonTypeVisitor)
            throws SQLException {
        return buildSchema(
                metaData,
                databaseName,
                null,
                tableName,
                tableComment,
                typeMapping,
                jdbcToPaimonTypeVisitor);
    }

    public static Schema buildSchema(
            DatabaseMetaData metaData,
            String databaseName,
            String schemaName,
            String tableName,
            String tableComment,
            TypeMapping typeMapping,
            JdbcToPaimonTypeVisitor jdbcToPaimonTypeVisitor)
            throws SQLException {
        Schema.Builder builder = Schema.newBuilder();
        try (ResultSet rs = metaData.getColumns(databaseName, schemaName, tableName, null)) {
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
                boolean isNullable =
                        typeMapping.containsMode(TO_NULLABLE)
                                || isNullableColumn(rs.getString("IS_NULLABLE"));
                DataType paimonType =
                        jdbcToPaimonTypeVisitor
                                .visit(fieldType, precision, scale, typeMapping)
                                .copy(isNullable);

                builder.column(fieldName, paimonType, fieldComment);
            }
        }

        // primary keys
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(databaseName, schemaName, tableName)) {
            while (rs.next()) {
                String fieldName = rs.getString("COLUMN_NAME");
                primaryKeys.add(fieldName);
            }
        }
        builder.primaryKey(primaryKeys);

        // comment
        builder.comment(tableComment);

        return builder.build();
    }

    public static Schema mergeSchema(
            String currentTable, Schema current, String otherTable, Schema other) {
        LinkedHashMap<String, DataField> currentFields = new LinkedHashMap<>();
        current.fields().forEach(field -> currentFields.put(field.name(), field));
        for (DataField newField : other.fields()) {
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
                                        currentTable,
                                        dataField,
                                        otherTable,
                                        newField));
                }
            } else {
                currentFields.put(newField.name(), newField);
            }
        }
        Schema.Builder builder = Schema.newBuilder();
        if (current.primaryKeys().equals(other.primaryKeys())) {
            builder.primaryKey(current.primaryKeys());
        }
        builder.comment(current.comment());
        builder.options(current.options());
        builder.partitionKeys(current.partitionKeys());
        currentFields.forEach(
                ((name, dataField) ->
                        builder.column(
                                dataField.name(), dataField.type(), dataField.description())));
        return builder.build();
    }

    private static boolean isNullableColumn(final String value) {
        if ("YES".equals(value)) {
            return true;
        }

        if ("NO".equals(value)) {
            return false;
        }

        LOG.error("Unrecognized nullable value: " + value);
        return true;
    }
}
