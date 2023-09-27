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

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnCaseConvertAndDuplicateCheck;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;

/** Schema builder for MySQL cdc. */
public class MySqlTableSchemaBuilder implements NewTableSchemaBuilder<TableChanges.TableChange> {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;
    private final TypeMapping typeMapping;

    public MySqlTableSchemaBuilder(
            Map<String, String> tableConfig, boolean caseSensitive, TypeMapping typeMapping) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
        this.typeMapping = typeMapping;
    }

    @Override
    public Optional<Schema> build(TableChanges.TableChange tableChange) {
        Table table = tableChange.getTable();
        String tableName = tableChange.getId().toString();
        List<Column> columns = table.columns();

        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg = columnDuplicateErrMsg(tableName);

        Schema.Builder builder = Schema.newBuilder();

        // column
        for (Column column : columns) {
            DataType dataType =
                    MySqlTypeUtils.toDataType(
                            column.typeExpression(),
                            column.length(),
                            column.scale().orElse(null),
                            typeMapping);

            dataType = dataType.copy(typeMapping.containsMode(TO_NULLABLE) || column.isOptional());

            String columnName =
                    columnCaseConvertAndDuplicateCheck(
                            column.name(), existedFields, caseSensitive, columnDuplicateErrMsg);

            // TODO : add table comment and column comment when we upgrade flink cdc to 2.4
            builder.column(columnName, dataType, null);
        }

        // primaryKey
        List<String> primaryKeys = table.primaryKeyColumnNames();
        primaryKeys = listCaseConvert(primaryKeys, caseSensitive);
        builder.primaryKey(primaryKeys);

        // options
        builder.options(tableConfig);

        return Optional.of(builder.build());
    }
}
