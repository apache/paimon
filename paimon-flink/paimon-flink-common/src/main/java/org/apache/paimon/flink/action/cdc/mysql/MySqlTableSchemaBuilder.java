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

import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.mysql.MySqlActionUtils.MYSQL_CONVERTER_TINYINT1_BOOL;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Schema builder for MySQL cdc. */
public class MySqlTableSchemaBuilder implements NewTableSchemaBuilder<MySqlCreateTableStatement> {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;

    public MySqlTableSchemaBuilder(Map<String, String> tableConfig, boolean caseSensitive) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
    }

    @Override
    public Optional<Schema> build(MySqlCreateTableStatement statement) {
        List<SQLTableElement> columns = statement.getTableElementList();
        LinkedHashMap<String, Tuple2<DataType, String>> fields = new LinkedHashMap<>();

        for (SQLTableElement element : columns) {
            if (element instanceof SQLColumnDefinition) {
                SQLColumnDefinition column = (SQLColumnDefinition) element;
                SQLName name = column.getName();
                SQLDataType dataType = column.getDataType();
                List<SQLExpr> arguments = dataType.getArguments();
                Integer precision = null;
                Integer scale = null;
                if (arguments.size() >= 1) {
                    precision = (int) (((SQLIntegerExpr) arguments.get(0)).getValue());
                }

                if (arguments.size() >= 2) {
                    scale = (int) (((SQLIntegerExpr) arguments.get(1)).getValue());
                }

                SQLCharExpr comment = (SQLCharExpr) column.getComment();
                fields.put(
                        name.getSimpleName(),
                        Tuple2.of(
                                MySqlTypeUtils.toDataType(
                                        column.getDataType().getName(),
                                        precision,
                                        scale,
                                        MYSQL_CONVERTER_TINYINT1_BOOL.defaultValue()),
                                comment == null ? null : String.valueOf(comment.getValue())));
            }
        }

        List<String> primaryKeys = statement.getPrimaryKeyNames();

        if (!caseSensitive) {
            LinkedHashMap<String, Tuple2<DataType, String>> tmp = new LinkedHashMap<>();
            for (Map.Entry<String, Tuple2<DataType, String>> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !tmp.containsKey(fieldName.toLowerCase()),
                        "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                        fieldName,
                        statement.getTableName());
                tmp.put(fieldName.toLowerCase(), entry.getValue());
            }
            fields = tmp;

            primaryKeys =
                    primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
        }

        Schema.Builder builder = Schema.newBuilder();
        builder.options(tableConfig);
        for (Map.Entry<String, Tuple2<DataType, String>> entry : fields.entrySet()) {
            builder.column(
                    entry.getKey().replaceAll("`", ""), entry.getValue().f0, entry.getValue().f1);
        }

        primaryKeys =
                primaryKeys.stream().map(m -> m.replaceAll("`", "")).collect(Collectors.toList());

        Schema schema = builder.primaryKey(primaryKeys).build();

        return Optional.of(schema);
    }
}
