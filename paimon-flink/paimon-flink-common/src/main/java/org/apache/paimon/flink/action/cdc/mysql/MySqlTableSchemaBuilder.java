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

import org.apache.paimon.flink.action.cdc.DataTypeMapMode;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Schema builder for MySQL cdc. */
public class MySqlTableSchemaBuilder implements NewTableSchemaBuilder<JsonNode> {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;
    private final boolean convertTinyint1ToBool;
    private final DataTypeMapMode dataTypeMapMode;

    public MySqlTableSchemaBuilder(
            Map<String, String> tableConfig,
            boolean caseSensitive,
            boolean convertTinyint1ToBool,
            DataTypeMapMode dataTypeMapMode) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
        this.convertTinyint1ToBool = convertTinyint1ToBool;
        this.dataTypeMapMode = dataTypeMapMode;
    }

    @Override
    public Optional<Schema> build(JsonNode tableChange) {
        JsonNode jsonTable = tableChange.get("table");
        String tableName = tableChange.get("id").asText();
        ArrayNode columns = (ArrayNode) jsonTable.get("columns");
        LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();

        DataType dataType;
        for (JsonNode element : columns) {
            switch (dataTypeMapMode) {
                case IDENTITY:
                    Integer precision =
                            element.has("length") ? element.get("length").asInt() : null;
                    Integer scale = element.has("scale") ? element.get("scale").asInt() : null;
                    dataType =
                            MySqlTypeUtils.toDataType(
                                            element.get("typeExpression").asText(),
                                            precision,
                                            scale,
                                            convertTinyint1ToBool)
                                    .copy(element.get("optional").asBoolean());
                    break;
                case ALL_TO_STRING:
                    dataType = DataTypes.STRING();
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported data type map mode: " + dataTypeMapMode);
            }

            // TODO : add table comment and column comment when we upgrade flink cdc to 2.4
            fields.put(element.get("name").asText(), dataType);
        }

        ArrayNode arrayNode = (ArrayNode) jsonTable.get("primaryKeyColumnNames");
        List<String> primaryKeys = new ArrayList<>();
        for (JsonNode primary : arrayNode) {
            primaryKeys.add(primary.asText());
        }

        if (!caseSensitive) {
            LinkedHashMap<String, DataType> tmp = new LinkedHashMap<>();
            for (Map.Entry<String, DataType> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                checkArgument(
                        !tmp.containsKey(fieldName.toLowerCase()),
                        "Duplicate key '%s' in table '%s' appears when converting fields map keys to case-insensitive form.",
                        fieldName,
                        tableName);
                tmp.put(fieldName.toLowerCase(), entry.getValue());
            }
            fields = tmp;

            primaryKeys =
                    primaryKeys.stream().map(String::toLowerCase).collect(Collectors.toList());
        }

        Schema.Builder builder = Schema.newBuilder();
        builder.options(tableConfig);
        for (Map.Entry<String, DataType> entry : fields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }
        Schema schema = builder.primaryKey(primaryKeys).build();

        return Optional.of(schema);
    }
}
