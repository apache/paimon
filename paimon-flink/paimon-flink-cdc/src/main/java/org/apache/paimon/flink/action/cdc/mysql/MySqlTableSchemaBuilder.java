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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;

/** Schema builder for MySQL cdc. */
public class MySqlTableSchemaBuilder implements NewTableSchemaBuilder<JsonNode> {

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
    public Optional<Schema> build(JsonNode tableChange) {
        JsonNode jsonTable = tableChange.get("table");
        String tableName = tableChange.get("id").asText();
        ArrayNode columns = (ArrayNode) jsonTable.get("columns");
        LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();

        for (JsonNode element : columns) {
            JsonNode length = element.get("length");
            JsonNode scale = element.get("scale");
            DataType dataType =
                    MySqlTypeUtils.toDataType(
                            element.get("typeExpression").asText(),
                            length == null ? null : length.asInt(),
                            scale == null ? null : scale.asInt(),
                            typeMapping);

            if (!typeMapping.containsMode(TO_NULLABLE)) {
                dataType.copy(element.get("optional").asBoolean());
            }

            // TODO : add table comment and column comment when we upgrade flink cdc to 2.4
            fields.put(element.get("name").asText(), dataType);
        }

        ArrayNode arrayNode = (ArrayNode) jsonTable.get("primaryKeyColumnNames");
        List<String> primaryKeys = new ArrayList<>();
        for (JsonNode primary : arrayNode) {
            primaryKeys.add(primary.asText());
        }

        fields = mapKeyCaseConvert(fields, caseSensitive, columnDuplicateErrMsg(tableName));
        primaryKeys = listCaseConvert(primaryKeys, caseSensitive);

        Schema.Builder builder = Schema.newBuilder();
        builder.options(tableConfig);
        for (Map.Entry<String, DataType> entry : fields.entrySet()) {
            builder.column(entry.getKey(), entry.getValue());
        }
        Schema schema = builder.primaryKey(primaryKeys).build();

        return Optional.of(schema);
    }
}
