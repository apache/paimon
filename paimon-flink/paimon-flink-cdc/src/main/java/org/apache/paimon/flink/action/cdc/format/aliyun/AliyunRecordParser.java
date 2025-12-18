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

package org.apache.paimon.flink.action.cdc.format.aliyun;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractJsonRecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.flink.api.java.tuple.Tuple3;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.utils.JsonSerdeUtil.getNodeAs;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * The {@code CanalRecordParser} class is responsible for parsing records from the Canal-JSON
 * format. Canal is a database binlog multi-platform consumer, which is used to synchronize data
 * across databases. This parser extracts relevant information from the Canal-JSON format and
 * transforms it into a list of {@link RichCdcMultiplexRecord} objects, which represent the changes
 * captured in the database.
 *
 * <p>The class handles different types of database operations such as INSERT, UPDATE, and DELETE,
 * and generates corresponding {@link RichCdcMultiplexRecord} objects for each operation.
 *
 * <p>Additionally, the parser supports schema extraction, which can be used to understand the
 * structure of the incoming data and its corresponding field types.
 */
public class AliyunRecordParser extends AbstractJsonRecordParser {

    private static final String FIELD_IS_DDL = "ddl";
    private static final String FIELD_OPERATION = "op";

    private static final String OP_UPDATE_BEFORE = "UPDATE_BEFORE";
    private static final String OP_UPDATE_AFTER = "UPDATE_AFTER";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";

    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_COLUMN = "dataColumn";

    private static final String FIELD_SCHEMA = "schema";
    private static final String FIELD_PK = "primaryKey";

    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";

    @Override
    protected boolean isDDL() {
        JsonNode node = root.get(FIELD_IS_DDL);
        return !isNull(node) && node.asBoolean();
    }

    public AliyunRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    protected String primaryField() {
        return "schema.primaryKey";
    }

    @Override
    protected String dataField() {
        return "payload.dataColumn";
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        JsonNode schemaNode = root.get(FIELD_SCHEMA);
        checkNotNull(schemaNode, FIELD_SCHEMA);
        ArrayNode pkNode = getNodeAs(schemaNode, FIELD_PK, ArrayNode.class);
        List<String> pkFields = new ArrayList<>();
        pkNode.forEach(
                pk -> {
                    if (isNull(pk)) {
                        throw new IllegalArgumentException(
                                String.format("Primary key cannot be null: %s", pk));
                    }

                    pkFields.add(pk.asText());
                });
        return pkFields;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        if (isDDL()) {
            return Collections.emptyList();
        }

        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        JsonNode payload = root.get(FIELD_PAYLOAD);
        checkNotNull(payload, FIELD_PAYLOAD);

        String type = payload.get(FIELD_OPERATION).asText();

        RowKind rowKind;
        String field;
        switch (type) {
            case OP_UPDATE_BEFORE:
                rowKind = RowKind.UPDATE_BEFORE;
                field = FIELD_BEFORE;
                break;
            case OP_UPDATE_AFTER:
                rowKind = RowKind.UPDATE_AFTER;
                field = FIELD_AFTER;
                break;
            case OP_INSERT:
                rowKind = RowKind.INSERT;
                field = FIELD_AFTER;
                break;
            case OP_DELETE:
                rowKind = RowKind.DELETE;
                field = FIELD_BEFORE;
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + type);
        }

        JsonNode container = payload.get(field);
        checkNotNull(container, String.format("%s.%s", FIELD_PAYLOAD, field));

        JsonNode data = getNodeAs(container, FIELD_COLUMN, JsonNode.class);
        checkNotNull(data, String.format("%s.%s.%s", FIELD_PAYLOAD, field, FIELD_COLUMN));

        processRecord(data, rowKind, records);

        return records;
    }

    @Override
    protected Map<String, String> extractRowData(JsonNode record, CdcSchema.Builder schemaBuilder) {

        Map<String, Object> recordMap =
                JsonSerdeUtil.convertValue(record, new TypeReference<Map<String, Object>>() {});
        Map<String, String> rowData = new HashMap<>();

        JsonNode schemaNode = root.get(FIELD_SCHEMA);
        checkNotNull(schemaNode, FIELD_SCHEMA);
        ArrayNode typeNodes = getNodeAs(schemaNode, FIELD_COLUMN, ArrayNode.class);
        checkNotNull(typeNodes, FIELD_COLUMN);

        for (int i = 0; i < typeNodes.size(); i++) {
            JsonNode typeNode = typeNodes.get(i);
            String originalName = typeNode.get(FIELD_NAME).asText();
            String originalType = typeNode.get(FIELD_TYPE).asText();
            Tuple3<String, Integer, Integer> typeInfo = MySqlTypeUtils.getTypeInfo(originalType);
            DataType paimonDataType =
                    MySqlTypeUtils.toDataType(typeInfo.f0, typeInfo.f1, typeInfo.f2, typeMapping);
            schemaBuilder.column(originalName, paimonDataType);
        }

        for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
            rowData.put(entry.getKey(), Objects.toString(entry.getValue(), null));
        }

        evalComputedColumns(rowData, schemaBuilder);
        evalMetadataColumns(rowData, schemaBuilder);

        return rowData;
    }

    @Override
    protected String format() {
        return "aliyun-json";
    }

    @Nullable
    @Override
    protected String getTableName() {
        JsonNode schemaNode = root.get(FIELD_SCHEMA);
        if (isNull(schemaNode)) {
            return null;
        }
        JsonNode sourceNode = schemaNode.get("source");
        if (isNull(sourceNode)) {
            return null;
        }

        JsonNode tableNode = sourceNode.get("tableName");
        if (isNull(tableNode)) {
            return null;
        }
        return tableNode.asText();
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        JsonNode schemaNode = root.get(FIELD_SCHEMA);
        if (isNull(schemaNode)) {
            return null;
        }
        JsonNode sourceNode = schemaNode.get("source");
        if (isNull(sourceNode)) {
            return null;
        }
        JsonNode databaseNode = sourceNode.get("dbName");
        if (isNull(databaseNode)) {
            return null;
        }
        return databaseNode.asText();
    }
}
