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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.JsonRecordParser;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import javax.annotation.Nullable;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_AFTER;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_BEFORE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_DB;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_PAYLOAD;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_PRIMARY;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SCHEMA;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SOURCE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_TYPE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_DELETE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_INSERT;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_READE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_UPDATE;
import static org.apache.paimon.utils.JsonSerdeUtil.getNodeAs;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * The {@code DebeziumRecordParser} class extends the abstract {@link JsonRecordParser} and is
 * designed to parse records from Debezium's JSON change data capture (CDC) format. Debezium is a
 * CDC solution for MySQL databases that captures row-level changes to database tables and outputs
 * them in JSON format. This parser extracts relevant information from the Debezium-JSON format and
 * converts it into a list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, DELETE, and READ
 * (snapshot reads), and creates corresponding {@link RichCdcMultiplexRecord} objects to represent
 * these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields,
 * including the 'before' and 'after' states for UPDATE operations, and the class also supports
 * schema extraction for the Kafka topic. Debezium's specific fields such as 'source', 'op' for
 * operation type, and primary key field names are used to construct the details of each record
 * event.
 */
public class DebeziumJsonRecordParser extends JsonRecordParser {

    private boolean hasSchema;
    private final Map<String, String> debeziumTypes = new HashMap<>();
    private final Map<String, String> classNames = new HashMap<>();
    private final Map<String, Map<String, String>> parameters = new HashMap<>();

    public DebeziumJsonRecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        String operation = getAndCheck(FIELD_TYPE).asText();
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        switch (operation) {
            case OP_INSERT:
            case OP_READE:
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(
                        mergeOldRecord(getData(), getBefore(operation)), RowKind.DELETE, records);
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    private JsonNode getData() {
        return getAndCheck(dataField());
    }

    private JsonNode getBefore(String op) {
        return getAndCheck(FIELD_BEFORE, FIELD_TYPE, op);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        JsonNode node = (JsonNode) record.getValue();

        hasSchema = false;
        if (node.has(FIELD_SCHEMA)) {
            root = node.get(FIELD_PAYLOAD);
            JsonNode schema = node.get(FIELD_SCHEMA);
            if (!isNull(schema)) {
                parseSchema(schema);
                hasSchema = true;
            }
        } else {
            root = node;
        }
    }

    private void parseSchema(JsonNode schema) {
        debeziumTypes.clear();
        classNames.clear();
        parameters.clear();

        ArrayNode schemaFields = getNodeAs(schema, "fields", ArrayNode.class);
        Preconditions.checkNotNull(schemaFields);

        ArrayNode fields = null;
        for (int i = 0; i < schemaFields.size(); i++) {
            JsonNode node = schemaFields.get(i);
            if ("after".equals(getString(node, "field"))) {
                fields = getNodeAs(node, "fields", ArrayNode.class);
                break;
            } else if ("before".equals(getString(node, "field"))) {
                if (fields == null) {
                    fields = getNodeAs(node, "fields", ArrayNode.class);
                }
            }
        }
        Preconditions.checkNotNull(fields);

        for (JsonNode node : fields) {
            String field = getString(node, "field");

            debeziumTypes.put(field, getString(node, "type"));
            classNames.put(field, getString(node, "name"));

            JsonNode parametersNode = node.get("parameters");
            Map<String, String> parametersMap =
                    isNull(parametersNode)
                            ? Collections.emptyMap()
                            : JsonSerdeUtil.convertValue(
                                    parametersNode,
                                    new TypeReference<HashMap<String, String>>() {});

            parameters.put(field, parametersMap);
        }
    }

    @Nullable
    private String getString(JsonNode node, String fieldName) {
        JsonNode fieldValue = node.get(fieldName);
        return isNull(fieldValue) ? null : fieldValue.asText();
    }

    @Override
    protected Map<String, String> extractRowData(
            JsonNode record, LinkedHashMap<String, DataType> paimonFieldTypes) {
        if (!hasSchema) {
            return super.extractRowData(record, paimonFieldTypes);
        }

        Map<String, Object> recordMap =
                JsonSerdeUtil.convertValue(record, new TypeReference<Map<String, Object>>() {});
        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
            String fieldName = entry.getKey();
            String rawValue = Objects.toString(entry.getValue(), null);
            String debeziumType = debeziumTypes.get(fieldName);
            String className = classNames.get(fieldName);

            String transformed =
                    DebeziumSchemaUtils.transformRawValue(
                            rawValue,
                            debeziumType,
                            className,
                            typeMapping,
                            record.get(fieldName),
                            ZoneOffset.UTC);
            resultMap.put(fieldName, transformed);

            paimonFieldTypes.put(
                    fieldName,
                    DebeziumSchemaUtils.toDataType(
                            debeziumType, className, parameters.get(fieldName)));
        }

        evalComputedColumns(resultMap, paimonFieldTypes);

        return resultMap;
    }

    @Override
    protected String primaryField() {
        return FIELD_PRIMARY;
    }

    @Override
    protected String dataField() {
        return FIELD_AFTER;
    }

    @Nullable
    @Override
    protected String getTableName() {
        return getFromSourceField(FIELD_TABLE);
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        return getFromSourceField(FIELD_DB);
    }

    @Override
    protected String format() {
        return "debezium-json";
    }

    @Nullable
    private String getFromSourceField(String key) {
        JsonNode node = root.get(FIELD_SOURCE);
        if (isNull(node)) {
            return null;
        }

        node = node.get(key);
        return isNull(node) ? null : node.asText();
    }
}
