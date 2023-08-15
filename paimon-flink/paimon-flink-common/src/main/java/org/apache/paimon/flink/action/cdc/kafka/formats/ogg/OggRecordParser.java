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

package org.apache.paimon.flink.action.cdc.kafka.formats.ogg;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Implementation of {@link RecordParser} for parsing messages in the Ogg format.
 *
 * <p>This parser handles records in the Ogg format and extracts relevant information to produce
 * {@link RichCdcMultiplexRecord} objects.
 */
public class OggRecordParser extends RecordParser {

    private static final String FIELD_PRIMARY_KEYS = "primary_keys";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_TYPE = "op_type";
    private static final String OP_UPDATE = "U";
    private static final String OP_INSERT = "I";
    private static final String OP_DELETE = "D";
    private final List<ComputedColumn> computedColumns;

    public OggRecordParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        super(tableNameConverter, caseSensitive);
        this.computedColumns = computedColumns;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        String type = extractString(FIELD_TYPE);
        switch (type) {
            case OP_UPDATE:
                processRecord(root.get(FIELD_BEFORE), RowKind.UPDATE_BEFORE, records);
                processRecord(root.get(FIELD_AFTER), RowKind.UPDATE_AFTER, records);
                break;
            case OP_INSERT:
                processRecord(root.get(FIELD_AFTER), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(root.get(FIELD_BEFORE), RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + type);
        }
        return records;
    }

    private void processRecord(
            JsonNode jsonNode, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        Map<String, String> rowData = extractRow(jsonNode, paimonFieldTypes);
        rowData = caseSensitive ? rowData : keyCaseInsensitive(rowData);
        records.add(createRecord(rowKind, rowData, paimonFieldTypes));
    }

    private RichCdcMultiplexRecord createRecord(
            RowKind rowKind,
            Map<String, String> data,
            LinkedHashMap<String, DataType> paimonFieldTypes) {
        return new RichCdcMultiplexRecord(
                databaseName,
                tableName,
                paimonFieldTypes,
                extractPrimaryKeys(FIELD_PRIMARY_KEYS),
                new CdcRecord(rowKind, data));
    }

    @Override
    public KafkaSchema getKafkaSchema(String record) {
        try {
            root = OBJECT_MAPPER.readValue(record, JsonNode.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON: " + record, e);
        }
        validateFormat();

        LinkedHashMap<String, DataType> paimonFieldTypes = extractFieldTypesFromOracleType();

        return new KafkaSchema(
                databaseName, tableName, paimonFieldTypes, extractPrimaryKeys(FIELD_PRIMARY_KEYS));
    }

    @Override
    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);
        checkNotNull(root.get(FIELD_PRIMARY_KEYS), errorMessageTemplate, FIELD_PRIMARY_KEYS);

        String fieldType = root.get(FIELD_TYPE).asText();

        switch (fieldType) {
            case OP_UPDATE:
                checkNotNull(root.get(FIELD_AFTER), errorMessageTemplate, FIELD_AFTER);
                checkNotNull(root.get(FIELD_BEFORE), errorMessageTemplate, FIELD_BEFORE);
                break;
            case OP_INSERT:
                checkNotNull(root.get(FIELD_AFTER), errorMessageTemplate, FIELD_AFTER);
                break;
            case OP_DELETE:
                checkNotNull(root.get(FIELD_BEFORE), errorMessageTemplate, FIELD_BEFORE);
                checkNotNull(root.get(FIELD_AFTER), errorMessageTemplate, "null");
                break;
        }
    }

    @Override
    protected String extractString(String key) {
        if (key.equals(FIELD_TABLE)) {
            extractDatabaseAndTableNames();
            return tableName;
        } else if (key.equals(FIELD_DATABASE)) {
            extractDatabaseAndTableNames();
            return databaseName;
        }
        return root.get(key) != null ? root.get(key).asText() : null;
    }

    private void extractDatabaseAndTableNames() {
        JsonNode tableNode = root.get(FIELD_TABLE);
        if (tableNode != null) {
            String[] dbt = tableNode.asText().split("\\.", 2); // Limit split to 2 parts
            if (dbt.length == 2) {
                databaseName = dbt[0];
                tableName = dbt[1];
            }
        }
    }

    private LinkedHashMap<String, DataType> extractFieldTypesFromOracleType() {
        LinkedHashMap<String, DataType> fieldTypes = new LinkedHashMap<>();

        JsonNode record = root.get(FIELD_AFTER);
        Map<String, Object> linkedHashMap =
                OBJECT_MAPPER.convertValue(
                        record, new TypeReference<LinkedHashMap<String, Object>>() {});
        if (linkedHashMap == null) {
            return new LinkedHashMap<>();
        }

        Set<String> keySet = linkedHashMap.keySet();
        String[] columns = keySet.toArray(new String[0]);
        for (String column : columns) {
            fieldTypes.put(toFieldName(column), DataTypes.STRING());
        }
        return fieldTypes;
    }

    private Map<String, String> extractRow(
            JsonNode record, LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, String> linkedHashMap =
                OBJECT_MAPPER.convertValue(
                        record, new TypeReference<LinkedHashMap<String, String>>() {});
        if (linkedHashMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> entry : linkedHashMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            paimonFieldTypes.put(key, DataTypes.STRING());
            resultMap.put(key, value);
        }

        // generate values for computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
        }
        return resultMap;
    }
}
