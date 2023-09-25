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

package org.apache.paimon.flink.action.cdc.kafka.formats;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;

/**
 * Provides a base implementation for parsing messages of various formats into {@link
 * RichCdcMultiplexRecord} objects.
 *
 * <p>This abstract class defines common functionalities and fields required for parsing messages.
 * Subclasses are expected to provide specific implementations for extracting records, validating
 * message formats, and other format-specific operations.
 */
public abstract class RecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    protected static final String FIELD_TABLE = "table";
    protected static final String FIELD_DATABASE = "database";
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    protected final boolean caseSensitive;
    protected final TypeMapping typeMapping;
    protected final List<ComputedColumn> computedColumns;
    protected String fieldPrimaryKeys;
    protected static String fieldData;
    protected List<String> primaryKeys;
    protected LinkedHashMap<String, String> fieldTypes;
    protected JsonNode root;
    protected String databaseName;
    protected String tableName;

    public RecordParser(
            boolean caseSensitive, TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        this.caseSensitive = caseSensitive;
        this.typeMapping = typeMapping;
        this.computedColumns = computedColumns;
    }

    public KafkaSchema getKafkaSchema(String record) {
        this.parseRootJson(record);
        if (this.isDDL()) {
            return null;
        }
        databaseName = extractStringFromRootJson(FIELD_DATABASE);
        tableName = extractStringFromRootJson(FIELD_TABLE);
        this.setPrimaryField();
        this.setDataField();
        this.validateFormat();
        this.extractPrimaryKeys();
        this.extractFieldTypesFromDatabaseSchema();
        LinkedHashMap<String, DataType> paimonFieldTypes = this.setPaimonFieldType();
        return new KafkaSchema(databaseName, tableName, paimonFieldTypes, primaryKeys);
    }

    protected abstract List<RichCdcMultiplexRecord> extractRecords();

    protected abstract void validateFormat();

    protected abstract void setPrimaryField();

    protected abstract void setDataField();

    protected boolean isDDL() {
        return false;
    }

    protected String extractStringFromRootJson(String key) {
        JsonNode node = root.get(key);
        return node == null ? null : node.asText();
    }

    protected Boolean extractBooleanFromRootJson(String key) {
        JsonNode node = root.get(key);
        return node == null ? null : node.asBoolean();
    }

    protected LinkedHashMap<String, DataType> setPaimonFieldType() {
        LinkedHashMap<String, DataType> fieldTypes = new LinkedHashMap<>();
        JsonNode record = root.get(fieldData);
        if (record == null) {
            return fieldTypes;
        }
        Map<String, Object> linkedHashMap =
                OBJECT_MAPPER.convertValue(
                        record, new TypeReference<LinkedHashMap<String, Object>>() {});
        linkedHashMap.forEach(
                (column, value) ->
                        fieldTypes.put(applyCaseSensitiveFieldName(column), DataTypes.STRING()));
        return fieldTypes;
    }

    @Override
    public void flatMap(String value, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = OBJECT_MAPPER.readValue(value, JsonNode.class);
        this.setPrimaryField();
        this.setDataField();
        validateFormat();

        databaseName = extractStringFromRootJson(FIELD_DATABASE);
        tableName = extractStringFromRootJson(FIELD_TABLE);

        extractRecords().forEach(out::collect);
    }

    protected void extractFieldTypesFromDatabaseSchema() {}

    protected Map<String, String> extractRowData(
            JsonNode record, LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, String> linkedHashMap =
                OBJECT_MAPPER.convertValue(
                        record, new TypeReference<LinkedHashMap<String, String>>() {});
        if (linkedHashMap == null) {
            return Collections.emptyMap();
        }

        Map<String, String> resultMap = new HashMap<>();
        linkedHashMap.forEach(
                (key, value) -> {
                    paimonFieldTypes.put(applyCaseSensitiveFieldName(key), DataTypes.STRING());
                    resultMap.put(key, value);
                });

        // generate values for computed columns
        computedColumns.forEach(
                computedColumn -> {
                    resultMap.put(
                            computedColumn.columnName(),
                            computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
                    paimonFieldTypes.put(
                            applyCaseSensitiveFieldName(computedColumn.columnName()),
                            computedColumn.columnType());
                });

        return resultMap;
    }

    protected void extractPrimaryKeys() {
        ArrayNode pkNames = JsonSerdeUtil.getNodeAs(root, fieldPrimaryKeys, ArrayNode.class);
        primaryKeys =
                StreamSupport.stream(pkNames.spliterator(), false)
                        .map(pk -> applyCaseSensitiveFieldName(pk.asText()))
                        .collect(Collectors.toList());
    }

    protected String applyCaseSensitiveFieldName(String rawName) {
        return StringUtils.caseSensitiveConversion(rawName, caseSensitive);
    }

    protected void processRecord(
            JsonNode jsonNode, RowKind rowKind, List<RichCdcMultiplexRecord> records) {
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>(jsonNode.size());
        this.extractPrimaryKeys();
        this.extractFieldTypesFromDatabaseSchema();
        Map<String, String> rowData = this.extractRowData(jsonNode, paimonFieldTypes);
        rowData = mapKeyCaseConvert(rowData, caseSensitive, recordKeyDuplicateErrMsg(rowData));
        records.add(createRecord(rowKind, rowData, paimonFieldTypes));
    }

    protected RichCdcMultiplexRecord createRecord(
            RowKind rowKind,
            Map<String, String> data,
            LinkedHashMap<String, DataType> paimonFieldTypes) {
        return new RichCdcMultiplexRecord(
                databaseName,
                tableName,
                paimonFieldTypes,
                primaryKeys,
                new CdcRecord(rowKind, data));
    }

    protected final void parseRootJson(String record) {
        try {
            root = OBJECT_MAPPER.readValue(record, JsonNode.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error processing JSON: " + record, e);
        }
    }

    protected JsonNode mergeOldRecord(JsonNode data, JsonNode oldNode) {
        JsonNode oldFullRecordNode = data.deepCopy();
        oldNode.fieldNames()
                .forEachRemaining(
                        fieldName ->
                                ((ObjectNode) oldFullRecordNode)
                                        .set(fieldName, oldNode.get(fieldName)));
        return oldFullRecordNode;
    }
}
