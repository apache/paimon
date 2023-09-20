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

package org.apache.paimon.flink.action.cdc.kafka.format.canal;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.format.RecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

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
public class CanalRecordParser extends RecordParser {

    private static final String FIELD_IS_DDL = "isDdl";
    private static final String FIELD_MYSQL_TYPE = "mysqlType";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_OLD = "old";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";

    @Override
    protected boolean isDDL() {
        return extractBooleanFromRootJson(FIELD_IS_DDL);
    }

    public CanalRecordParser(
            boolean caseSensitive,
            TypeMapping typeMapping,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        super(caseSensitive, typeMapping, tableNameConverter, computedColumns);
    }

    @Override
    protected void extractFieldTypesFromDatabaseSchema() {
        JsonNode schema = root.get(FIELD_MYSQL_TYPE);
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();

        schema.fieldNames()
                .forEachRemaining(
                        fieldName -> {
                            String fieldType = schema.get(fieldName).asText();
                            fieldTypes.put(applyCaseSensitiveFieldName(fieldName), fieldType);
                        });
        this.fieldTypes = fieldTypes;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        if (extractBooleanFromRootJson(FIELD_IS_DDL)) {
            return Collections.emptyList();
        }
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        ArrayNode arrayData = JsonSerdeUtil.getNodeAs(root, fieldData, ArrayNode.class);
        String type = extractStringFromRootJson(FIELD_TYPE);
        for (JsonNode data : arrayData) {
            switch (type) {
                case OP_UPDATE:
                    ArrayNode oldArrayData =
                            JsonSerdeUtil.getNodeAs(root, FIELD_OLD, ArrayNode.class);
                    Map<JsonNode, JsonNode> matchedOldRecords =
                            matchOldRecords(arrayData, oldArrayData);
                    JsonNode old = matchedOldRecords.get(data);
                    processRecord(mergeOldRecord(data, old), RowKind.DELETE, records);
                    processRecord(data, RowKind.INSERT, records);
                    break;
                case OP_INSERT:
                    processRecord(data, RowKind.INSERT, records);
                    break;
                case OP_DELETE:
                    processRecord(data, RowKind.DELETE, records);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown record operation: " + type);
            }
        }
        return records;
    }

    @Override
    protected LinkedHashMap<String, DataType> setPaimonFieldType() {
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        fieldTypes.forEach(
                (name, type) ->
                        paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type, typeMapping)));
        return paimonFieldTypes;
    }

    @Override
    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Only supports canal-json format,"
                        + "please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_DATABASE), errorMessageTemplate, FIELD_DATABASE);
        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);
        checkNotNull(root.get(fieldData), errorMessageTemplate, fieldData);
        checkNotNull(root.get(FIELD_IS_DDL), errorMessageTemplate, FIELD_IS_DDL);

        if (!extractBooleanFromRootJson(FIELD_IS_DDL)) {
            checkNotNull(root.get(FIELD_MYSQL_TYPE), errorMessageTemplate, FIELD_MYSQL_TYPE);
            checkNotNull(root.get(fieldPrimaryKeys), errorMessageTemplate, fieldPrimaryKeys);
        }
    }

    @Override
    protected void setPrimaryField() {
        fieldPrimaryKeys = "pkNames";
    }

    @Override
    protected void setDataField() {
        fieldData = "data";
    }

    @Override
    protected Map<String, String> extractRowData(
            JsonNode record, LinkedHashMap<String, DataType> paimonFieldTypes) {
        fieldTypes.forEach(
                (name, type) ->
                        paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type, typeMapping)));
        Map<String, Object> jsonMap =
                OBJECT_MAPPER.convertValue(record, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap =
                fieldTypes.entrySet().stream()
                        .filter(entry -> jsonMap.get(entry.getKey()) != null)
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                transformValue(
                                                        jsonMap.get(entry.getKey()).toString(),
                                                        entry.getValue())));

        // generate values for computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
            paimonFieldTypes.put(computedColumn.columnName(), computedColumn.columnType());
        }
        return resultMap;
    }

    private Map<JsonNode, JsonNode> matchOldRecords(ArrayNode newData, ArrayNode oldData) {
        return IntStream.range(0, newData.size())
                .boxed()
                .collect(Collectors.toMap(newData::get, oldData::get));
    }

    private String transformValue(String oldValue, String mySqlType) {
        String shortType = MySqlTypeUtils.getShortType(mySqlType);

        if (MySqlTypeUtils.isSetType(shortType)) {
            return CanalFieldParser.convertSet(oldValue, mySqlType);
        }

        if (MySqlTypeUtils.isEnumType(shortType)) {
            return CanalFieldParser.convertEnum(oldValue, mySqlType);
        }

        if (MySqlTypeUtils.isGeoType(shortType)) {
            try {
                byte[] wkb =
                        CanalFieldParser.convertGeoType2WkbArray(
                                oldValue.getBytes(StandardCharsets.ISO_8859_1));
                return MySqlTypeUtils.convertWkbArray(wkb);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", oldValue), e);
            }
        }
        return oldValue;
    }
}
