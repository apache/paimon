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

package org.apache.paimon.flink.action.cdc.kafka.formats.canal;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.kafka.KafkaSchema;
import org.apache.paimon.flink.action.cdc.kafka.formats.RecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.NullNode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Convert canal-json format string to list of {@link RichCdcMultiplexRecord}s. */
public class CanalRecordParser extends RecordParser {

    private static final String FIELD_SQL = "sql";
    private static final String FIELD_MYSQL_TYPE = "mysqlType";
    private static final String FIELD_PRIMARY_KEYS = "pkNames";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATA = "data";
    private static final String FIELD_OLD = "old";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";

    private final List<ComputedColumn> computedColumns;

    public CanalRecordParser(
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            List<ComputedColumn> computedColumns) {
        super(tableNameConverter, caseSensitive);
        this.computedColumns = computedColumns;
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        if (isDdl()) {
            return Collections.emptyList();
        }

        List<String> primaryKeys = extractPrimaryKeys();

        // extract field types
        LinkedHashMap<String, String> mySqlFieldTypes = extractFieldTypesFromMySqlType();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        mySqlFieldTypes.forEach(
                (name, type) -> paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type)));

        // extract row kind and field values
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        String type = extractString(FIELD_TYPE);
        ArrayNode data = (ArrayNode) root.get(FIELD_DATA);
        switch (type) {
            case OP_UPDATE:
                ArrayNode old =
                        root.get(FIELD_OLD) instanceof NullNode
                                ? null
                                : (ArrayNode) root.get(FIELD_OLD);
                for (int i = 0; i < data.size(); i++) {
                    Map<String, String> after =
                            extractRow(data.get(i), mySqlFieldTypes, paimonFieldTypes);
                    if (old != null) {
                        Map<String, String> before =
                                extractRow(old.get(i), mySqlFieldTypes, paimonFieldTypes);
                        // fields in "old" (before) means the fields are changed
                        // fields not in "old" (before) means the fields are not changed,
                        // so we just copy the not changed fields into before
                        for (Map.Entry<String, String> entry : after.entrySet()) {
                            if (!before.containsKey(entry.getKey())) {
                                before.put(entry.getKey(), entry.getValue());
                            }
                        }
                        before = caseSensitive ? before : keyCaseInsensitive(before);
                        records.add(
                                new RichCdcMultiplexRecord(
                                        databaseName,
                                        tableName,
                                        paimonFieldTypes,
                                        primaryKeys,
                                        new CdcRecord(RowKind.DELETE, before)));
                    }
                    after = caseSensitive ? after : keyCaseInsensitive(after);
                    records.add(
                            new RichCdcMultiplexRecord(
                                    databaseName,
                                    tableName,
                                    paimonFieldTypes,
                                    primaryKeys,
                                    new CdcRecord(RowKind.INSERT, after)));
                }
                break;
            case OP_INSERT:
                // fall through
            case OP_DELETE:
                for (JsonNode datum : data) {
                    Map<String, String> after =
                            extractRow(datum, mySqlFieldTypes, paimonFieldTypes);
                    after = caseSensitive ? after : keyCaseInsensitive(after);
                    RowKind kind = type.equals(OP_INSERT) ? RowKind.INSERT : RowKind.DELETE;
                    records.add(
                            new RichCdcMultiplexRecord(
                                    databaseName,
                                    tableName,
                                    paimonFieldTypes,
                                    primaryKeys,
                                    new CdcRecord(kind, after)));
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown record type: " + type);
        }

        return records;
    }

    @Override
    public KafkaSchema getKafkaSchema(String record) {
        try {
            root = OBJECT_MAPPER.readValue(record, JsonNode.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        validateFormat();

        if (isDdl()) {
            return null;
        }

        LinkedHashMap<String, String> mySqlFieldTypes = extractFieldTypesFromMySqlType();
        LinkedHashMap<String, DataType> paimonFieldTypes = new LinkedHashMap<>();
        mySqlFieldTypes.forEach(
                (name, type) -> paimonFieldTypes.put(name, MySqlTypeUtils.toDataType(type)));

        return new KafkaSchema(
                extractString(FIELD_DATABASE),
                extractString(FIELD_TABLE),
                paimonFieldTypes,
                extractPrimaryKeys());
    }

    @Override
    protected void validateFormat() {
        String errorMessageTemplate =
                "Didn't find '%s' node in json. Only supports canal-json format,"
                        + "please make sure your topic's format is correct.";

        checkNotNull(root.get(FIELD_DATABASE), errorMessageTemplate, FIELD_DATABASE);
        checkNotNull(root.get(FIELD_TABLE), errorMessageTemplate, FIELD_TABLE);
        checkNotNull(root.get(FIELD_TYPE), errorMessageTemplate, FIELD_TYPE);
        checkNotNull(root.get(FIELD_DATA), errorMessageTemplate, FIELD_DATA);

        if (isDdl()) {
            checkNotNull(root.get(FIELD_SQL), errorMessageTemplate, FIELD_SQL);
        } else {
            checkNotNull(root.get(FIELD_MYSQL_TYPE), errorMessageTemplate, FIELD_MYSQL_TYPE);
            checkNotNull(root.get(FIELD_PRIMARY_KEYS), errorMessageTemplate, FIELD_PRIMARY_KEYS);
        }
    }

    @Override
    protected String extractString(String key) {
        return root.get(key).asText();
    }

    private boolean isDdl() {
        return root.get("isDdl") != null && root.get("isDdl").asBoolean();
    }

    private List<String> extractPrimaryKeys() {
        List<String> primaryKeys = new ArrayList<>();
        ArrayNode pkNames = (ArrayNode) root.get(FIELD_PRIMARY_KEYS);
        pkNames.iterator().forEachRemaining(pk -> primaryKeys.add(toFieldName(pk.asText())));
        return primaryKeys;
    }

    private LinkedHashMap<String, String> extractFieldTypesFromMySqlType() {
        LinkedHashMap<String, String> fieldTypes = new LinkedHashMap<>();

        JsonNode schema = root.get(FIELD_MYSQL_TYPE);
        Iterator<String> iterator = schema.fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            String fieldType = schema.get(fieldName).asText();
            fieldTypes.put(toFieldName(fieldName), fieldType);
        }

        return fieldTypes;
    }

    private Map<String, String> extractRow(
            JsonNode record,
            Map<String, String> mySqlFieldTypes,
            LinkedHashMap<String, DataType> paimonFieldTypes) {
        Map<String, Object> jsonMap =
                OBJECT_MAPPER.convertValue(record, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : mySqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue();
            Object objectValue = jsonMap.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String oldValue = objectValue.toString();
            String newValue = oldValue;

            if (MySqlTypeUtils.isSetType(MySqlTypeUtils.getShortType(mySqlType))) {
                newValue = CanalFieldParser.convertSet(newValue, mySqlType);
            } else if (MySqlTypeUtils.isEnumType(MySqlTypeUtils.getShortType(mySqlType))) {
                newValue = CanalFieldParser.convertEnum(newValue, mySqlType);
            } else if (MySqlTypeUtils.isGeoType(MySqlTypeUtils.getShortType(mySqlType))) {
                try {
                    byte[] wkb =
                            CanalFieldParser.convertGeoType2WkbArray(
                                    oldValue.getBytes(StandardCharsets.ISO_8859_1));
                    newValue = MySqlTypeUtils.convertWkbArray(wkb);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Failed to convert %s to geometry JSON.", oldValue), e);
                }
            }
            resultMap.put(fieldName, newValue);
        }

        // generate values for computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
            paimonFieldTypes.put(computedColumn.columnName(), computedColumn.columnType());
        }

        return resultMap;
    }
}
