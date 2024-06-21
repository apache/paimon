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

package org.apache.paimon.flink.action.cdc.format.canal;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.RecordParser;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.flink.api.java.tuple.Tuple3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
public class CanalRecordParser extends RecordParser {

    private static final Logger LOG = LoggerFactory.getLogger(CanalRecordParser.class);

    private static final String FIELD_IS_DDL = "isDdl";
    private static final String FIELD_MYSQL_TYPE = "mysqlType";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_OLD = "old";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_ROW = "ROW";

    @Override
    protected boolean isDDL() {
        JsonNode node = root.get(FIELD_IS_DDL);
        return !isNull(node) && node.asBoolean();
    }

    public CanalRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        if (isDDL()) {
            return Collections.emptyList();
        }

        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        ArrayNode arrayData = getNodeAs(root, dataField(), ArrayNode.class);
        checkNotNull(arrayData, dataField());

        String type = getAndCheck(FIELD_TYPE).asText();

        for (JsonNode data : arrayData) {
            switch (type) {
                case OP_UPDATE:
                    ArrayNode oldArrayData = getNodeAs(root, FIELD_OLD, ArrayNode.class);
                    checkNotNull(oldArrayData, FIELD_OLD, FIELD_TYPE, type);

                    Map<JsonNode, JsonNode> matchedOldRecords =
                            matchOldRecords(arrayData, oldArrayData);
                    JsonNode old = matchedOldRecords.get(data);
                    processRecord(mergeOldRecord(data, old), RowKind.DELETE, records);
                    processRecord(data, RowKind.INSERT, records);
                    break;
                case OP_INSERT:
                case OP_ROW:
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

    @Nullable
    private LinkedHashMap<String, String> tryExtractOriginalFieldTypes() {
        JsonNode schema = root.get(FIELD_MYSQL_TYPE);
        if (isNull(schema)) {
            LOG.debug(
                    "Cannot get original field types because '{}' field is missing.",
                    FIELD_MYSQL_TYPE);
            return null;
        }

        return JsonSerdeUtil.convertValue(
                schema, new TypeReference<LinkedHashMap<String, String>>() {});
    }

    @Override
    protected String primaryField() {
        return "pkNames";
    }

    @Override
    protected String dataField() {
        return "data";
    }

    @Override
    protected Map<String, String> extractRowData(JsonNode record, RowType.Builder rowTypeBuilder) {
        LinkedHashMap<String, String> originalFieldTypes = tryExtractOriginalFieldTypes();
        Map<String, Object> recordMap =
                JsonSerdeUtil.convertValue(record, new TypeReference<Map<String, Object>>() {});
        Map<String, String> rowData = new HashMap<>();

        if (originalFieldTypes != null) {
            for (Map.Entry<String, String> e : originalFieldTypes.entrySet()) {
                String originalName = e.getKey();
                String originalType = e.getValue();
                Tuple3<String, Integer, Integer> typeInfo =
                        MySqlTypeUtils.getTypeInfo(originalType);
                DataType paimonDataType =
                        MySqlTypeUtils.toDataType(
                                typeInfo.f0, typeInfo.f1, typeInfo.f2, typeMapping);
                rowTypeBuilder.field(originalName, paimonDataType);

                String filedValue = Objects.toString(recordMap.get(originalName), null); // get会返回空？
                String newValue = transformValue(filedValue, typeInfo.f0, originalType);
                rowData.put(originalName, newValue);
            }
        } else {
            fillDefaultTypes(record, rowTypeBuilder);
            for (Map.Entry<String, Object> entry : recordMap.entrySet()) {
                rowData.put(entry.getKey(), Objects.toString(entry.getValue(), null));
            }
        }

        evalComputedColumns(rowData, rowTypeBuilder);
        return rowData;
    }

    @Override
    protected String format() {
        return "canal-json";
    }

    private Map<JsonNode, JsonNode> matchOldRecords(ArrayNode newData, ArrayNode oldData) {
        return IntStream.range(0, newData.size())
                .boxed()
                .collect(Collectors.toMap(newData::get, oldData::get));
    }

    private String transformValue(@Nullable String oldValue, String shortType, String mySqlType) {
        if (oldValue == null) {
            return null;
        }

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
