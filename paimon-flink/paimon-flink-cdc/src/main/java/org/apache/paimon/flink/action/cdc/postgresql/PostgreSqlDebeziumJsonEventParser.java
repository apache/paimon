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

/* This file is based on source code from JsonDebeziumSchemaSerializer in the doris-flink-connector
 * (https://github.com/apache/doris-flink-connector/), licensed by the Apache Software Foundation (ASF) under the
 * Apache License, Version 2.0. See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership. */

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link EventParser} for PostgreSQL Debezium JSON. */
public class PostgreSqlDebeziumJsonEventParser implements EventParser<String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    private final List<ComputedColumn> computedColumns;

    private JsonNode payload;
    private Map<String, String> postgreSqlFieldTypes;
    private Map<String, String> fieldClassNames;

    public PostgreSqlDebeziumJsonEventParser(
            ZoneId serverTimeZone, boolean caseSensitive, List<ComputedColumn> computedColumns) {
        this(serverTimeZone, caseSensitive, computedColumns, new TableNameConverter(caseSensitive));
    }

    public PostgreSqlDebeziumJsonEventParser(
            ZoneId serverTimeZone, boolean caseSensitive, TableNameConverter tableNameConverter) {
        this(serverTimeZone, caseSensitive, Collections.emptyList(), tableNameConverter);
    }

    public PostgreSqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            TableNameConverter tableNameConverter) {
        this.serverTimeZone = serverTimeZone;
        this.caseSensitive = caseSensitive;
        this.computedColumns = computedColumns;
        this.tableNameConverter = tableNameConverter;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            JsonNode root = objectMapper.readValue(rawEvent, JsonNode.class);
            JsonNode schema =
                    Preconditions.checkNotNull(
                            root.get("schema"),
                            "PostgreSqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                    + "Please make sure that `includeSchema` is true "
                                    + "in the JsonDebeziumDeserializationSchema you created");
            payload = root.get("payload");

            updateFieldTypes(schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String parseTableName() {
        String tableName = payload.get("source").get("table").asText();
        return tableNameConverter.convert(tableName);
    }

    private void updateFieldTypes(JsonNode schema) {
        postgreSqlFieldTypes = new HashMap<>();
        fieldClassNames = new HashMap<>();
        JsonNode arrayNode = schema.get("fields");
        for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode elementNode = arrayNode.get(i);
            String field = elementNode.get("field").asText();
            if ("before".equals(field) || "after".equals(field)) {
                JsonNode innerArrayNode = elementNode.get("fields");
                for (int j = 0; j < innerArrayNode.size(); j++) {
                    JsonNode innerElementNode = innerArrayNode.get(j);
                    String fieldName = innerElementNode.get("field").asText();
                    String fieldType = innerElementNode.get("type").asText();
                    postgreSqlFieldTypes.put(fieldName, fieldType);
                    if (innerElementNode.get("name") != null) {
                        String className = innerElementNode.get("name").asText();
                        fieldClassNames.put(fieldName, className);
                    }
                }
            }
        }
    }

    @Override
    public List<DataField> parseSchemaChange() {
        return Collections.emptyList();
    }

    @Override
    public List<CdcRecord> parseRecords() {
        List<CdcRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(payload.get("before"));
        if (before.size() > 0) {
            before = caseSensitive ? before : keyCaseInsensitive(before);
            records.add(new CdcRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(payload.get("after"));
        if (after.size() > 0) {
            after = caseSensitive ? after : keyCaseInsensitive(after);
            records.add(new CdcRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        // the geometry, point type can not be converted to string, so we convert it to Object
        // first.
        Map<String, Object> jsonMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
        if (jsonMap == null) {
            return new HashMap<>();
        }

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : postgreSqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String postgreSqlType = field.getValue();
            Object objectValue = jsonMap.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String className = fieldClassNames.get(fieldName);
            String oldValue = objectValue.toString();
            String newValue = oldValue;

            if ("bytes".equals(postgreSqlType) && className == null) {
                // PostgreSQL bytea
                newValue = new String(Base64.getDecoder().decode(oldValue));
            } else if ("bytes".equals(postgreSqlType)
                    && "org.apache.kafka.connect.data.Decimal".equals(className)) {
                // PostgreSQL numeric, decimal, money
                try {
                    new BigDecimal(oldValue);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            "Invalid big decimal value "
                                    + oldValue
                                    + ". Make sure that in the `customConverterConfigs` "
                                    + "of the JsonDebeziumDeserializationSchema you created, set '"
                                    + JsonConverterConfig.DECIMAL_FORMAT_CONFIG
                                    + "' to 'numeric'",
                            e);
                }
            } else if ("io.debezium.time.Date".equals(className)) {
                // PostgreSQL date
                newValue = DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)).toString();
            } else if ("io.debezium.time.Timestamp".equals(className)) {
                // PostgreSQL timestamp (precision 0-3)
                LocalDateTime localDateTime =
                        Instant.ofEpochMilli(Long.parseLong(oldValue))
                                .atZone(ZoneOffset.UTC)
                                .toLocalDateTime();
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
            } else if ("io.debezium.time.MicroTimestamp".equals(className)) {
                // PostgreSQL timestamp (precision 4-6)
                long microseconds = Long.parseLong(oldValue);
                long microsecondsPerSecond = 1_000_000;
                long nanosecondsPerMicros = 1_000;
                long seconds = microseconds / microsecondsPerSecond;
                long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

                LocalDateTime localDateTime =
                        Instant.ofEpochSecond(seconds, nanoAdjustment)
                                .atZone(ZoneOffset.UTC)
                                .toLocalDateTime();
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
            } else if ("io.debezium.time.MicroTime".equals(className)) {
                long microseconds = Long.parseLong(oldValue);
                long microsecondsPerSecond = 1_000_000;
                long nanosecondsPerMicros = 1_000;
                long seconds = microseconds / microsecondsPerSecond;
                long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

                newValue =
                        Instant.ofEpochSecond(seconds, nanoAdjustment)
                                .atZone(ZoneOffset.UTC)
                                .toLocalTime()
                                .toString();
            } else if ("io.debezium.data.geometry.Point".equals(className)
                    || "io.debezium.data.geometry.Geography".equals(className)
                    || "io.debezium.data.geometry.Geometry".equals(className)) {
                JsonNode jsonNode = recordRow.get(fieldName);
                try {
                    byte[] wkb = jsonNode.get("wkb").binaryValue();
                    newValue = PostgreSqlTypeUtils.convertWkbArray(wkb);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Failed to convert %s to geometry JSON.", jsonNode), e);
                }
            }

            resultMap.put(fieldName, newValue);
        }

        // generate values of computed columns
        for (ComputedColumn computedColumn : computedColumns) {
            resultMap.put(
                    computedColumn.columnName(),
                    computedColumn.eval(resultMap.get(computedColumn.fieldReference())));
        }

        return resultMap;
    }

    private Map<String, String> keyCaseInsensitive(Map<String, String> origin) {
        Map<String, String> keyCaseInsensitive = new HashMap<>();
        for (Map.Entry<String, String> entry : origin.entrySet()) {
            String fieldName = entry.getKey().toLowerCase();
            checkArgument(
                    !keyCaseInsensitive.containsKey(fieldName),
                    "Duplicate key appears when converting map keys to case-insensitive form. Original map is:\n%s",
                    origin);
            keyCaseInsensitive.put(fieldName, entry.getValue());
        }
        return keyCaseInsensitive;
    }
}
