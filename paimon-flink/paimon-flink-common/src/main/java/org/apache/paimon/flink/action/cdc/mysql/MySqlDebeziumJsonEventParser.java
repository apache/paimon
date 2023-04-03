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

import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link EventParser} for MySQL Debezium JSON.
 *
 * <p>Some implementation is referenced from <a
 * href="https://github.com/apache/doris-flink-connector/blob/master/flink-doris-connector/src/main/java/org/apache/doris/flink/sink/writer/JsonDebeziumSchemaSerializer.java">apache
 * / doris-flink-connector</a>.
 */
public class MySqlDebeziumJsonEventParser implements EventParser<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonEventParser.class);
    private static final String SCHEMA_CHANGE_REGEX =
            "ALTER\\s+TABLE\\s+[^\\s]+\\s+(ADD|DROP|MODIFY)\\s+(COLUMN\\s+)?([^\\s]+)(\\s+([^\\s\\(]+))?\\s*(\\((.*?)\\))?.*";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Pattern schemaChangePattern =
            Pattern.compile(SCHEMA_CHANGE_REGEX, Pattern.CASE_INSENSITIVE);

    private final ZoneId serverTimeZone;

    private JsonNode payload;
    private Map<String, String> mySqlFieldTypes;
    private Map<String, String> fieldClassNames;

    public MySqlDebeziumJsonEventParser() {
        this(ZoneId.systemDefault());
    }

    public MySqlDebeziumJsonEventParser(ZoneId serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            JsonNode root = objectMapper.readValue(rawEvent, JsonNode.class);
            JsonNode schema =
                    Preconditions.checkNotNull(
                            root.get("schema"),
                            "MySqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                    + "Please make sure that `includeSchema` is true "
                                    + "in the JsonDebeziumDeserializationSchema you created");
            payload = root.get("payload");

            if (!isSchemaChange()) {
                updateFieldTypes(schema);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void updateFieldTypes(JsonNode schema) {
        mySqlFieldTypes = new HashMap<>();
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
                    mySqlFieldTypes.put(fieldName, fieldType);
                    if (innerElementNode.get("name") != null) {
                        String className = innerElementNode.get("name").asText();
                        fieldClassNames.put(fieldName, className);
                    }
                }
            }
        }
    }

    @Override
    public boolean isSchemaChange() {
        return payload.get("op") == null;
    }

    @Override
    public List<SchemaChange> getSchemaChanges() {
        JsonNode historyRecord = payload.get("historyRecord");
        if (historyRecord == null) {
            return Collections.emptyList();
        }

        JsonNode ddlNode;
        try {
            ddlNode = objectMapper.readTree(historyRecord.asText()).get("ddl");
        } catch (Exception e) {
            LOG.debug("Failed to parse history record for schema changes", e);
            return Collections.emptyList();
        }
        if (ddlNode == null) {
            return Collections.emptyList();
        }
        String ddl = ddlNode.asText();

        Matcher matcher = schemaChangePattern.matcher(ddl);
        if (matcher.find()) {
            String op = matcher.group(1);
            String column = matcher.group(3);
            String type = matcher.group(5);
            String len = matcher.group(7);
            if ("add".equalsIgnoreCase(op)) {
                return Collections.singletonList(
                        SchemaChange.addColumn(column, MySqlTypeUtils.toDataType(type, len)));
            } else if ("modify".equalsIgnoreCase(op)) {
                return Collections.singletonList(
                        SchemaChange.updateColumnType(
                                column, MySqlTypeUtils.toDataType(type, len)));
            }
        }
        return Collections.emptyList();
    }

    @Override
    public List<CdcRecord> getRecords() {
        List<CdcRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(payload.get("before"));
        if (before.size() > 0) {
            records.add(new CdcRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(payload.get("after"));
        if (after.size() > 0) {
            records.add(new CdcRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        Map<String, String> recordMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, String>>() {});
        if (recordMap == null) {
            return new HashMap<>();
        }

        for (Map.Entry<String, String> field : mySqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue();
            if (recordMap.containsKey(fieldName)) {
                String className = fieldClassNames.get(fieldName);
                String oldValue = recordMap.get(fieldName);
                String newValue = oldValue;

                if (newValue == null) {
                    continue;
                }

                if ("bytes".equals(mySqlType) && className == null) {
                    // MySQL binary, varbinary, blob
                    newValue = new String(Base64.getDecoder().decode(oldValue));
                } else if ("bytes".equals(mySqlType)
                        && "org.apache.kafka.connect.data.Decimal".equals(className)) {
                    // MySQL numeric, fixed, decimal
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
                    // MySQL date
                    newValue = LocalDate.ofEpochDay(Integer.parseInt(oldValue)).toString();
                } else if ("io.debezium.time.Timestamp".equals(className)) {
                    // MySQL datetime
                    newValue =
                            Instant.ofEpochMilli(Long.parseLong(oldValue))
                                    .atZone(serverTimeZone)
                                    .toLocalDateTime()
                                    .toString()
                                    .replace('T', ' ');
                } else if ("io.debezium.time.ZonedTimestamp".equals(className)) {
                    // MySQL timestamp
                    newValue =
                            Instant.parse(oldValue)
                                    .atZone(serverTimeZone)
                                    .toLocalDateTime()
                                    .toString()
                                    .replace('T', ' ');
                }

                recordMap.put(fieldName, newValue);
            }
        }

        return recordMap;
    }
}
