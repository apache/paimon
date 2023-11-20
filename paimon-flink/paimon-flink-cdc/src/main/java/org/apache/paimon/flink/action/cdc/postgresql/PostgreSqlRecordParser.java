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

package org.apache.paimon.flink.action.cdc.postgresql;

import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;

/**
 * A parser for PostgreSql Debezium JSON strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class PostgreSqlRecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    private static final String FIELD_PAYLOAD = "payload";
    private static final String FIELD_SOURCE = "source";
    private static final String FIELD_BEFORE = "before";
    private static final String FIELD_AFTER = "after";
    private static final String FIELD_FIELDS = "fields";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_FIELD = "field";
    private static final String FIELD_DB = "db";
    private static final String FIELD_TABLE = "table";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final List<ComputedColumn> computedColumns;

    private JsonNode root;
    private JsonNode payload;

    private Map<String, String> fieldClassNames;
    private String currentTable;
    private String databaseName;

    public PostgreSqlRecordParser(boolean caseSensitive, Configuration postgresqlConfig) {
        this(caseSensitive, Collections.emptyList(), postgresqlConfig);
    }

    public PostgreSqlRecordParser(
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            Configuration postgresqlConfig) {
        this.caseSensitive = caseSensitive;
        this.computedColumns = computedColumns;
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String stringifyServerTimeZone =
                postgresqlConfig.get(PostgresSourceOptions.SERVER_TIME_ZONE);
        this.serverTimeZone =
                stringifyServerTimeZone == null
                        ? ZoneId.systemDefault()
                        : ZoneId.of(stringifyServerTimeZone);
    }

    @Override
    public void flatMap(String rawEvent, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = objectMapper.readValue(rawEvent, JsonNode.class);
        payload = root.get(FIELD_PAYLOAD);
        databaseName = payload.get(FIELD_SOURCE).get(FIELD_DB).asText();
        currentTable = payload.get(FIELD_SOURCE).get(FIELD_TABLE).asText();
        extractRecords().forEach(out::collect);
    }

    private List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        Map<String, String> before = extractRow(payload.get(FIELD_BEFORE));
        if (!before.isEmpty()) {
            before = mapKeyCaseConvert(before, caseSensitive, recordKeyDuplicateErrMsg(before));
            records.add(createRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(payload.get(FIELD_AFTER));
        if (!after.isEmpty()) {
            after = mapKeyCaseConvert(after, caseSensitive, recordKeyDuplicateErrMsg(after));
            records.add(createRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        if (recordRow == null) {
            return new HashMap<>();
        }

        JsonNode schema =
                Preconditions.checkNotNull(
                        root.get("schema"),
                        "PostgreSqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, String> postgreSqlFieldTypes = beforeAndAfterFields(schema);

        Map<String, String> resultMap = new HashMap<>();
        for (Map.Entry<String, String> field : postgreSqlFieldTypes.entrySet()) {
            String fieldName = field.getKey();
            String postgreSqlType = field.getValue();
            JsonNode objectValue = recordRow.get(fieldName);
            if (objectValue == null) {
                continue;
            }

            String className = fieldClassNames.get(fieldName);
            String oldValue = objectValue.toString();
            String newValue = oldValue;

            if ("bytes".equals(postgreSqlType) && className == null) {
                // PostgreSQL bytea
                newValue = new String(Base64.getDecoder().decode(oldValue));
            } else if ("bytes".equals(postgreSqlType) && Decimal.LOGICAL_NAME.equals(className)) {
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
            } else if (Date.SCHEMA_NAME.equals(className)) {
                // PostgreSQL date
                newValue = DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)).toString();
            } else if (Timestamp.SCHEMA_NAME.equals(className)) {
                // PostgreSQL timestamp (precision 0-3)
                LocalDateTime localDateTime =
                        Instant.ofEpochMilli(Long.parseLong(oldValue))
                                .atZone(ZoneOffset.UTC)
                                .toLocalDateTime();
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
            } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
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
            } else if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
                LocalDateTime localDateTime =
                        Instant.parse(oldValue).atZone(serverTimeZone).toLocalDateTime();
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
            } else if (MicroTime.SCHEMA_NAME.equals(className)) {
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
            } else if (Point.LOGICAL_NAME.equals(className)
                    || Geography.LOGICAL_NAME.equals(className)
                    || Geometry.LOGICAL_NAME.equals(className)) {
                JsonNode jsonNode = recordRow.get(fieldName);
                try {
                    byte[] wkb = jsonNode.get(Geometry.WKB_FIELD).binaryValue();
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

    protected RichCdcMultiplexRecord createRecord(RowKind rowKind, Map<String, String> data) {
        return new RichCdcMultiplexRecord(
                databaseName,
                currentTable,
                new LinkedHashMap<>(0),
                Collections.emptyList(),
                new CdcRecord(rowKind, data));
    }

    private Map<String, String> beforeAndAfterFields(JsonNode schema) {
        Map<String, String> postgreSqlFieldTypes = new HashMap<>();
        fieldClassNames = new HashMap<>();
        JsonNode arrayNode = schema.get(FIELD_FIELDS);
        arrayNode.forEach(
                elementNode -> {
                    String field = elementNode.get(FIELD_FIELD).asText();
                    if (FIELD_BEFORE.equals(field) || FIELD_AFTER.equals(field)) {
                        JsonNode innerArrayNode = elementNode.get(FIELD_FIELDS);
                        innerArrayNode.forEach(
                                innerElementNode -> {
                                    String fieldName = innerElementNode.get(FIELD_FIELD).asText();
                                    String fieldType = innerElementNode.get(FIELD_TYPE).asText();
                                    postgreSqlFieldTypes.put(fieldName, fieldType);
                                    if (innerElementNode.hasNonNull(FIELD_NAME)) {
                                        String className =
                                                innerElementNode.get(FIELD_NAME).asText();
                                        fieldClassNames.put(fieldName, className);
                                    }
                                });
                    }
                });
        return postgreSqlFieldTypes;
    }
}
