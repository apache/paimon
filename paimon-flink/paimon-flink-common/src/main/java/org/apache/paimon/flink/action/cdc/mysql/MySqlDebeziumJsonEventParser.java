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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.data.Bits;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.relational.history.TableChanges;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** {@link EventParser} for MySQL Debezium JSON. */
public class MySqlDebeziumJsonEventParser implements EventParser<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonEventParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    private final List<ComputedColumn> computedColumns;
    private final NewTableSchemaBuilder<JsonNode> schemaBuilder;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();
    private final boolean convertTinyint1ToBool;

    private JsonNode root;
    private JsonNode payload;
    // NOTE: current table name is not converted by tableNameConverter
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            boolean convertTinyint1ToBool) {
        this(
                serverTimeZone,
                caseSensitive,
                computedColumns,
                new TableNameConverter(caseSensitive),
                ddl -> Optional.empty(),
                null,
                null,
                convertTinyint1ToBool);
    }

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            NewTableSchemaBuilder<JsonNode> schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            boolean convertTinyint1ToBool) {
        this(
                serverTimeZone,
                caseSensitive,
                Collections.emptyList(),
                tableNameConverter,
                schemaBuilder,
                includingPattern,
                excludingPattern,
                convertTinyint1ToBool);
    }

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            TableNameConverter tableNameConverter,
            NewTableSchemaBuilder<JsonNode> schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            boolean convertTinyint1ToBool) {
        this.serverTimeZone = serverTimeZone;
        this.caseSensitive = caseSensitive;
        this.computedColumns = computedColumns;
        this.tableNameConverter = tableNameConverter;
        this.schemaBuilder = schemaBuilder;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.convertTinyint1ToBool = convertTinyint1ToBool;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            root = objectMapper.readValue(rawEvent, JsonNode.class);
            payload = root.get("payload");
            currentTable = payload.get("source").get("table").asText();
            shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String parseTableName() {
        return tableNameConverter.convert(Identifier.create(getDatabaseName(), currentTable));
    }

    private boolean isSchemaChange() {
        return payload.get("op") == null;
    }

    @Override
    public List<DataField> parseSchemaChange() {
        if (!shouldSynchronizeCurrentTable || !isSchemaChange()) {
            return Collections.emptyList();
        }

        JsonNode historyRecord = payload.get("historyRecord");
        if (historyRecord == null) {
            return Collections.emptyList();
        }

        JsonNode columns;
        try {
            String historyRecordString = historyRecord.asText();
            JsonNode tableChanges = objectMapper.readTree(historyRecordString).get("tableChanges");
            if (tableChanges.size() != 1) {
                throw new IllegalArgumentException(
                        "Invalid historyRecord, because tableChanges should contain exactly 1 item.\n"
                                + historyRecordString);
            }
            columns = tableChanges.get(0).get("table").get("columns");
        } catch (Exception e) {
            LOG.info("Failed to parse history record for schema changes", e);
            return Collections.emptyList();
        }
        if (columns == null) {
            return Collections.emptyList();
        }

        List<DataField> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            JsonNode column = columns.get(i);
            JsonNode length = column.get("length");
            JsonNode scale = column.get("scale");
            DataType type =
                    MySqlTypeUtils.toDataType(
                            column.get("typeName").asText(),
                            length == null ? null : length.asInt(),
                            scale == null ? null : scale.asInt(),
                            convertTinyint1ToBool);
            if (column.get("optional").asBoolean()) {
                type = type.nullable();
            } else {
                type = type.notNull();
            }

            String fieldName = column.get("name").asText();
            result.add(new DataField(i, caseSensitive ? fieldName : fieldName.toLowerCase(), type));
        }
        return result;
    }

    @Override
    public Optional<Schema> parseNewTable() {
        if (!shouldSynchronizeCurrentTable) {
            return Optional.empty();
        }

        JsonNode historyRecord = payload.get("historyRecord");
        if (historyRecord == null) {
            return Optional.empty();
        }

        try {
            String historyRecordString = historyRecord.asText();
            JsonNode tableChanges = objectMapper.readTree(historyRecordString).get("tableChanges");
            if (tableChanges.size() != 1) {
                throw new IllegalArgumentException(
                        "Invalid historyRecord, because tableChanges should contain exactly 1 item.\n"
                                + historyRecord.asText());
            }

            JsonNode tableChange = tableChanges.get(0);
            if (!tableChange
                    .get("type")
                    .asText()
                    .equals(TableChanges.TableChangeType.CREATE.name())) {
                return Optional.empty();
            }

            JsonNode primaryKeyColumnNames = tableChange.get("table").get("primaryKeyColumnNames");
            if (primaryKeyColumnNames.size() == 0) {
                LOG.debug(
                        "Didn't find primary keys from MySQL DDL for table '{}'. "
                                + "This table won't be synchronized.",
                        currentTable);
                excludedTables.add(currentTable);
                shouldSynchronizeCurrentTable = false;
                return Optional.empty();
            }

            return schemaBuilder.build(tableChange);
        } catch (Exception e) {
            LOG.info("Failed to parse history record for schema changes", e);
            return Optional.empty();
        }
    }

    @Override
    public List<CdcRecord> parseRecords() {
        if (!shouldSynchronizeCurrentTable || isSchemaChange()) {
            return Collections.emptyList();
        }
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

    private String getDatabaseName() {
        return payload.get("source").get("db").asText();
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        JsonNode schema =
                Preconditions.checkNotNull(
                        root.get("schema"),
                        "MySqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, String> mySqlFieldTypes = new HashMap<>();
        Map<String, String> fieldClassNames = new HashMap<>();
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

        // the geometry, point type can not be converted to string, so we convert it to Object
        // first.
        Map<String, Object> jsonMap =
                objectMapper.convertValue(recordRow, new TypeReference<Map<String, Object>>() {});
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

            String className = fieldClassNames.get(fieldName);
            String oldValue = objectValue.toString();
            String newValue = oldValue;

            // pay attention to the temporal types
            // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types
            if (("bytes".equals(mySqlType) && className == null)
                    || Bits.LOGICAL_NAME.equals(className)) {
                // MySQL binary, varbinary, blob
                newValue = new String(Base64.getDecoder().decode(oldValue));
            } else if ("bytes".equals(mySqlType) && Decimal.LOGICAL_NAME.equals(className)) {
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
            } else if (Date.SCHEMA_NAME.equals(className)) {
                // MySQL date
                newValue = DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)).toString();
            } else if (Timestamp.SCHEMA_NAME.equals(className)) {
                // MySQL datetime (precision 0-3)

                // display value of datetime is not affected by timezone, see
                // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
                // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
                // for implementation
                LocalDateTime localDateTime =
                        DateTimeUtils.toLocalDateTime(Long.parseLong(oldValue), ZoneOffset.UTC);
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
            } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
                // MySQL datetime (precision 4-6)
                long microseconds = Long.parseLong(oldValue);
                long microsecondsPerSecond = 1_000_000;
                long nanosecondsPerMicros = 1_000;
                long seconds = microseconds / microsecondsPerSecond;
                long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

                // display value of datetime is not affected by timezone, see
                // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
                // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
                // for implementation
                LocalDateTime localDateTime =
                        Instant.ofEpochSecond(seconds, nanoAdjustment)
                                .atZone(ZoneOffset.UTC)
                                .toLocalDateTime();
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
            } else if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
                // MySQL timestamp

                // display value of timestamp is affected by timezone, see
                // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
                // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
                // for implementation
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
                    || Geometry.LOGICAL_NAME.equals(className)) {
                JsonNode jsonNode = recordRow.get(fieldName);
                try {
                    byte[] wkb = jsonNode.get("wkb").binaryValue();
                    newValue = MySqlTypeUtils.convertWkbArray(wkb);
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

    private boolean shouldSynchronizeCurrentTable() {
        if (excludedTables.contains(currentTable)) {
            return false;
        }

        if (includedTables.contains(currentTable)) {
            return true;
        }

        boolean shouldSynchronize = true;
        if (includingPattern != null) {
            shouldSynchronize = includingPattern.matcher(currentTable).matches();
        }
        if (excludingPattern != null) {
            shouldSynchronize =
                    shouldSynchronize && !excludingPattern.matcher(currentTable).matches();
        }
        if (!shouldSynchronize) {
            LOG.debug(
                    "Source table {} won't be synchronized because it was excluded. ",
                    currentTable);
            excludedTables.add(currentTable);
            return false;
        }

        includedTables.add(currentTable);
        return true;
    }
}
