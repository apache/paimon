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
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/** {@link EventParser} for MySQL Debezium JSON. */
public class MySqlDebeziumJsonEventParser implements EventParser<String> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumJsonEventParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final TableNameConverter tableNameConverter;
    private final List<ComputedColumn> computedColumns;
    private final NewTableSchemaBuilder<TableChanges.TableChange> schemaBuilder;
    @Nullable private final Pattern includingPattern;
    @Nullable private final Pattern excludingPattern;
    private final Set<String> includedTables = new HashSet<>();
    private final Set<String> excludedTables = new HashSet<>();
    private final TypeMapping typeMapping;

    private DebeziumEvent root;

    // NOTE: current table name is not converted by tableNameConverter
    private String currentTable;
    private boolean shouldSynchronizeCurrentTable;
    private final CdcMetadataConverter[] metadataConverters;

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this(
                serverTimeZone,
                caseSensitive,
                computedColumns,
                new TableNameConverter(caseSensitive),
                new MySqlTableSchemaBuilder(new HashMap<>(), caseSensitive, typeMapping),
                null,
                null,
                typeMapping,
                metadataConverters);
    }

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            TableNameConverter tableNameConverter,
            NewTableSchemaBuilder<TableChanges.TableChange> schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this(
                serverTimeZone,
                caseSensitive,
                Collections.emptyList(),
                tableNameConverter,
                schemaBuilder,
                includingPattern,
                excludingPattern,
                typeMapping,
                metadataConverters);
    }

    public MySqlDebeziumJsonEventParser(
            ZoneId serverTimeZone,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            TableNameConverter tableNameConverter,
            NewTableSchemaBuilder<TableChanges.TableChange> schemaBuilder,
            @Nullable Pattern includingPattern,
            @Nullable Pattern excludingPattern,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this.serverTimeZone = serverTimeZone;
        this.caseSensitive = caseSensitive;
        this.computedColumns = computedColumns;
        this.tableNameConverter = tableNameConverter;
        this.schemaBuilder = schemaBuilder;
        this.includingPattern = includingPattern;
        this.excludingPattern = excludingPattern;
        this.typeMapping = typeMapping;
        this.metadataConverters = metadataConverters;
    }

    @Override
    public void setRawEvent(String rawEvent) {
        try {
            objectMapper
                    .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            root = objectMapper.readValue(rawEvent, DebeziumEvent.class);
            currentTable = root.payload().source().table();
            shouldSynchronizeCurrentTable = shouldSynchronizeCurrentTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String parseTableName() {
        return tableNameConverter.convert(Identifier.create(getDatabaseName(), currentTable));
    }

    @Override
    public List<DataField> parseSchemaChange() {
        if (!shouldSynchronizeCurrentTable || !root.payload().isSchemaChange()) {
            return Collections.emptyList();
        }

        DebeziumEvent.Payload payload = root.payload();
        if (!payload.hasHistoryRecord()) {
            return Collections.emptyList();
        }

        TableChanges.TableChange tableChange = null;
        try {
            Iterator<TableChanges.TableChange> tableChanges = payload.getTableChanges();
            long count;
            for (count = 0L; tableChanges.hasNext(); ++count) {
                tableChange = tableChanges.next();
            }
            if (count != 1) {
                LOG.error(
                        "Invalid historyRecord, because tableChanges should contain exactly 1 item.\n"
                                + payload.historyRecord());
                return Collections.emptyList();
            }
        } catch (Exception e) {
            LOG.info("Failed to parse history record for schema changes", e);
            return Collections.emptyList();
        }

        Optional<Schema> schema = schemaBuilder.build(tableChange);
        return schema.get().fields();
    }

    @Override
    public Optional<Schema> parseNewTable() {
        if (!shouldSynchronizeCurrentTable) {
            return Optional.empty();
        }

        DebeziumEvent.Payload payload = root.payload();
        if (!payload.hasHistoryRecord()) {
            return Optional.empty();
        }

        try {
            TableChanges.TableChange tableChange = null;
            Iterator<TableChanges.TableChange> tableChanges = payload.getTableChanges();
            long count;
            for (count = 0L; tableChanges.hasNext(); ++count) {
                tableChange = tableChanges.next();
            }
            if (count != 1) {
                LOG.error(
                        "Invalid historyRecord, because tableChanges should contain exactly 1 item.\n"
                                + payload.historyRecord());
                return Optional.empty();
            }

            if (TableChanges.TableChangeType.CREATE != tableChange.getType()) {
                return Optional.empty();
            }

            List<String> primaryKeyColumnNames = tableChange.getTable().primaryKeyColumnNames();
            if (primaryKeyColumnNames.isEmpty()) {
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
        if (!shouldSynchronizeCurrentTable || root.payload().isSchemaChange()) {
            return Collections.emptyList();
        }
        List<CdcRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(root.payload().before());
        if (!before.isEmpty()) {
            before = mapKeyCaseConvert(before, caseSensitive, recordKeyDuplicateErrMsg(before));
            records.add(new CdcRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(root.payload().after());
        if (!after.isEmpty()) {
            after = mapKeyCaseConvert(after, caseSensitive, recordKeyDuplicateErrMsg(after));
            records.add(new CdcRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private String getDatabaseName() {
        return root.payload().source().db();
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        if (recordRow == null) {
            return new HashMap<>();
        }

        DebeziumEvent.Field schema =
                Preconditions.checkNotNull(
                        root.schema(),
                        "MySqlDebeziumJsonEventParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, DebeziumEvent.Field> fields = schema.beforeAndAfterFields();

        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, DebeziumEvent.Field> field : fields.entrySet()) {
            String fieldName = field.getKey();
            String mySqlType = field.getValue().type();
            JsonNode objectValue = recordRow.get(fieldName);
            if (isNull(objectValue)) {
                continue;
            }

            String className = field.getValue().name();
            String oldValue = objectValue.asText();
            String newValue = oldValue;

            if (Bits.LOGICAL_NAME.equals(className)) {
                // transform little-endian form to normal order
                // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
                byte[] littleEndian = Base64.getDecoder().decode(oldValue);
                byte[] bigEndian = new byte[littleEndian.length];
                for (int i = 0; i < littleEndian.length; i++) {
                    bigEndian[i] = littleEndian[littleEndian.length - 1 - i];
                }
                if (typeMapping.containsMode(TO_STRING)) {
                    newValue = StringUtils.bytesToBinaryString(bigEndian);
                } else {
                    newValue = Base64.getEncoder().encodeToString(bigEndian);
                }
            } else if (("bytes".equals(mySqlType) && className == null)) {
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
            }
            // pay attention to the temporal types
            // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-temporal-types
            else if (Date.SCHEMA_NAME.equals(className)) {
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
                try {
                    byte[] wkb = objectValue.get(Geometry.WKB_FIELD).binaryValue();
                    newValue = MySqlTypeUtils.convertWkbArray(wkb);
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            String.format("Failed to convert %s to geometry JSON.", objectValue),
                            e);
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

        for (CdcMetadataConverter metadataConverter : metadataConverters) {
            resultMap.put(
                    metadataConverter.columnName(),
                    metadataConverter.read(root.payload().source()));
        }

        return resultMap;
    }

    private boolean shouldSynchronizeCurrentTable() {
        // When database DDL operation, the current table is null.
        if (currentTable == null) {
            return false;
        }

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
