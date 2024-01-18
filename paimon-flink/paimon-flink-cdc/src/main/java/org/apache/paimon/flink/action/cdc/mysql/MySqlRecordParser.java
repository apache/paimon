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

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Bits;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnCaseConvertAndDuplicateCheck;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * A parser for MySql Debezium JSON strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class MySqlRecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlRecordParser.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final List<ComputedColumn> computedColumns;
    private final TypeMapping typeMapping;

    private DebeziumEvent root;

    // NOTE: current table name is not converted by tableNameConverter
    private String currentTable;
    private String databaseName;
    private final CdcMetadataConverter[] metadataConverters;

    private final Set<String> nonPkTables = new HashSet<>();

    public MySqlRecordParser(
            Configuration mySqlConfig,
            boolean caseSensitive,
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this.caseSensitive = caseSensitive;
        this.computedColumns = computedColumns;
        this.typeMapping = typeMapping;
        this.metadataConverters = metadataConverters;
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String stringifyServerTimeZone = mySqlConfig.get(MySqlSourceOptions.SERVER_TIME_ZONE);
        this.serverTimeZone =
                stringifyServerTimeZone == null
                        ? ZoneId.systemDefault()
                        : ZoneId.of(stringifyServerTimeZone);
    }

    @Override
    public void flatMap(String rawEvent, Collector<RichCdcMultiplexRecord> out) throws Exception {
        root = objectMapper.readValue(rawEvent, DebeziumEvent.class);
        currentTable = root.payload().source().get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        databaseName = root.payload().source().get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();

        if (root.payload().isSchemaChange()) {
            extractSchemaChange().forEach(out::collect);
            return;
        }
        extractRecords().forEach(out::collect);
    }

    private List<RichCdcMultiplexRecord> extractSchemaChange() {
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
            LOG.error("Failed to parse history record for schema changes", e);
            return Collections.emptyList();
        }

        if (TableChanges.TableChangeType.CREATE == tableChange.getType()
                && tableChange.getTable().primaryKeyColumnNames().isEmpty()) {
            LOG.error(
                    "Didn't find primary keys from MySQL DDL for table '{}'. "
                            + "This table won't be synchronized.",
                    currentTable);
            nonPkTables.add(currentTable);
            return Collections.emptyList();
        }

        Table table = tableChange.getTable();

        LinkedHashMap<String, DataType> fieldTypes = extractFieldTypes(table);
        List<String> primaryKeys = listCaseConvert(table.primaryKeyColumnNames(), caseSensitive);

        // TODO : add table comment and column comment when we upgrade flink cdc to 2.4
        return Collections.singletonList(
                new RichCdcMultiplexRecord(
                        databaseName,
                        currentTable,
                        fieldTypes,
                        primaryKeys,
                        CdcRecord.emptyRecord()));
    }

    private LinkedHashMap<String, DataType> extractFieldTypes(Table table) {
        List<Column> columns = table.columns();
        LinkedHashMap<String, DataType> fieldTypes = new LinkedHashMap<>(columns.size());
        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg =
                columnDuplicateErrMsg(table.id().toString());

        for (Column column : columns) {
            String columnName =
                    columnCaseConvertAndDuplicateCheck(
                            column.name(), existedFields, caseSensitive, columnDuplicateErrMsg);

            DataType dataType =
                    MySqlTypeUtils.toDataType(
                            column.typeExpression(),
                            column.length(),
                            column.scale().orElse(null),
                            typeMapping);
            dataType = dataType.copy(typeMapping.containsMode(TO_NULLABLE) || column.isOptional());

            fieldTypes.put(columnName, dataType);
        }
        return fieldTypes;
    }

    private List<RichCdcMultiplexRecord> extractRecords() {
        if (nonPkTables.contains(currentTable)) {
            return Collections.emptyList();
        }
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(root.payload().before());
        if (!before.isEmpty()) {
            before = mapKeyCaseConvert(before, caseSensitive, recordKeyDuplicateErrMsg(before));
            records.add(createRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(root.payload().after());
        if (!after.isEmpty()) {
            after = mapKeyCaseConvert(after, caseSensitive, recordKeyDuplicateErrMsg(after));
            records.add(createRecord(RowKind.INSERT, after));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        if (JsonSerdeUtil.isNull(recordRow)) {
            return new HashMap<>();
        }

        DebeziumEvent.Field schema =
                Preconditions.checkNotNull(
                        root.schema(),
                        "MySqlRecordParser only supports debezium JSON with schema. "
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

    protected RichCdcMultiplexRecord createRecord(RowKind rowKind, Map<String, String> data) {
        return new RichCdcMultiplexRecord(
                databaseName,
                currentTable,
                new LinkedHashMap<>(0),
                Collections.emptyList(),
                new CdcRecord(rowKind, data));
    }
}
