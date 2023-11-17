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

package org.apache.paimon.flink.action.cdc.sqlserver;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.time.Conversions;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Time;
import io.debezium.time.Timestamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * A parser for SqlServer Debezium JSON strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class SqlServerRecordParser implements FlatMapFunction<String, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(SqlServerRecordParser.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ZoneId serverTimeZone;
    private final boolean caseSensitive;
    private final List<ComputedColumn> computedColumns;
    private final TypeMapping typeMapping;
    private DebeziumEvent root;
    private String currentTable;
    private String schemaName;
    private String databaseName;
    private final Set<String> nonPkTables = new HashSet<>();
    private final CdcMetadataConverter[] metadataConverters;

    public SqlServerRecordParser(
            Configuration sqlServerConfig,
            boolean caseSensitive,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this(
                sqlServerConfig,
                caseSensitive,
                Collections.emptyList(),
                typeMapping,
                metadataConverters);
    }

    public SqlServerRecordParser(
            Configuration sqlServerConfig,
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
        String stringifyServerTimeZone =
                sqlServerConfig.get(SqlServerSourceOptions.SERVER_TIME_ZONE);
        this.serverTimeZone =
                stringifyServerTimeZone == null
                        ? ZoneId.systemDefault()
                        : ZoneId.of(stringifyServerTimeZone);
    }

    @Override
    public void flatMap(String rawEvent, Collector<RichCdcMultiplexRecord> out) throws Exception {
        this.root = objectMapper.readValue(rawEvent, DebeziumEvent.class);

        currentTable = root.payload().source().get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        schemaName = root.payload().source().get(AbstractSourceInfo.SCHEMA_NAME_KEY).asText();
        databaseName = root.payload().source().get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();

        if (nonPkTables.contains(currentTable)) {
            return;
        }
        if (root.payload().isSchemaChange()) {
            throw new UnsupportedOperationException(
                    "Temporarily not supporting schema change events");
        }
        convertRecords().forEach(out::collect);
    }

    private List<RichCdcMultiplexRecord> convertRecords() {
        List<RichCdcMultiplexRecord> records = Lists.newArrayList();
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

    private Map<String, String> extractRow(JsonNode record) {
        if (JsonSerdeUtil.isNull(record)) {
            return new HashMap<>();
        }
        DebeziumEvent.Field schema =
                Preconditions.checkNotNull(
                        root.schema(),
                        "SqlServerRecordParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, DebeziumEvent.Field> fields = schema.beforeAndAfterFields();
        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, DebeziumEvent.Field> field : fields.entrySet()) {
            String fieldName = field.getKey();
            JsonNode value = record.get(fieldName);
            if (isNull(value)) {
                continue;
            }
            String newValue = this.maybeLogicalType(field.getValue(), value);
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

    private String maybeLogicalType(DebeziumEvent.Field field, JsonNode value) {
        // https://debezium.io/documentation/reference/stable/connectors/sqlserver.html#sqlserver-data-types
        String logicalClassName = field.name();
        String schemaType = field.type();
        // Extract new value
        String oldValue = value.asText();
        if (isSchemaBytes(schemaType) && logicalClassName == null) {
            return new String(Base64.getDecoder().decode(oldValue));
        }
        if (StringUtils.isBlank(logicalClassName)) {
            return oldValue;
        }
        switch (logicalClassName) {
            case Decimal.LOGICAL_NAME:
                if (isSchemaBytes(schemaType)) {
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
                return oldValue;
            case Date.SCHEMA_NAME:
                return DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)).toString();
            case Timestamp.SCHEMA_NAME:
                LocalDateTime localDateTime =
                        DateTimeUtils.toLocalDateTime(Long.parseLong(oldValue), serverTimeZone);
                return DateTimeUtils.formatLocalDateTime(localDateTime, 3);
            case Time.SCHEMA_NAME:
                long durationMillis = Long.parseLong(oldValue);
                LocalTime localTime =
                        LocalTime.ofNanoOfDay(
                                Duration.of(durationMillis, ChronoUnit.MILLIS).toNanos());
                return localTime.toString();
            case NanoTime.SCHEMA_NAME:
                long durationNanos = Long.parseLong(oldValue);
                LocalTime durationNanosLocalTime =
                        LocalTime.ofNanoOfDay(
                                Duration.of(durationNanos, ChronoUnit.NANOS).toNanos());
                return durationNanosLocalTime.toString();
            case MicroTime.SCHEMA_NAME:
                long durationMicros = Long.parseLong(oldValue);
                LocalTime durationMicrosLocalTime =
                        LocalTime.ofNanoOfDay(
                                Duration.of(durationMicros, ChronoUnit.MICROS).toNanos());
                return durationMicrosLocalTime.toString();
            case MicroTimestamp.SCHEMA_NAME:
                long epochMicros = Long.parseLong(oldValue);
                LocalDateTime epochMicrosLocalDateTime =
                        LocalDateTime.ofInstant(
                                Conversions.toInstantFromMicros(epochMicros), serverTimeZone);
                return DateTimeUtils.formatLocalDateTime(epochMicrosLocalDateTime, 6);
            case NanoTimestamp.SCHEMA_NAME:
                long epochNanos = Long.parseLong(oldValue);
                LocalDateTime epochNanosLocalDateTime =
                        LocalDateTime.ofInstant(toInstantFromNanos(epochNanos), serverTimeZone);
                return DateTimeUtils.formatLocalDateTime(epochNanosLocalDateTime, 7);
            default:
                return oldValue;
        }
    }

    public Instant toInstantFromNanos(long epochNanos) {
        final long epochSeconds = TimeUnit.NANOSECONDS.toSeconds(epochNanos);
        final long adjustment =
                TimeUnit.NANOSECONDS.toNanos(epochNanos % TimeUnit.SECONDS.toNanos(1));
        return Instant.ofEpochSecond(epochSeconds, adjustment);
    }

    private boolean isSchemaBytes(String schemaType) {
        return "bytes".equals(schemaType);
    }

    protected RichCdcMultiplexRecord createRecord(RowKind rowKind, Map<String, String> data) {
        return new RichCdcMultiplexRecord(
                String.format("%s_%s", databaseName, schemaName),
                currentTable,
                new LinkedHashMap<>(0),
                Collections.emptyList(),
                new CdcRecord(rowKind, data));
    }
}
