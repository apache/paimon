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

package org.apache.paimon.flink.action.cdc.postgres;

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.format.DebeziumEvent;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Bits;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnCaseConvertAndDuplicateCheck;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.columnDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.mapKeyCaseConvert;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.recordKeyDuplicateErrMsg;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_NULLABLE;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.decimalLogicalName;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;

/**
 * A parser for PostgreSQL Debezium JSON strings, converting them into a list of {@link
 * RichCdcMultiplexRecord}s.
 */
public class PostgresRecordParser
        implements FlatMapFunction<CdcSourceRecord, RichCdcMultiplexRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresRecordParser.class);

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

    public PostgresRecordParser(
            Configuration postgresConfig,
            boolean caseSensitive,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters) {
        this(
                postgresConfig,
                caseSensitive,
                Collections.emptyList(),
                typeMapping,
                metadataConverters);
    }

    public PostgresRecordParser(
            Configuration postgresConfig,
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
        String stringifyServerTimeZone = postgresConfig.get(PostgresSourceOptions.SERVER_TIME_ZONE);
        this.serverTimeZone =
                stringifyServerTimeZone == null
                        ? ZoneId.systemDefault()
                        : ZoneId.of(stringifyServerTimeZone);
    }

    @Override
    public void flatMap(CdcSourceRecord rawEvent, Collector<RichCdcMultiplexRecord> out)
            throws Exception {
        root = objectMapper.readValue((String) rawEvent.getValue(), DebeziumEvent.class);

        currentTable = root.payload().source().get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        databaseName = root.payload().source().get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();

        extractRecords().forEach(out::collect);
    }

    private List<DataField> extractFields(DebeziumEvent.Field schema) {
        Map<String, DebeziumEvent.Field> afterFields = schema.afterFields();
        Preconditions.checkArgument(
                !afterFields.isEmpty(),
                "PostgresRecordParser only supports debezium JSON with schema. "
                        + "Please make sure that `includeSchema` is true "
                        + "in the JsonDebeziumDeserializationSchema you created");

        RowType.Builder rowType = RowType.builder();
        Set<String> existedFields = new HashSet<>();
        Function<String, String> columnDuplicateErrMsg = columnDuplicateErrMsg(currentTable);
        afterFields.forEach(
                (key, value) -> {
                    String columnName =
                            columnCaseConvertAndDuplicateCheck(
                                    key, existedFields, caseSensitive, columnDuplicateErrMsg);

                    DataType dataType = extractFieldType(value);
                    dataType =
                            dataType.copy(
                                    typeMapping.containsMode(TO_NULLABLE) || value.optional());

                    rowType.field(columnName, dataType);
                });
        return rowType.build().getFields();
    }

    /**
     * Extract fields from json records, see <a
     * href="https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-data-types">postgresql-data-types</a>.
     */
    private DataType extractFieldType(DebeziumEvent.Field field) {
        switch (field.type()) {
            case "array":
                return DataTypes.ARRAY(DataTypes.STRING());
            case "map":
            case "struct":
                return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
            case "int8":
                return DataTypes.TINYINT();
            case "int16":
                return DataTypes.SMALLINT();
            case "int32":
                if (Date.SCHEMA_NAME.equals(field.name())) {
                    return DataTypes.DATE();
                }
                return DataTypes.INT();
            case "int64":
                if (MicroTimestamp.SCHEMA_NAME.equals(field.name())) {
                    return DataTypes.TIMESTAMP(6);
                } else if (MicroTime.SCHEMA_NAME.equals(field.name())) {
                    return DataTypes.TIME(6);
                }
                return DataTypes.BIGINT();
            case "float":
            case "float32":
                return DataTypes.FLOAT();
            case "float64":
            case "double":
                return DataTypes.DOUBLE();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "string":
                return DataTypes.STRING();
            case "bytes":
                if (decimalLogicalName().equals(field.name())) {
                    int precision = field.parameters().get("connect.decimal.precision").asInt();
                    int scale = field.parameters().get("scale").asInt();
                    return DataTypes.DECIMAL(precision, scale);
                } else if (Bits.LOGICAL_NAME.equals(field.name())) {
                    String stringifyLength = field.parameters().get("length").asText();
                    if (StringUtils.isBlank(stringifyLength)) {
                        return DataTypes.BOOLEAN();
                    }
                    Integer length = Integer.valueOf(stringifyLength);
                    if (length == 1) {
                        return DataTypes.BOOLEAN();
                    } else {
                        return DataTypes.BINARY(
                                length == Integer.MAX_VALUE ? length / 8 : (length + 7) / 8);
                    }
                }
                // field.name() == null
                return DataTypes.BYTES();
            default:
                // default to String type
                return DataTypes.STRING();
        }
    }

    private List<RichCdcMultiplexRecord> extractRecords() {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();

        Map<String, String> before = extractRow(root.payload().before());
        if (!before.isEmpty()) {
            before = mapKeyCaseConvert(before, caseSensitive, recordKeyDuplicateErrMsg(before));
            records.add(createRecord(RowKind.DELETE, before));
        }

        Map<String, String> after = extractRow(root.payload().after());
        if (!after.isEmpty()) {
            after = mapKeyCaseConvert(after, caseSensitive, recordKeyDuplicateErrMsg(after));
            List<DataField> fields = extractFields(root.schema());
            records.add(
                    new RichCdcMultiplexRecord(
                            databaseName,
                            currentTable,
                            fields,
                            Collections.emptyList(),
                            new CdcRecord(RowKind.INSERT, after)));
        }

        return records;
    }

    private Map<String, String> extractRow(JsonNode recordRow) {
        if (isNull(recordRow)) {
            return new HashMap<>();
        }

        DebeziumEvent.Field schema =
                Preconditions.checkNotNull(
                        root.schema(),
                        "PostgresRecordParser only supports debezium JSON with schema. "
                                + "Please make sure that `includeSchema` is true "
                                + "in the JsonDebeziumDeserializationSchema you created");

        Map<String, DebeziumEvent.Field> fields = schema.beforeAndAfterFields();

        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, DebeziumEvent.Field> field : fields.entrySet()) {
            String fieldName = field.getKey();
            String postgresSqlType = field.getValue().type();
            JsonNode objectValue = recordRow.get(fieldName);
            if (isNull(objectValue)) {
                continue;
            }

            String className = field.getValue().name();
            String oldValue = objectValue.asText();
            String newValue = oldValue;

            if (Bits.LOGICAL_NAME.equals(className)) {
                // transform little-endian form to normal order
                // https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-basic-types
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
            } else if (("bytes".equals(postgresSqlType) && className == null)) {
                // binary, varbinary
                newValue = new String(Base64.getDecoder().decode(oldValue));
            } else if ("bytes".equals(postgresSqlType) && decimalLogicalName().equals(className)) {
                // numeric, decimal
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
            // https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
            else if (Date.SCHEMA_NAME.equals(className)) {
                // date
                newValue = DateTimeUtils.toLocalDate(Integer.parseInt(oldValue)).toString();
            } else if (Timestamp.SCHEMA_NAME.equals(className)) {
                // timestamp (precision 0-3)

                LocalDateTime localDateTime =
                        DateTimeUtils.toLocalDateTime(Long.parseLong(oldValue), ZoneOffset.UTC);
                newValue = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
            } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
                // timestamp (precision 4-6)
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
                // timestamptz

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
            } else if ("array".equals(postgresSqlType)) {
                ArrayNode arrayNode = (ArrayNode) objectValue;
                List<String> newArrayValues = new ArrayList<>();
                arrayNode
                        .elements()
                        .forEachRemaining(
                                element -> {
                                    newArrayValues.add(element.asText());
                                });
                try {
                    newValue = objectMapper.writer().writeValueAsString(newArrayValues);
                } catch (JsonProcessingException e) {
                    LOG.error("Failed to convert array to JSON.", e);
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
                Collections.emptyList(),
                Collections.emptyList(),
                new CdcRecord(rowKind, data));
    }
}
