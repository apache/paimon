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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.data.Bits;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.time.Date;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Timestamp;
import io.debezium.time.ZonedTimestamp;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.connect.json.JsonConverterConfig;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;
import static org.apache.paimon.utils.TypeUtils.OBJECT_MAPPER;

/**
 * Utils to handle 'schema' field in debezium Json. TODO: The methods have many duplicate codes with
 * MySqlRecordParser. Need refactor.
 */
public class DebeziumSchemaUtils {

    public static final String FIELD_SCHEMA = "schema";
    public static final String FIELD_PAYLOAD = "payload";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_PRIMARY = "pkNames";
    public static final String FIELD_DB = "db";

    public static final String FIELD_BEFORE = "before";
    public static final String FIELD_AFTER = "after";

    public static final String FIELD_TYPE = "op";
    public static final String OP_READE = "r";
    public static final String OP_INSERT = "c";
    public static final String OP_UPDATE = "u";
    public static final String OP_DELETE = "d";
    public static final String OP_TRUNCATE = "t";
    public static final String OP_MESSAGE = "m";

    public static String transformRawValue(
            @Nullable String rawValue,
            String debeziumType,
            @Nullable String className,
            TypeMapping typeMapping,
            JsonNode origin,
            ZoneId serverTimeZone) {
        return transformRawValue(
                rawValue,
                debeziumType,
                className,
                typeMapping,
                () -> {
                    try {
                        return ByteBuffer.wrap(origin.get(Geometry.WKB_FIELD).binaryValue());
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                                String.format("Failed to convert %s to geometry JSON.", rawValue),
                                e);
                    }
                },
                origin,
                serverTimeZone);
    }

    public static String transformAvroRawValue(
            @Nullable String rawValue,
            String debeziumType,
            @Nullable String className,
            TypeMapping typeMapping,
            Object origin,
            ZoneId serverTimeZone) {

        if (rawValue != null && "bytes".equals(debeziumType) && className == null) {
            return new String(((ByteBuffer) origin).array());
        }

        return transformRawValue(
                rawValue,
                debeziumType,
                className,
                typeMapping,
                () -> (ByteBuffer) ((GenericRecord) origin).get(Geometry.WKB_FIELD),
                origin,
                serverTimeZone);
    }

    /** Transform raw string value according to schema. */
    public static String transformRawValue(
            @Nullable String rawValue,
            String debeziumType,
            @Nullable String className,
            TypeMapping typeMapping,
            Supplier<ByteBuffer> geometryGetter,
            Object origin,
            ZoneId serverTimeZone) {
        if (rawValue == null) {
            return null;
        }

        String transformed = rawValue;

        if (Bits.LOGICAL_NAME.equals(className)) {
            // transform little-endian form to normal order
            // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-data-types
            byte[] littleEndian = Base64.getDecoder().decode(rawValue);
            byte[] bigEndian = new byte[littleEndian.length];
            for (int i = 0; i < littleEndian.length; i++) {
                bigEndian[i] = littleEndian[littleEndian.length - 1 - i];
            }
            if (typeMapping.containsMode(TO_STRING)) {
                transformed = StringUtils.bytesToBinaryString(bigEndian);
            } else {
                transformed = Base64.getEncoder().encodeToString(bigEndian);
            }
        } else if (("bytes".equals(debeziumType) && className == null)) {
            // MySQL binary, varbinary, blob
            transformed = new String(Base64.getDecoder().decode(rawValue));
        } else if ("bytes".equals(debeziumType) && decimalLogicalName().equals(className)) {
            // MySQL numeric, fixed, decimal
            try {
                new BigDecimal(rawValue);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid big decimal value "
                                + rawValue
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
            transformed = DateTimeUtils.toLocalDate(Integer.parseInt(rawValue)).toString();
        } else if (Timestamp.SCHEMA_NAME.equals(className)) {
            // MySQL datetime (precision 0-3)

            // display value of datetime is not affected by timezone, see
            // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
            // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
            // for implementation
            LocalDateTime localDateTime =
                    DateTimeUtils.toLocalDateTime(Long.parseLong(rawValue), ZoneOffset.UTC);
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 3);
        } else if (MicroTimestamp.SCHEMA_NAME.equals(className)) {
            // MySQL datetime (precision 4-6)
            long microseconds = Long.parseLong(rawValue);
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
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
        } else if (ZonedTimestamp.SCHEMA_NAME.equals(className)) {
            // MySQL timestamp

            // display value of timestamp is affected by timezone, see
            // https://dev.mysql.com/doc/refman/8.0/en/datetime.html for standard, and
            // RowDataDebeziumDeserializeSchema#convertToTimestamp in flink-cdc-connector
            // for implementation
            LocalDateTime localDateTime =
                    Instant.parse(rawValue).atZone(serverTimeZone).toLocalDateTime();
            transformed = DateTimeUtils.formatLocalDateTime(localDateTime, 6);
        } else if (MicroTime.SCHEMA_NAME.equals(className)) {
            long microseconds = Long.parseLong(rawValue);
            long microsecondsPerSecond = 1_000_000;
            long nanosecondsPerMicros = 1_000;
            long seconds = microseconds / microsecondsPerSecond;
            long nanoAdjustment = (microseconds % microsecondsPerSecond) * nanosecondsPerMicros;

            transformed =
                    Instant.ofEpochSecond(seconds, nanoAdjustment)
                            .atZone(ZoneOffset.UTC)
                            .toLocalTime()
                            .toString();
        } else if (Point.LOGICAL_NAME.equals(className)
                || Geometry.LOGICAL_NAME.equals(className)) {
            try {
                transformed = MySqlTypeUtils.convertWkbArray(geometryGetter.get());
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to convert %s to geometry JSON.", rawValue), e);
            }
        } else if ((origin instanceof GenericData.Record)
                || (origin instanceof GenericData.Array)
                || (origin instanceof Map)
                || (origin instanceof List)) {
            Object convertedObject = convertAvroObjectToJsonCompatible(origin);
            try {
                transformed = OBJECT_MAPPER.writer().writeValueAsString(convertedObject);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(
                        String.format("Failed to convert %s to JSON.", origin), e);
            }
        }

        return transformed;
    }

    public static Object convertAvroObjectToJsonCompatible(Object avroObject) {
        if (avroObject instanceof GenericData.Record) {
            return convertRecord((GenericData.Record) avroObject);
        } else if (avroObject instanceof GenericData.Array) {
            return convertArray((GenericData.Array<?>) avroObject);
        } else if (avroObject instanceof Utf8) {
            return avroObject.toString();
        } else if (avroObject instanceof Map) {
            return convertMap((Map<Object, Object>) avroObject);
        } else if (avroObject instanceof List) {
            return convertList((List<Object>) avroObject);
        } else {
            return avroObject;
        }
    }

    private static Map<Object, Object> convertMap(Map<Object, Object> map) {
        Map<Object, Object> newMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            Object key = convertAvroObjectToJsonCompatible(entry.getKey());
            Object value = convertAvroObjectToJsonCompatible(entry.getValue());
            newMap.put(key, value);
        }
        return newMap;
    }

    private static List<Object> convertList(List<Object> list) {
        List<Object> newList = new ArrayList<>();
        for (Object element : list) {
            newList.add(convertAvroObjectToJsonCompatible(element));
        }
        return newList;
    }

    private static Map<String, Object> convertRecord(GenericData.Record record) {
        Map<String, Object> map = new HashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.pos());
            map.put(field.name(), convertAvroObjectToJsonCompatible(value));
        }
        return map;
    }

    private static List<Object> convertArray(GenericData.Array<?> array) {
        List<Object> list = new ArrayList<>();
        for (Object element : array) {
            list.add(convertAvroObjectToJsonCompatible(element));
        }
        return list;
    }

    public static DataType toDataType(
            String debeziumType, @Nullable String className, Map<String, String> parameters) {
        if (className == null) {
            return fromDebeziumType(debeziumType);
        }

        if (Bits.LOGICAL_NAME.equals(className)) {
            int length = Integer.parseInt(parameters.get("length"));
            return DataTypes.BINARY((length + 7) / 8);
        }

        if (decimalLogicalName().equals(className)) {
            String precision = parameters.get("connect.decimal.precision");
            if (precision == null) {
                return DataTypes.DECIMAL(20, 0);
            }

            int p = Integer.parseInt(precision);
            if (p > DecimalType.MAX_PRECISION) {
                return DataTypes.STRING();
            } else {
                int scale = Integer.parseInt(parameters.get("scale"));
                return DataTypes.DECIMAL(p, scale);
            }
        }

        if (Date.SCHEMA_NAME.equals(className)) {
            return DataTypes.DATE();
        }

        if (Timestamp.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIMESTAMP(3);
        }

        if (MicroTimestamp.SCHEMA_NAME.equals(className)
                || ZonedTimestamp.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIMESTAMP(6);
        }

        if (MicroTime.SCHEMA_NAME.equals(className)) {
            return DataTypes.TIME();
        }

        return fromDebeziumType(debeziumType);
    }

    private static DataType fromDebeziumType(String dbzType) {
        switch (dbzType) {
            case "int8":
                return DataTypes.TINYINT();
            case "int16":
                return DataTypes.SMALLINT();
            case "int32":
                return DataTypes.INT();
            case "int64":
                return DataTypes.BIGINT();
            case "float":
            case "float32":
            case "float64":
                return DataTypes.FLOAT();
            case "double":
                return DataTypes.DOUBLE();
            case "boolean":
                return DataTypes.BOOLEAN();
            case "bytes":
                return DataTypes.BYTES();
            case "string":
            default:
                return DataTypes.STRING();
        }
    }

    /**
     * get decimal logical name.
     *
     * <p>Using the maven shade plugin will shade the constant value. see <a
     * href="https://issues.apache.org/jira/browse/MSHADE-156">...</a> so the string
     * org.apache.kafka.connect.data.Decimal is shaded to org.apache.flink.kafka.shaded
     * .org.apache.kafka.connect.data.Decimal.
     */
    public static String decimalLogicalName() {
        return "org.apache.#.connect.data.Decimal".replace("#", "kafka");
    }

    protected static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
    protected static final String CONNECT_NAME_PROP = "connect.name";

    static final String SCHEMA_PARAMETER_COLUMN_TYPE = "__debezium.source.column.type";
    private static final String SCHEMA_PARAMETER_COLUMN_SIZE = "__debezium.source.column.length";
    private static final String SCHEMA_PARAMETER_COLUMN_PRECISION =
            "__debezium.source.column.scale";
    static final String SCHEMA_PARAMETER_COLUMN_NAME = "__debezium.source.column.name";

    public static Map<String, String> getAvroConnectParameters(Schema schema) {
        // Mapping by mysql types
        // Parse actual source column type from connect.parameters if enable debezium property
        // "column.propagate.source.type", otherwise will infer avro schema type mapping to paimon
        if (schema.getObjectProp(CONNECT_PARAMETERS_PROP) != null) {
            return (Map<String, String>) schema.getObjectProp(CONNECT_PARAMETERS_PROP);
        }
        return new HashMap<>();
    }

    public static DataType avroToPaimonDataType(Schema schema) {
        Map<String, String> connectParameters = getAvroConnectParameters(schema);
        if (!connectParameters.isEmpty()) {
            String typeName =
                    connectParameters.getOrDefault(
                            SCHEMA_PARAMETER_COLUMN_TYPE, schema.getType().name());
            Integer length =
                    Optional.ofNullable(connectParameters.get(SCHEMA_PARAMETER_COLUMN_SIZE))
                            .map(Integer::valueOf)
                            .orElse(null);
            Integer scale =
                    Optional.ofNullable(connectParameters.get(SCHEMA_PARAMETER_COLUMN_PRECISION))
                            .map(Integer::valueOf)
                            .orElse(null);
            return MySqlTypeUtils.toDataType(typeName, length, scale, TypeMapping.defaultMapping());
        }

        // Mapping by avro schema type
        return fromDebeziumAvroType(schema);
    }

    private static DataType fromDebeziumAvroType(Schema schema) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null) {
            if (logicalType instanceof LogicalTypes.Date) {
                return DataTypes.DATE();
            } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                return DataTypes.TIMESTAMP_MILLIS();
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                return DataTypes.TIMESTAMP();
            } else if (logicalType instanceof LogicalTypes.Decimal) {
                LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
            } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                return DataTypes.TIME(3);
            } else if (logicalType instanceof LogicalTypes.TimeMicros) {
                return DataTypes.TIME(6);
            } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
            } else {
                throw new UnsupportedOperationException(
                        String.format("Don't support logical avro type '%s' yet.", logicalType));
            }
        }
        Schema.Type avroType = schema.getType();
        switch (avroType) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case BYTES:
            case FIXED:
                return DataTypes.BYTES();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case FLOAT:
                return DataTypes.FLOAT();
            case INT:
                return DataTypes.INT();
            case LONG:
                return DataTypes.BIGINT();
            case STRING:
                return DataTypes.STRING();
            case RECORD:
                List<DataField> fields = new ArrayList<>();
                for (Schema.Field field : schema.getFields()) {
                    DataType fieldType = fromDebeziumAvroType(field.schema());
                    fields.add(DataTypes.FIELD(field.pos(), field.name(), fieldType, field.doc()));
                }
                return DataTypes.ROW(fields.toArray(new DataField[0]));
            case ARRAY:
                Schema elementSchema = schema.getElementType();
                DataType elementType = fromDebeziumAvroType(elementSchema);
                return DataTypes.ARRAY(elementType);
            case MAP:
                DataType valueType = fromDebeziumAvroType(schema.getValueType());
                return DataTypes.MAP(DataTypes.STRING(), valueType);
            case UNION:
                List<Schema> unionTypes = schema.getTypes();
                // Check if it's a nullable type union
                if (unionTypes.size() == 2
                        && unionTypes.contains(Schema.create(Schema.Type.NULL))) {
                    Schema actualSchema =
                            unionTypes.stream()
                                    .filter(s -> s.getType() != Schema.Type.NULL)
                                    .findFirst()
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Union type does not contain a non-null type"));
                    return fromDebeziumAvroType(actualSchema)
                            .copy(true); // Return nullable version of the non-null type
                }
                // Handle generic unions or throw an exception
                throw new UnsupportedOperationException("Generic unions are not supported");
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support avro type '%s' yet.", avroType));
        }
    }
}
