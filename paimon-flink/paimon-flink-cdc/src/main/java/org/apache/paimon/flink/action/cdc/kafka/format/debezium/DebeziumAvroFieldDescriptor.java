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

package org.apache.paimon.flink.action.cdc.kafka.format.debezium;

import org.apache.paimon.flink.action.cdc.SchemaUtils;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.kafka.format.FieldDescriptor;
import org.apache.paimon.flink.action.cdc.mysql.MySqlTypeUtils;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** An immutable representation of a AvroFieldDescriptor. */
public class DebeziumAvroFieldDescriptor extends FieldDescriptor<Schema> {

    protected static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
    protected static final String CONNECT_NAME_PROP = "connect.name";

    private static final String POINT_LOGICAL_NAME = "io.debezium.data.geometry.Point";
    private static final String GEOMETRY_LOGICAL_NAME = "io.debezium.data.geometry.Geometry";
    private static final String ENUM_SET_LOGICAL_NAME = "io.debezium.data.EnumSet";
    private static final String DATE_SCHEMA_NAME = "io.debezium.time.Date";
    private static final String TIMESTAMP_SCHEMA_NAME = "io.debezium.time.Timestamp";
    private static final String MICRO_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.MicroTimestamp";
    private static final String NANO_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.NanoTimestamp";
    private static final String TIME_SCHEMA_NAME = "io.debezium.time.Time";
    private static final String MICRO_TIME_SCHEMA_NAME = "io.debezium.time.MicroTime";
    private static final String NANO_TIME_SCHEMA_NAME = "io.debezium.time.NanoTime";
    private static final String ZONED_TIME_SCHEMA_NAME = "io.debezium.time.ZonedTime";
    private static final String ZONED_TIMESTAMP_SCHEMA_NAME = "io.debezium.time.ZonedTimestamp";
    private static final String DECIMAL_PRECISE_SCHEMA_NAME =
            "org.apache.kafka.connect.data.Decimal";

    private final Map<String, String> connectParameters;

    public DebeziumAvroFieldDescriptor(Schema schema, String name, boolean key) {
        super(schema, name, key);
        // Parse actual source column type from connect.parameters if enable debezium property
        // "column.propagate.source.type", otherwise will infer avro schema type mapping to paimon
        this.connectParameters = getConnectParameters();
        this.columnName =
                isAvroRecordType()
                        ? name
                        : SchemaUtils.getSourceColumnName(connectParameters).orElse(name);
        this.typeName =
                SchemaUtils.getSourceColumnType(connectParameters).orElse(schema.getType().name());
        this.length =
                SchemaUtils.getSourceColumnSize(connectParameters)
                        .map(Integer::valueOf)
                        .orElse(null);
        this.scale =
                SchemaUtils.getSourceColumnPrecision(connectParameters)
                        .map(Integer::valueOf)
                        .orElse(null);
        this.paimonType = toPaimonDataType();
    }

    public DataType toPaimonDataType() {
        // Mapping by mysql types
        if (SchemaUtils.getSourceColumnType(connectParameters).isPresent()) {
            if (isZonedTimeType() || isZonedTimestampType()) {
                return DataTypes.STRING();
            } else if (isDateType() || isTimeType()) {
                return DataTypes.INT();
            } else if (isMicroTimeType()
                    || isNanoTimeType()
                    || isTimestampType()
                    || isMicroTimestampType()
                    || isNanoTimestampType()) {
                return DataTypes.BIGINT();
            }
            return MySqlTypeUtils.toDataType(typeName, length, scale, TypeMapping.defaultMapping());
        }
        // Mapping by avro schema type
        return toPaimonDataType(schema);
    }

    private DataType toPaimonDataType(Schema schema) {
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
                if (isSetType()) {
                    DataTypes.ARRAY(DataTypes.STRING());
                }
                return DataTypes.STRING();
            case RECORD:
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support avro type '%s' yet.", avroType));
        }
    }

    private Map<String, String> getConnectParameters() {
        if (schema.getObjectProp(CONNECT_PARAMETERS_PROP) != null) {
            return (Map<String, String>) schema.getObjectProp(CONNECT_PARAMETERS_PROP);
        }
        return new HashMap<>();
    }

    public boolean isGeoType() {
        String logicName = schema.getFullName();
        return logicName.equals(POINT_LOGICAL_NAME) || logicName.equals(GEOMETRY_LOGICAL_NAME);
    }

    public boolean isSetType() {
        return schema.getFullName().equals(ENUM_SET_LOGICAL_NAME);
    }

    public boolean isDateType() {
        return Objects.equals(getConnectName(), DATE_SCHEMA_NAME);
    }

    public boolean isTimeType() {
        return Objects.equals(getConnectName(), TIME_SCHEMA_NAME);
    }

    public boolean isMicroTimeType() {
        return Objects.equals(getConnectName(), MICRO_TIME_SCHEMA_NAME);
    }

    public boolean isNanoTimeType() {
        return Objects.equals(getConnectName(), NANO_TIME_SCHEMA_NAME);
    }

    public boolean isTimestampType() {
        return Objects.equals(getConnectName(), TIMESTAMP_SCHEMA_NAME);
    }

    public boolean isMicroTimestampType() {
        return Objects.equals(getConnectName(), MICRO_TIMESTAMP_SCHEMA_NAME);
    }

    public boolean isNanoTimestampType() {
        return Objects.equals(getConnectName(), NANO_TIMESTAMP_SCHEMA_NAME);
    }

    public boolean isZonedTimeType() {
        return Objects.equals(getConnectName(), ZONED_TIME_SCHEMA_NAME);
    }

    public boolean isZonedTimestampType() {
        return Objects.equals(getConnectName(), ZONED_TIMESTAMP_SCHEMA_NAME);
    }

    public boolean isDecimalPreciseType() {
        return Objects.equals(getConnectName(), DECIMAL_PRECISE_SCHEMA_NAME);
    }

    private String getConnectName() {
        return (String) schema.getObjectProp(CONNECT_NAME_PROP);
    }

    private boolean isAvroRecordType() {
        return schema.getType() == Schema.Type.RECORD;
    }
}
