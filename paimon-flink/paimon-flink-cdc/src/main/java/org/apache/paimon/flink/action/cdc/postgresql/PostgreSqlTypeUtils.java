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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import com.esri.core.geometry.ogc.OGCGeometry;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Converts from PostgreSQL type to {@link DataType}. */
public class PostgreSqlTypeUtils {

    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String BIT = "BIT";
    private static final String VARBIT = "VARBIT";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLSERIAL = "SMALLSERIAL";
    private static final String SERIAL_2 = "SERIAL2";
    private static final String INT_2 = "INT2";
    private static final String INT_4 = "INT4";
    private static final String INTEGER = "INTEGER";
    private static final String INT = "INT";
    private static final String MONEY = "MONEY";
    private static final String SERIAL = "SERIAL";
    private static final String SERIAL_4 = "SERIAL4";
    private static final String BIGINT = "BIGINT";
    private static final String BIGSERIAL = "BIGSERIAL";
    private static final String SERIAL_8 = "SERIAL8";
    private static final String INT_8 = "INT8";
    private static final String REAL = "REAL";
    private static final String FLOAT_4 = "FLOAT4";
    private static final String FLOAT_8 = "FLOAT8";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String BPCHAR = "BPCHAR";
    private static final String INET = "INET";
    private static final String TEXT = "TEXT";
    private static final String JSON = "JSON";
    private static final String XML = "XML";
    private static final String UUID = "UUID";
    private static final String POLYGON = "POLYGON";
    private static final String POINT = "POINT";
    private static final String BOX = "BOX";
    private static final String CIRCLE = "CIRCLE";
    private static final String CIDR = "CIDR";
    private static final String LINE = "LINE";
    private static final String LSEG = "LSEG";
    private static final String MACADDR = "MACADDR";
    private static final String MACADDR8 = "MACADDR8";
    private static final String PATH = "PATH";
    private static final String DATE = "DATE";
    private static final String TIMESTAMP_WITHOUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
    private static final String TIMESTAMPTZ = "TIMESTAMPTZ";
    private static final String TIME_WITHOUT_TIME_ZONE = "TIME WITHOUT TIME ZONE";
    private static final String TIME = "TIME";
    private static final String TIME_WITH_TIME_ZONE = "TIME WITH TIME ZONE";
    private static final String TIMETZ = "TIMETZ";
    private static final String NUMERIC = "NUMERIC";
    private static final String DECIMAL = "DECIMAL";
    private static final String BYTEA = "BYTEA";

    private static final int JDBC_TIMESTAMP_BASE_LENGTH = 19;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static DataType toDataType(
            String type, @Nullable Integer length, @Nullable Integer scale) {
        switch (type.toUpperCase()) {
            case BOOLEAN:
            case BOOL:
            case SMALLINT:
            case INT_2:
            case SMALLSERIAL:
            case SERIAL_2:
                return DataTypes.SMALLINT();
            case INT_4:
            case INT:
            case INTEGER:
            case SERIAL:
            case SERIAL_4:
                return DataTypes.INT();
            case BIGINT:
            case INT_8:
            case BIGSERIAL:
            case SERIAL_8:
                return DataTypes.BIGINT();
            case REAL:
            case FLOAT_4:
                return DataTypes.FLOAT();
            case FLOAT_8:
            case DOUBLE_PRECISION:
                return DataTypes.DOUBLE();
            case CHAR:
                return DataTypes.CHAR(Preconditions.checkNotNull(length));
            case VARCHAR:
            case BPCHAR:
                return DataTypes.VARCHAR(Preconditions.checkNotNull(length));
            case TEXT:
            case INET:
            case JSON:
            case XML:
            case UUID:
            case POLYGON:
            case POINT:
            case BOX:
            case CIRCLE:
            case CIDR:
            case LINE:
            case LSEG:
            case MACADDR:
            case MACADDR8:
            case PATH:
                return DataTypes.STRING();
            case DATE:
                return DataTypes.DATE();
            case TIMESTAMP:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (length >= JDBC_TIMESTAMP_BASE_LENGTH) {
                    int precision =
                            length > JDBC_TIMESTAMP_BASE_LENGTH + 1
                                    ? length - JDBC_TIMESTAMP_BASE_LENGTH - 1
                                    : 0;
                    return DataTypes.TIMESTAMP(precision);
                } else if (length >= 0 && length <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIMESTAMP(length);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length " + length + " for PostgreSQL TIMESTAMP type");
                }
            case TIMESTAMPTZ:
            case TIMESTAMP_WITH_TIME_ZONE:
                if (length >= JDBC_TIMESTAMP_BASE_LENGTH) {
                    int precision =
                            length > JDBC_TIMESTAMP_BASE_LENGTH + 1
                                    ? Math.min(
                                            length - JDBC_TIMESTAMP_BASE_LENGTH - 1,
                                            TimestampType.MAX_PRECISION)
                                    : 0;
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision);
                } else if (length >= 0 && length <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(length);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length " + length + " for PostgreSQL TIMESTAMPZ type");
                }
            case TIME:
            case TIME_WITHOUT_TIME_ZONE:
                return DataTypes.TIME();
            case TIMETZ:
            case TIME_WITH_TIME_ZONE:
                throw new UnsupportedOperationException(
                        "PostgreSQL's TIMETZ and TIME_WITH_TIME_ZONE cannot be converted to Paimon data types. Paimon does not support TIME types with a time zone.");
            case NUMERIC:
            case DECIMAL:
            case MONEY:
                return length != null && length <= 38
                        ? DataTypes.DECIMAL(length, scale != null && scale >= 0 ? scale : 0)
                        : DataTypes.STRING();
            case BIT:
                if (length == 1) {
                    return DataTypes.BOOLEAN();
                } else if (length > 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length "
                                    + length
                                    + " for PostgreSQL DATETIME and BIT types");
                }
            case BYTEA:
            case VARBIT:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support PostgreSQL type '%s' yet.", type));
        }
    }

    public static String convertWkbArray(byte[] wkb) throws JsonProcessingException {
        String geoJson = OGCGeometry.fromBinary(ByteBuffer.wrap(wkb)).asGeoJson();
        JsonNode originGeoNode = objectMapper.readTree(geoJson);

        Optional<Integer> srid =
                Optional.ofNullable(
                        originGeoNode.has("srid") ? originGeoNode.get("srid").intValue() : null);
        Map<String, Object> geometryInfo = new HashMap<>();
        String geometryType = originGeoNode.get("type").asText();
        geometryInfo.put("type", geometryType);
        if (geometryType.equalsIgnoreCase("GeometryCollection")) {
            geometryInfo.put("geometries", originGeoNode.get("geometries"));
        } else {
            geometryInfo.put("coordinates", originGeoNode.get("coordinates"));
        }
        geometryInfo.put("srid", srid.orElse(0));
        ObjectWriter objectWriter = objectMapper.writer();
        return objectWriter.writeValueAsString(geometryInfo);
    }
}
