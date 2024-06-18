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

import org.apache.paimon.flink.action.cdc.JdbcToPaimonTypeVisitor;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectWriter;

import com.esri.core.geometry.ogc.OGCGeometry;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.BIGINT_UNSIGNED_TO_BIGINT;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.CHAR_TO_STRING;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.LONGTEXT_TO_BYTES;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;

/* This file is based on source code from MySqlTypeUtils in the flink-cdc-connectors Project
 * (https://ververica.github.io/flink-cdc-connectors/), licensed by the Apache Software Foundation (ASF)
 * under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Converts from MySQL type to {@link DataType}. */
public class MySqlTypeUtils {

    // ------ MySQL Type ------
    // https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    private static final String BIT = "BIT";
    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String TINYINT = "TINYINT";
    private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
    private static final String TINYINT_UNSIGNED_ZEROFILL = "TINYINT UNSIGNED ZEROFILL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
    private static final String SMALLINT_UNSIGNED_ZEROFILL = "SMALLINT UNSIGNED ZEROFILL";
    private static final String MEDIUMINT = "MEDIUMINT";
    private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
    private static final String MEDIUMINT_UNSIGNED_ZEROFILL = "MEDIUMINT UNSIGNED ZEROFILL";
    private static final String INT = "INT";
    private static final String INT_UNSIGNED = "INT UNSIGNED";
    private static final String INT_UNSIGNED_ZEROFILL = "INT UNSIGNED ZEROFILL";
    private static final String INTEGER = "INTEGER";
    private static final String INTEGER_UNSIGNED = "INTEGER UNSIGNED";
    private static final String INTEGER_UNSIGNED_ZEROFILL = "INTEGER UNSIGNED ZEROFILL";
    private static final String BIGINT = "BIGINT";
    private static final String SERIAL = "SERIAL";
    private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
    private static final String BIGINT_UNSIGNED_ZEROFILL = "BIGINT UNSIGNED ZEROFILL";
    private static final String REAL = "REAL";
    private static final String REAL_UNSIGNED = "REAL UNSIGNED";
    private static final String REAL_UNSIGNED_ZEROFILL = "REAL UNSIGNED ZEROFILL";
    private static final String FLOAT = "FLOAT";
    private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
    private static final String FLOAT_UNSIGNED_ZEROFILL = "FLOAT UNSIGNED ZEROFILL";
    private static final String DOUBLE = "DOUBLE";
    private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
    private static final String DOUBLE_UNSIGNED_ZEROFILL = "DOUBLE UNSIGNED ZEROFILL";
    private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
    private static final String DOUBLE_PRECISION_UNSIGNED = "DOUBLE PRECISION UNSIGNED";
    private static final String DOUBLE_PRECISION_UNSIGNED_ZEROFILL =
            "DOUBLE PRECISION UNSIGNED ZEROFILL";
    private static final String NUMERIC = "NUMERIC";
    private static final String NUMERIC_UNSIGNED = "NUMERIC UNSIGNED";
    private static final String NUMERIC_UNSIGNED_ZEROFILL = "NUMERIC UNSIGNED ZEROFILL";
    private static final String FIXED = "FIXED";
    private static final String FIXED_UNSIGNED = "FIXED UNSIGNED";
    private static final String FIXED_UNSIGNED_ZEROFILL = "FIXED UNSIGNED ZEROFILL";
    private static final String DECIMAL = "DECIMAL";
    private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
    private static final String DECIMAL_UNSIGNED_ZEROFILL = "DECIMAL UNSIGNED ZEROFILL";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TINYTEXT = "TINYTEXT";
    private static final String MEDIUMTEXT = "MEDIUMTEXT";
    private static final String TEXT = "TEXT";
    private static final String LONGTEXT = "LONGTEXT";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String YEAR = "YEAR";
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String TINYBLOB = "TINYBLOB";
    private static final String MEDIUMBLOB = "MEDIUMBLOB";
    private static final String BLOB = "BLOB";
    private static final String LONGBLOB = "LONGBLOB";
    private static final String JSON = "JSON";
    private static final String SET = "SET";
    private static final String ENUM = "ENUM";
    private static final String GEOMETRY = "GEOMETRY";
    private static final String POINT = "POINT";
    private static final String LINESTRING = "LINESTRING";
    private static final String POLYGON = "POLYGON";
    private static final String MULTIPOINT = "MULTIPOINT";
    private static final String MULTILINESTRING = "MULTILINESTRING";
    private static final String MULTIPOLYGON = "MULTIPOLYGON";
    private static final String GEOMETRYCOLLECTION = "GEOMETRYCOLLECTION";
    private static final String UNKNOWN = "UNKNOWN";

    // This length is from JDBC.
    // It returns the number of characters when converting this timestamp to string.
    // The base length of a timestamp is 19, for example "2023-03-23 17:20:00".
    private static final int JDBC_TIMESTAMP_BASE_LENGTH = 19;

    private static final String LEFT_BRACKETS = "(";
    private static final String RIGHT_BRACKETS = ")";
    private static final String COMMA = ",";

    private static final List<String> HAVE_SCALE_LIST =
            Arrays.asList(DECIMAL, NUMERIC, DOUBLE, REAL, FIXED);
    private static final List<String> MAP_TO_DECIMAL_TYPES =
            Arrays.asList(
                    NUMERIC,
                    NUMERIC_UNSIGNED,
                    NUMERIC_UNSIGNED_ZEROFILL,
                    FIXED,
                    FIXED_UNSIGNED,
                    FIXED_UNSIGNED_ZEROFILL,
                    DECIMAL,
                    DECIMAL_UNSIGNED,
                    DECIMAL_UNSIGNED_ZEROFILL);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static DataType toDataType(String mysqlFullType, TypeMapping typeMapping) {
        return toDataType(
                MySqlTypeUtils.getShortType(mysqlFullType),
                MySqlTypeUtils.getPrecision(mysqlFullType),
                MySqlTypeUtils.getScale(mysqlFullType),
                typeMapping);
    }

    public static DataType toDataType(
            String type,
            @Nullable Integer length,
            @Nullable Integer scale,
            TypeMapping typeMapping) {
        if (typeMapping.containsMode(TO_STRING)) {
            return DataTypes.STRING();
        }

        switch (type.toUpperCase()) {
            case BIT:
                if (length == null || length == 1) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.BINARY((length + 7) / 8);
                }
            case BOOLEAN:
            case BOOL:
                return DataTypes.BOOLEAN();
            case TINYINT:
                // MySQL haven't boolean type, it uses tinyint(1) to represents boolean type.
                // User should not use tinyint(1) to store number although jdbc url parameter
                // tinyInt1isBit=false can help change the return value, it's not a general way.
                // Mybatis and mysql-connector-java map tinyint(1) to boolean by default, we behave
                // the same way by default. To store number (-128~127), user can set the type
                // mapping option 'tinyint1-not-bool' then tinyint(1) will be mapped to tinyint.
                return length != null && length == 1 && !typeMapping.containsMode(TINYINT1_NOT_BOOL)
                        ? DataTypes.BOOLEAN()
                        : DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case INTEGER:
            case MEDIUMINT:
            case YEAR:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case INTEGER_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case INTEGER_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return typeMapping.containsMode(BIGINT_UNSIGNED_TO_BIGINT)
                        ? DataTypes.BIGINT()
                        : DataTypes.DECIMAL(20, 0);
            case FLOAT:
            case FLOAT_UNSIGNED:
            case FLOAT_UNSIGNED_ZEROFILL:
                return DataTypes.FLOAT();
            case REAL:
            case REAL_UNSIGNED:
            case REAL_UNSIGNED_ZEROFILL:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DOUBLE_UNSIGNED_ZEROFILL:
            case DOUBLE_PRECISION:
            case DOUBLE_PRECISION_UNSIGNED:
            case DOUBLE_PRECISION_UNSIGNED_ZEROFILL:
                return DataTypes.DOUBLE();
            case NUMERIC:
            case NUMERIC_UNSIGNED:
            case NUMERIC_UNSIGNED_ZEROFILL:
            case FIXED:
            case FIXED_UNSIGNED:
            case FIXED_UNSIGNED_ZEROFILL:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case DECIMAL_UNSIGNED_ZEROFILL:
                return length != null && length <= 38
                        ? DataTypes.DECIMAL(length, scale != null && scale >= 0 ? scale : 0)
                        : DataTypes.STRING();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case DATETIME:
            case TIMESTAMP:
                if (length == null || length <= 0) {
                    // default precision is 0
                    // see https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html
                    return DataTypes.TIMESTAMP(0);
                } else if (length >= JDBC_TIMESTAMP_BASE_LENGTH) {
                    if (length > JDBC_TIMESTAMP_BASE_LENGTH + 1) {
                        // Timestamp with a fraction of seconds.
                        // For example "2023-03-23 17:20:00.01".
                        // The decimal point will occupy 1 character.
                        return DataTypes.TIMESTAMP(length - JDBC_TIMESTAMP_BASE_LENGTH - 1);
                    } else {
                        return DataTypes.TIMESTAMP(0);
                    }
                } else if (length <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIMESTAMP(length);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length "
                                    + length
                                    + " for MySQL DATETIME and TIMESTAMP types");
                }
                // because tidb ddl event does not contain field precision
            case CHAR:
                return length == null || length == 0 || typeMapping.containsMode(CHAR_TO_STRING)
                        ? DataTypes.STRING()
                        : DataTypes.CHAR(length);
            case VARCHAR:
                return length == null || length == 0 || typeMapping.containsMode(CHAR_TO_STRING)
                        ? DataTypes.STRING()
                        : DataTypes.VARCHAR(length);
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case JSON:
            case ENUM:
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
            case GEOMETRYCOLLECTION:
                return DataTypes.STRING();
                // MySQL BINARY and VARBINARY are stored as bytes in JSON. We convert them to
                // DataTypes.VARBINARY to retain the length information
            case BINARY:
            case VARBINARY:
                return length == null || length == 0
                        ? DataTypes.VARBINARY(VarBinaryType.DEFAULT_LENGTH)
                        : DataTypes.VARBINARY(length);
            case LONGTEXT:
                return typeMapping.containsMode(LONGTEXT_TO_BYTES)
                        ? DataTypes.BYTES()
                        : DataTypes.STRING();
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                return DataTypes.BYTES();
            case SET:
                return DataTypes.ARRAY(DataTypes.STRING());
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", type));
        }
    }

    public static boolean isGeoType(String type) {
        switch (type.toUpperCase()) {
            case GEOMETRY:
            case POINT:
            case LINESTRING:
            case POLYGON:
            case MULTIPOINT:
            case MULTILINESTRING:
            case MULTIPOLYGON:
            case GEOMETRYCOLLECTION:
                return true;
            default:
                return false;
        }
    }

    public static String convertWkbArray(byte[] wkb) throws JsonProcessingException {
        return convertWkbArray(ByteBuffer.wrap(wkb));
    }

    public static String convertWkbArray(ByteBuffer wkbByteBuffer) throws JsonProcessingException {
        String geoJson = OGCGeometry.fromBinary(wkbByteBuffer).asGeoJson();
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

    public static boolean isScaleType(String typeName) {
        return HAVE_SCALE_LIST.stream()
                .anyMatch(type -> getShortType(typeName).toUpperCase().startsWith(type));
    }

    public static boolean isEnumType(String typeName) {
        return typeName.toUpperCase().startsWith(ENUM);
    }

    public static boolean isSetType(String typeName) {
        return typeName.toUpperCase().startsWith(SET);
    }

    private static boolean isDecimalType(String typeName) {
        return MAP_TO_DECIMAL_TYPES.stream()
                .anyMatch(type -> getShortType(typeName).toUpperCase().startsWith(type));
    }

    /* Get type after the brackets are removed.*/
    public static String getShortType(String typeName) {

        if (typeName.contains(LEFT_BRACKETS) && typeName.contains(RIGHT_BRACKETS)) {
            return typeName.substring(0, typeName.indexOf(LEFT_BRACKETS)).trim()
                    + typeName.substring(typeName.indexOf(RIGHT_BRACKETS) + 1);
        } else {
            return typeName;
        }
    }

    public static int getPrecision(String typeName) {
        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            return Integer.parseInt(
                    typeName.substring(typeName.indexOf(LEFT_BRACKETS) + 1, typeName.indexOf(COMMA))
                            .trim());
        } else if ((typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && !isScaleType(typeName)
                && !isEnumType(typeName)
                && !isSetType(typeName))) {
            return Integer.parseInt(
                    typeName.substring(
                                    typeName.indexOf(LEFT_BRACKETS) + 1,
                                    typeName.indexOf(RIGHT_BRACKETS))
                            .trim());
        } else {
            // when missing precision of the decimal, we
            // use the max precision to avoid parse error
            return isDecimalType(typeName) ? 38 : 0;
        }
    }

    public static int getScale(String typeName) {
        if (typeName.contains(LEFT_BRACKETS)
                && typeName.contains(RIGHT_BRACKETS)
                && isScaleType(typeName)) {
            return Integer.parseInt(
                    typeName.substring(
                                    typeName.indexOf(COMMA) + 1, typeName.indexOf(RIGHT_BRACKETS))
                            .trim());
        } else {
            // When missing scale of the decimal, we
            // use the max scale to avoid parse error
            return isDecimalType(typeName) ? 18 : 0;
        }
    }

    public static JdbcToPaimonTypeVisitor toPaimonTypeVisitor() {
        return MySqlToPaimonTypeVisitor.INSTANCE;
    }

    private static class MySqlToPaimonTypeVisitor implements JdbcToPaimonTypeVisitor {

        private static final MySqlToPaimonTypeVisitor INSTANCE = new MySqlToPaimonTypeVisitor();

        @Override
        public DataType visit(
                String type,
                @Nullable Integer length,
                @Nullable Integer scale,
                TypeMapping typeMapping) {
            return toDataType(type, length, scale, typeMapping);
        }
    }
}
