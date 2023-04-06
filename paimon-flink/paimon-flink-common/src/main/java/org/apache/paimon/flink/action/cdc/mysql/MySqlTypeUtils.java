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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts from MySQL type to {@link DataType}.
 *
 * <p>Mostly referenced from <a
 * href="https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mysql-cdc/src/main/java/com/ververica/cdc/connectors/mysql/schema/MySqlTypeUtils.java">ververica
 * / flink-cdc-connectors</a>.
 */
public class MySqlTypeUtils {

    // ------ MySQL Type ------
    // https://dev.mysql.com/doc/refman/8.0/en/data-types.html
    private static final String BIT = "BIT";
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
    private static final String UNKNOWN = "UNKNOWN";

    // This length is from JDBC.
    // It returns the number of characters when converting this timestamp to string.
    // The base length of a timestamp is 19, for example "2023-03-23 17:20:00".
    private static final int JDBC_TIMESTAMP_BASE_LENGTH = 19;

    public static DataType toDataType(String type, String params) {
        List<Integer> paramList = new ArrayList<>();
        if (params != null) {
            for (String s : params.split(",")) {
                paramList.add(Integer.parseInt(s.trim()));
            }
        }
        return toDataType(
                type,
                paramList.size() > 0 ? paramList.get(0) : null,
                paramList.size() > 1 ? paramList.get(1) : null);
    }

    public static DataType toDataType(
            String type, @Nullable Integer length, @Nullable Integer scale) {
        switch (type.toUpperCase()) {
            case BIT:
                return DataTypes.BOOLEAN();
            case TINYINT:
                // MySQL haven't boolean type, it uses tinyint(1) to represents boolean type
                // user should not use tinyint(1) to store number although jdbc url parameter
                // tinyInt1isBit=false can help change the return value, it's not a general way
                // btw: mybatis and mysql-connector-java map tinyint(1) to boolean by default
                return length != null && length == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
            case TINYINT_UNSIGNED:
            case TINYINT_UNSIGNED_ZEROFILL:
            case SMALLINT:
                return DataTypes.SMALLINT();
            case SMALLINT_UNSIGNED:
            case SMALLINT_UNSIGNED_ZEROFILL:
            case INT:
            case MEDIUMINT:
                return DataTypes.INT();
            case INT_UNSIGNED:
            case INT_UNSIGNED_ZEROFILL:
            case MEDIUMINT_UNSIGNED:
            case MEDIUMINT_UNSIGNED_ZEROFILL:
            case BIGINT:
                return DataTypes.BIGINT();
            case BIGINT_UNSIGNED:
            case BIGINT_UNSIGNED_ZEROFILL:
            case SERIAL:
                return DataTypes.DECIMAL(20, 0);
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
            case DATETIME:
            case TIMESTAMP:
                if (length == null) {
                    return DataTypes.TIMESTAMP();
                } else if (length >= JDBC_TIMESTAMP_BASE_LENGTH) {
                    if (length > JDBC_TIMESTAMP_BASE_LENGTH + 1) {
                        // Timestamp with a fraction of seconds.
                        // For example "2023-03-23 17:20:00.01".
                        // The decimal point will occupy 1 character.
                        return DataTypes.TIMESTAMP(length - JDBC_TIMESTAMP_BASE_LENGTH - 1);
                    } else {
                        return DataTypes.TIMESTAMP(0);
                    }
                } else if (length >= 0 && length <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIMESTAMP(length);
                } else {
                    return DataTypes.TIMESTAMP();
                }
            case CHAR:
                return DataTypes.CHAR(Preconditions.checkNotNull(length));
            case VARCHAR:
                return DataTypes.VARCHAR(Preconditions.checkNotNull(length));
            case TEXT:
                return DataTypes.STRING();
            case BINARY:
                return DataTypes.BINARY(Preconditions.checkNotNull(length));
            case VARBINARY:
                return DataTypes.VARBINARY(Preconditions.checkNotNull(length));
            case BLOB:
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support MySQL type '%s' yet.", type));
        }
    }
}
