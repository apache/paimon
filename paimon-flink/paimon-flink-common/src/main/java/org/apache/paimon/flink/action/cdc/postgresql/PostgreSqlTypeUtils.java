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
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

/**
 * Converts from MySQL type to {@link DataType}.
 */
public class PostgreSqlTypeUtils {

    private static final String BOOLEAN = "BOOLEAN";
    private static final String BOOL = "BOOL";
    private static final String SMALLINT = "SMALLINT";
    private static final String SMALLSERIAL = "SMALLSERIAL";
    private static final String SERIAL_2 = "SERIAL2";
    private static final String INT_2 = "INT2";
    private static final String INT_4 = "INT4";
    private static final String INTEGER = "INTEGER";
    private static final String INT = "INT";
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
    private static final String INET = "INET";
    private static final String TEXT = "TEXT";

    public static DataType toDataType(
            String type, @Nullable Integer length, @Nullable Integer scale) {
        switch (type.toUpperCase()) {
            case BOOLEAN:
            case BOOL:
                return DataTypes.BOOLEAN();
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
                return DataTypes.VARCHAR(Preconditions.checkNotNull(length));
            case TEXT:
            case INET:
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support PostgreSQL type '%s' yet.", type));
        }
    }
}
