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

import org.apache.paimon.flink.action.cdc.JdbcToPaimonTypeVisitor;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.TimestampType;

import io.debezium.relational.Column;

import javax.annotation.Nullable;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.CHAR_TO_STRING;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL;
import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;

/** Converts from SqlServer type to {@link DataType}. */
public class SqlServerTypeUtils {

    private static final String BIT = "BIT";
    private static final String TINYINT = "TINYINT";
    private static final String SMALLINT = "SMALLINT";
    private static final String INT = "INT";
    private static final String BIGINT = "BIGINT";
    private static final String REAL = "REAL";
    private static final String FLOAT = "FLOAT";
    private static final String CHAR = "CHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String TEXT = "TEXT";
    private static final String NCHAR = "NCHAR";
    private static final String NVARCHAR = "NVARCHAR";
    private static final String NTEXT = "NTEXT";
    private static final String XML = "XML";
    private static final String DATETIMEOFFSET = "DATETIMEOFFSET";
    private static final String DATE = "DATE";
    private static final String TIME = "TIME";
    private static final String DATETIME = "DATETIME";
    private static final String SMALLDATETIME = "SMALLDATETIME";
    private static final String DATETIME2 = "DATETIME2";
    private static final String NUMERIC = "NUMERIC";
    private static final String DECIMAL = "DECIMAL";
    private static final String SMALLMONEY = "SMALLMONEY";
    private static final String MONEY = "MONEY";

    // binary
    private static final String BINARY = "BINARY";
    private static final String VARBINARY = "VARBINARY";
    private static final String IMAGE = "IMAGE";

    // Geospatial Types
    private static final String GEOGRAPHY = "GEOGRAPHY";
    private static final String GEOMETRY = "GEOMETRY";

    /**
     * Returns a corresponding Paimon data type from a debezium {@link Column} with nullable always
     * be true.
     */
    public static DataType toPaimonDataType(
            String type,
            @Nullable Integer length,
            @Nullable Integer scale,
            TypeMapping typeMapping) {
        if (typeMapping.containsMode(TO_STRING)) {
            return DataTypes.STRING();
        }
        switch (type.toUpperCase()) {
            case BIT:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return length != null && length == 1 && !typeMapping.containsMode(TINYINT1_NOT_BOOL)
                        ? DataTypes.BOOLEAN()
                        : DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case REAL:
                return DataTypes.DOUBLE();
            case FLOAT:
                return DataTypes.FLOAT();
            case CHAR:
                return length == null || length == 0 || typeMapping.containsMode(CHAR_TO_STRING)
                        ? DataTypes.STRING()
                        : DataTypes.CHAR(length);
            case NCHAR:
            case NVARCHAR:
            case VARCHAR:
                return length == null || length == 0 || typeMapping.containsMode(CHAR_TO_STRING)
                        ? DataTypes.STRING()
                        : DataTypes.VARCHAR(length);
            case TEXT:
            case NTEXT:
            case XML:
                return DataTypes.STRING();
            case DATETIMEOFFSET:
                return DataTypes.STRING();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                if (scale == null || scale <= 0) {
                    return DataTypes.TIME();
                } else if (scale <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIME(scale);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length " + length + " for SqlServer TIME types");
                }
            case DATETIME:
            case SMALLDATETIME:
            case DATETIME2:
                if (scale == null || scale <= 0) {
                    return DataTypes.TIMESTAMP(0);
                } else if (scale <= TimestampType.MAX_PRECISION) {
                    return DataTypes.TIMESTAMP(scale);
                } else {
                    throw new UnsupportedOperationException(
                            "Unsupported length "
                                    + length
                                    + " for SqlServer DATETIME、SMALLDATETIME and DATETIME2 types");
                }
            case NUMERIC:
            case DECIMAL:
                return length == null || length <= 38
                        ? DataTypes.DECIMAL(length, scale != null && scale >= 0 ? scale : 0)
                        : DataTypes.STRING();
            case SMALLMONEY:
                return DataTypes.DECIMAL(10, 4);
            case MONEY:
                return DataTypes.DECIMAL(19, 4);
            default:
                throw new UnsupportedOperationException(
                        String.format("Don't support SQLServer type '%s' yet.", type));
        }
    }

    public static JdbcToPaimonTypeVisitor toPaimonTypeVisitor() {
        return SqlServerTypeUtils.SqlServerToPaimonTypeVisitor.INSTANCE;
    }

    private static class SqlServerToPaimonTypeVisitor implements JdbcToPaimonTypeVisitor {

        private static final SqlServerTypeUtils.SqlServerToPaimonTypeVisitor INSTANCE =
                new SqlServerTypeUtils.SqlServerToPaimonTypeVisitor();

        @Override
        public DataType visit(
                String type,
                @Nullable Integer length,
                @Nullable Integer scale,
                TypeMapping typeMapping) {
            return toPaimonDataType(type, length, scale, typeMapping);
        }
    }
}
