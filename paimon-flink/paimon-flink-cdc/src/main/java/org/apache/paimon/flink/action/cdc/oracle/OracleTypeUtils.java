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

package org.apache.paimon.flink.action.cdc.oracle;

import org.apache.paimon.flink.action.cdc.JdbcToPaimonTypeVisitor;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import javax.annotation.Nullable;

import static org.apache.paimon.flink.action.cdc.TypeMapping.TypeMappingMode.TO_STRING;

/** Converts from Oracle type to {@link DataType}. */
public class OracleTypeUtils {
    private static final String NUMBER = "NUMBER";
    private static final String FLOAT = "FLOAT";
    private static final String BINARY_FLOAT = "BINARY_FLOAT";
    private static final String BINARY_DOUBLE = "BINARY_DOUBLE";
    private static final String DATE = "DATE";
    private static final String TIMESTAMP = "TIMESTAMP";
    private static final String TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
    private static final String TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";
    private static final String CHAR = "CHAR";
    private static final String NCHAR = "NCHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String VARCHAR2 = "VARCHAR2";
    private static final String NVARCHAR2 = "NVARCHAR2";
    private static final String CLOB = "CLOB";
    private static final String NCLOB = "NCLOB";
    private static final String BLOB = "BLOB";
    private static final String SYS_XMLTYPE = "SYS.XMLTYPE";
    private static final String XMLTYPE = "XMLTYPE";
    private static final String INTERVAL_DAY_TO_SECDOND = "INTERVAL DAY TO SECDOND";
    private static final String INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH";

    public static DataType toDataType(
            String typeName,
            @Nullable Integer precision,
            @Nullable Integer scale,
            TypeMapping typeMapping) {
        if (typeMapping.containsMode(TO_STRING)) {
            return DataTypes.STRING();
        }
        precision = precision == null ? 0 : precision;
        scale = scale == null ? 0 : scale;
        // refer:
        // https://ververica.github.io/flink-cdc-connectors/master/content/connectors/oracle-cdc.html#data-type-mapping
        switch (typeName.toUpperCase()) {
            case CHAR:
            case NCHAR:
            case NVARCHAR2:
            case VARCHAR:
            case VARCHAR2:
            case CLOB:
            case NCLOB:
            case XMLTYPE:
            case SYS_XMLTYPE:
            case TIMESTAMP_WITH_TIME_ZONE:
                return DataTypes.STRING();
            case FLOAT:
            case BINARY_FLOAT:
                return DataTypes.FLOAT();
            case BINARY_DOUBLE:
                return DataTypes.DOUBLE();
            case DATE:
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(precision);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision);
            case BLOB:
                return DataTypes.BYTES();
            case INTERVAL_DAY_TO_SECDOND:
            case INTERVAL_YEAR_TO_MONTH:
                return DataTypes.BIGINT();
            case NUMBER:
                if (precision == 1) {
                    return DataTypes.BOOLEAN();
                } else if (precision > 0 && scale > 0) {
                    return DataTypes.DECIMAL(precision, scale);
                } else {
                    int diff = precision - scale;
                    if (diff < 3) {
                        return DataTypes.TINYINT();
                    } else if (diff < 5) {
                        return DataTypes.SMALLINT();
                    } else if (diff < 10) {
                        return DataTypes.INT();
                    } else if (diff < 19) {
                        return DataTypes.BIGINT();
                    } else if (diff <= 38) {
                        return DataTypes.DECIMAL(diff, 0);
                    } else {
                        return DataTypes.STRING();
                    }
                }
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Oracle type '%s' yet", typeName));
        }
    }

    public static JdbcToPaimonTypeVisitor toPaimonTypeVisitor() {
        return OracleTypeUtils.OracleToPaimonTypeVisitor.INSTANCE;
    }

    private static class OracleToPaimonTypeVisitor implements JdbcToPaimonTypeVisitor {

        private static final OracleTypeUtils.OracleToPaimonTypeVisitor INSTANCE =
                new OracleTypeUtils.OracleToPaimonTypeVisitor();

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
