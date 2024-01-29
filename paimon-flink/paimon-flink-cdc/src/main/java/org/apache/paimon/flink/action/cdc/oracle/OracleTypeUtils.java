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
    private static final String TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";
    private static final String CHAR = "CHAR";
    private static final String NCHAR = "NCHAR";
    private static final String VARCHAR = "VARCHAR";
    private static final String VARCHAR2 = "VARCHAR2";
    private static final String NVARCHAR2 = "NVARCHAR2";
    private static final String XMLTYPE = "XMLTYPE";

    public static DataType toDataType(
            String typeName,
            @Nullable Integer precision,
            @Nullable Integer scale,
            TypeMapping typeMapping) {
        if (typeMapping.containsMode(TO_STRING)) {
            return DataTypes.STRING();
        }
        typeName = getRealTypeName(typeName);
        precision = precision == null ? 0 : precision;
        scale = scale == null ? 0 : scale;
        switch (typeName.toUpperCase()) {
            case CHAR:
            case NCHAR:
            case NVARCHAR2:
            case VARCHAR:
            case VARCHAR2:
            case XMLTYPE:
            case FLOAT:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return DataTypes.STRING();
            case BINARY_FLOAT:
                return DataTypes.FLOAT();
            case BINARY_DOUBLE:
                return DataTypes.DOUBLE();
            case DATE:
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(6);
            case NUMBER:
                if (precision > 0 && scale > 0) {
                    return DataTypes.STRING();
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
                    } else {
                        return DataTypes.STRING();
                    }
                }
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support Oracle type '%s' yet", typeName));
        }
    }

    protected static String getRealTypeName(String input) {
        int leftParenIndex = input.indexOf('(');
        int rightParenIndex = input.indexOf(')');

        if (leftParenIndex != -1 && rightParenIndex != -1) {
            String leftPart = input.substring(0, leftParenIndex);
            String rightPart = input.substring(rightParenIndex + 1);
            return leftPart + rightPart;
        } else {
            return input;
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
