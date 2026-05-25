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

package org.apache.paimon.cli.sql;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/** Maps Paimon types to Calcite types and converts InternalRow values to Java objects. */
public class PaimonTypeMapping {

    public static RelDataType toCalciteRowType(
            RelDataTypeFactory typeFactory, RowType paimonRowType) {
        List<DataField> fields = paimonRowType.getFields();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (DataField field : fields) {
            RelDataType fieldType = toCalciteType(typeFactory, field.type());
            if (field.type().isNullable()) {
                fieldType = typeFactory.createTypeWithNullability(fieldType, true);
            }
            builder.add(field.name(), fieldType);
        }
        return builder.build();
    }

    public static RelDataType toCalciteType(RelDataTypeFactory typeFactory, DataType paimonType) {
        switch (paimonType.getTypeRoot()) {
            case BOOLEAN:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case TINYINT:
                return typeFactory.createSqlType(SqlTypeName.TINYINT);
            case SMALLINT:
                return typeFactory.createSqlType(SqlTypeName.SMALLINT);
            case INTEGER:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case BIGINT:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case DECIMAL:
                {
                    int precision = DataTypeChecks.getPrecision(paimonType);
                    int scale = DataTypeChecks.getScale(paimonType);
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                }
            case CHAR:
                {
                    int length = DataTypeChecks.getLength(paimonType);
                    return typeFactory.createSqlType(SqlTypeName.CHAR, Math.max(length, 1));
                }
            case VARCHAR:
                {
                    int length = DataTypeChecks.getLength(paimonType);
                    if (length == VarCharType.MAX_LENGTH || length == Integer.MAX_VALUE) {
                        return typeFactory.createSqlType(SqlTypeName.VARCHAR, 65536);
                    }
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, length);
                }
            case DATE:
                return typeFactory.createSqlType(SqlTypeName.DATE);
            case TIME_WITHOUT_TIME_ZONE:
                return typeFactory.createSqlType(SqlTypeName.TIME);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    int precision = DataTypeChecks.getPrecision(paimonType);
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, precision);
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    int precision = DataTypeChecks.getPrecision(paimonType);
                    return typeFactory.createSqlType(
                            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision);
                }
            case BINARY:
            case VARBINARY:
                {
                    int length = DataTypeChecks.getLength(paimonType);
                    return typeFactory.createSqlType(SqlTypeName.VARBINARY, Math.max(length, 1));
                }
            default:
                return typeFactory.createSqlType(SqlTypeName.ANY);
        }
    }

    public static Object getValue(InternalRow row, int index, DataType type) {
        if (row.isNullAt(index)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(index);
            case TINYINT:
                return row.getByte(index);
            case SMALLINT:
                return row.getShort(index);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return row.getInt(index);
            case BIGINT:
                return row.getLong(index);
            case FLOAT:
                return row.getFloat(index);
            case DOUBLE:
                return row.getDouble(index);
            case DECIMAL:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    int scale = DataTypeChecks.getScale(type);
                    Decimal d = row.getDecimal(index, precision, scale);
                    return d.toBigDecimal();
                }
            case CHAR:
            case VARCHAR:
                {
                    BinaryString s = row.getString(index);
                    return s.toString();
                }
            case BINARY:
            case VARBINARY:
                return row.getBinary(index);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    Timestamp ts = row.getTimestamp(index, precision);
                    return ts.getMillisecond();
                }
            default:
                return row.getString(index).toString();
        }
    }

    public static DataTypeRoot getTypeRoot(DataType type) {
        return type.getTypeRoot();
    }
}
