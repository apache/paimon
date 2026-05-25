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

package org.apache.paimon.cli;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import java.util.List;

/** Converts {@link InternalRow} field values to printable strings. */
public class RowPrinter {

    private RowPrinter() {}

    public static String getFieldValue(InternalRow row, int index, DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case BOOLEAN:
                return String.valueOf(row.getBoolean(index));
            case TINYINT:
                return String.valueOf(row.getByte(index));
            case SMALLINT:
                return String.valueOf(row.getShort(index));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return String.valueOf(row.getInt(index));
            case BIGINT:
                return String.valueOf(row.getLong(index));
            case FLOAT:
                return String.valueOf(row.getFloat(index));
            case DOUBLE:
                return String.valueOf(row.getDouble(index));
            case DECIMAL:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    int scale = DataTypeChecks.getScale(type);
                    return row.getDecimal(index, precision, scale).toBigDecimal().toPlainString();
                }
            case CHAR:
            case VARCHAR:
                return row.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return formatBytes(row.getBinary(index));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    return row.getTimestamp(index, precision).toString();
                }
            case ARRAY:
                return formatArray(row.getArray(index), ((ArrayType) type).getElementType());
            case MAP:
            case MULTISET:
                return formatMap(row.getMap(index), type);
            case ROW:
                return formatRow(
                        row.getRow(index, DataTypeChecks.getFieldCount(type)), (RowType) type);
            default:
                return "<unsupported:" + root + ">";
        }
    }

    public static boolean isNumericOrBoolean(DataTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return true;
            default:
                return false;
        }
    }

    private static String formatArray(InternalArray array, DataType elementType) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < array.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(formatArrayElement(array, i, elementType));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatMap(InternalMap map, DataType mapType) {
        DataType keyType;
        DataType valueType;
        if (mapType instanceof MapType) {
            keyType = ((MapType) mapType).getKeyType();
            valueType = ((MapType) mapType).getValueType();
        } else {
            keyType = ((MultisetType) mapType).getElementType();
            valueType = new org.apache.paimon.types.IntType();
        }
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(formatArrayElement(keys, i, keyType));
            sb.append(":");
            sb.append(formatArrayElement(values, i, valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatRow(InternalRow row, RowType rowType) {
        List<org.apache.paimon.types.DataField> fields = rowType.getFields();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(fields.get(i).name()).append(":");
            if (row.isNullAt(i)) {
                sb.append("null");
            } else {
                sb.append(getFieldValue(row, i, fields.get(i).type()));
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String formatArrayElement(InternalArray array, int index, DataType type) {
        if (array.isNullAt(index)) {
            return "null";
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return String.valueOf(array.getBoolean(index));
            case TINYINT:
                return String.valueOf(array.getByte(index));
            case SMALLINT:
                return String.valueOf(array.getShort(index));
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return String.valueOf(array.getInt(index));
            case BIGINT:
                return String.valueOf(array.getLong(index));
            case FLOAT:
                return String.valueOf(array.getFloat(index));
            case DOUBLE:
                return String.valueOf(array.getDouble(index));
            case CHAR:
            case VARCHAR:
                return "\"" + array.getString(index).toString() + "\"";
            case DECIMAL:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    int scale = DataTypeChecks.getScale(type);
                    return array.getDecimal(index, precision, scale).toBigDecimal().toPlainString();
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    int precision = DataTypeChecks.getPrecision(type);
                    return array.getTimestamp(index, precision).toString();
                }
            default:
                return String.valueOf(array.getString(index));
        }
    }

    private static String formatBytes(byte[] data) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < data.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(data[i] & 0xFF);
        }
        sb.append("]");
        return sb.toString();
    }
}
