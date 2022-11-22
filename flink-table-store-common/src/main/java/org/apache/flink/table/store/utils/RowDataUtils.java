/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for {@link RowData} structures. */
public class RowDataUtils {

    public static RowData copyRowData(RowData row, RowType rowType) {
        if (row instanceof BinaryRowData) {
            return ((BinaryRowData) row).copy();
        } else if (row instanceof NestedRowData) {
            return ((NestedRowData) row).copy();
        } else {
            GenericRowData ret = new GenericRowData(row.getArity());
            ret.setRowKind(row.getRowKind());

            for (int i = 0; i < row.getArity(); ++i) {
                LogicalType fieldType = rowType.getTypeAt(i);
                ret.setField(i, copy(get(row, i, fieldType), fieldType));
            }

            return ret;
        }
    }

    public static ArrayData copyArray(ArrayData from, LogicalType eleType) {
        if (from instanceof BinaryArrayData) {
            return ((BinaryArrayData) from).copy();
        }

        if (!eleType.isNullable()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArrayData(from.toBooleanArray());
                case TINYINT:
                    return new GenericArrayData(from.toByteArray());
                case SMALLINT:
                    return new GenericArrayData(from.toShortArray());
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return new GenericArrayData(from.toIntArray());
                case BIGINT:
                    return new GenericArrayData(from.toLongArray());
                case FLOAT:
                    return new GenericArrayData(from.toFloatArray());
                case DOUBLE:
                    return new GenericArrayData(from.toDoubleArray());
            }
        }

        Object[] newArray = new Object[from.size()];

        for (int i = 0; i < newArray.length; ++i) {
            if (!from.isNullAt(i)) {
                newArray[i] = copy(get(from, i, eleType), eleType);
            } else {
                newArray[i] = null;
            }
        }

        return new GenericArrayData(newArray);
    }

    private static MapData copyMap(MapData map, LogicalType keyType, LogicalType valueType) {
        if (map instanceof BinaryMapData) {
            return ((BinaryMapData) map).copy();
        }

        Map<Object, Object> javaMap = new HashMap<>();
        ArrayData keys = map.keyArray();
        ArrayData values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            javaMap.put(
                    copy(get(keys, i, keyType), keyType),
                    copy(get(values, i, valueType), valueType));
        }
        return new GenericMapData(javaMap);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Object copy(Object o, LogicalType type) {
        if (o instanceof StringData) {
            BinaryStringData string = (BinaryStringData) o;
            return string.getBinarySection() == null
                    ? StringData.fromString(string.toString())
                    : ((BinaryStringData) o).copy();
        } else if (o instanceof RowData) {
            return copyRowData((RowData) o, (RowType) type);
        } else if (o instanceof ArrayData) {
            return copyArray((ArrayData) o, ((ArrayType) type).getElementType());
        } else if (o instanceof MapData) {
            if (type instanceof MapType) {
                return copyMap(
                        (MapData) o,
                        ((MapType) type).getKeyType(),
                        ((MapType) type).getValueType());
            } else {
                return copyMap((MapData) o, ((MultisetType) type).getElementType(), new IntType());
            }
        } else if (o instanceof RawValueData) {
            BinaryRawValueData raw = (BinaryRawValueData) o;
            if (raw.getBinarySection() != null) {
                return BinaryRawValueData.fromBytes(raw.toBytes(null));
            }
        } else if (o instanceof DecimalData) {
            return ((DecimalData) o).copy();
        }
        return o;
    }

    public static Object get(RowData row, int pos, LogicalType fieldType) {
        if (row.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(pos);
            case TINYINT:
                return row.getByte(pos);
            case SMALLINT:
                return row.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return row.getInt(pos);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return row.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return row.getTimestamp(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return row.getTimestamp(pos, lzTs.getPrecision());
            case FLOAT:
                return row.getFloat(pos);
            case DOUBLE:
                return row.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return row.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return row.getArray(pos);
            case MAP:
            case MULTISET:
                return row.getMap(pos);
            case ROW:
                return row.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
            case VARBINARY:
                return row.getBinary(pos);
            case RAW:
                return row.getRawValue(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    public static Object get(ArrayData array, int pos, LogicalType fieldType) {
        if (array.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return array.getBoolean(pos);
            case TINYINT:
                return array.getByte(pos);
            case SMALLINT:
                return array.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return array.getInt(pos);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return array.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return array.getTimestamp(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return array.getTimestamp(pos, lzTs.getPrecision());
            case FLOAT:
                return array.getFloat(pos);
            case DOUBLE:
                return array.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return array.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return array.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return array.getArray(pos);
            case MAP:
            case MULTISET:
                return array.getMap(pos);
            case ROW:
                return array.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
            case VARBINARY:
                return array.getBinary(pos);
            case RAW:
                return array.getRawValue(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    public static ArrayData toStringArrayData(List<String> list) {
        return new GenericArrayData(list.stream().map(StringData::fromString).toArray());
    }

    public static List<String> fromStringArrayData(ArrayData arrayData) {
        List<String> list = new ArrayList<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            list.add(arrayData.isNullAt(i) ? null : arrayData.getString(i).toString());
        }
        return list;
    }

    public static long castToIntegral(DecimalData dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static RowData.FieldGetter[] createFieldGetters(List<LogicalType> fieldTypes) {
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldGetters[i] = createNullCheckingFieldGetter(fieldTypes.get(i), i);
        }
        return fieldGetters;
    }

    public static RowData.FieldGetter createNullCheckingFieldGetter(
            LogicalType logicalType, int index) {
        RowData.FieldGetter getter = RowData.createFieldGetter(logicalType, index);
        if (logicalType.isNullable()) {
            return getter;
        } else {
            return row -> {
                if (row.isNullAt(index)) {
                    return null;
                }
                return getter.getFieldOrNull(row);
            };
        }
    }

    public static int compare(Object x, Object y, LogicalTypeRoot type) {
        int ret;
        switch (type) {
            case DECIMAL:
                DecimalData xDD = (DecimalData) x;
                DecimalData yDD = (DecimalData) y;
                assert xDD.scale() == yDD.scale() : "Inconsistent scale of aggregate DecimalData!";
                assert xDD.precision() == yDD.precision()
                        : "Inconsistent precision of aggregate DecimalData!";
                ret = DecimalDataUtils.compare(xDD, yDD);
                break;
            case TINYINT:
                ret = Byte.compare((byte) x, (byte) y);
                break;
            case SMALLINT:
                ret = Short.compare((short) x, (short) y);
                break;
            case INTEGER:
            case DATE:
                ret = Integer.compare((int) x, (int) y);
                break;
            case BIGINT:
                ret = Long.compare((long) x, (long) y);
                break;
            case FLOAT:
                ret = Float.compare((float) x, (float) y);
                break;
            case DOUBLE:
                ret = Double.compare((double) x, (double) y);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                TimestampData xDD1 = (TimestampData) x;
                TimestampData yDD1 = (TimestampData) y;
                ret = xDD1.compareTo(yDD1);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            default:
                throw new IllegalArgumentException();
        }
        return ret;
    }
}
