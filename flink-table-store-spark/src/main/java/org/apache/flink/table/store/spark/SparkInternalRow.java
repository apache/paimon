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

package org.apache.flink.table.store.spark;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.flink.table.store.utils.RowDataUtils.copyRowData;
import static org.apache.flink.table.store.utils.TypeUtils.timestampPrecision;

/** Spark {@link InternalRow} to wrap {@link RowData}. */
public class SparkInternalRow extends InternalRow {

    private final RowType rowType;

    private RowData row;

    public SparkInternalRow(RowType rowType) {
        this.rowType = rowType;
    }

    public SparkInternalRow replace(RowData row) {
        this.row = row;
        return this;
    }

    @Override
    public int numFields() {
        return row.getArity();
    }

    @Override
    public void setNullAt(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void update(int i, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow copy() {
        return new SparkInternalRow(rowType).replace(copyRowData(row, rowType));
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return row.isNullAt(ordinal);
    }

    @Override
    public boolean getBoolean(int ordinal) {
        return row.getBoolean(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        return row.getByte(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        return row.getShort(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
        return row.getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
        if (rowType.getTypeAt(ordinal) instanceof BigIntType) {
            return row.getLong(ordinal);
        }

        return getTimestampMicros(ordinal);
    }

    private long getTimestampMicros(int ordinal) {
        LogicalType type = rowType.getTypeAt(ordinal);
        return fromFlink(row.getTimestamp(ordinal, timestampPrecision(type)));
    }

    @Override
    public float getFloat(int ordinal) {
        return row.getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        return row.getDouble(ordinal);
    }

    @Override
    public Decimal getDecimal(int ordinal, int precision, int scale) {
        DecimalData decimal = row.getDecimal(ordinal, precision, scale);
        return fromFlink(decimal);
    }

    @Override
    public UTF8String getUTF8String(int ordinal) {
        return fromFlink(row.getString(ordinal));
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return row.getBinary(ordinal);
    }

    @Override
    public CalendarInterval getInterval(int ordinal) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
        return fromFlink(row.getRow(ordinal, numFields), (RowType) rowType.getTypeAt(ordinal));
    }

    @Override
    public ArrayData getArray(int ordinal) {
        return fromFlink(row.getArray(ordinal), (ArrayType) rowType.getTypeAt(ordinal));
    }

    @Override
    public MapData getMap(int ordinal) {
        return fromFlink(row.getMap(ordinal), rowType.getTypeAt(ordinal));
    }

    @Override
    public Object get(int ordinal, DataType dataType) {
        return SpecializedGettersReader.read(this, ordinal, dataType, true, true);
    }

    public static Object fromFlink(Object o, LogicalType type) {
        if (o == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return fromFlink((TimestampData) o);
            case CHAR:
            case VARCHAR:
                return fromFlink((StringData) o);
            case DECIMAL:
                return fromFlink((DecimalData) o);
            case ARRAY:
                return fromFlink((org.apache.flink.table.data.ArrayData) o, (ArrayType) type);
            case MAP:
            case MULTISET:
                return fromFlink((org.apache.flink.table.data.MapData) o, type);
            case ROW:
                return fromFlink((RowData) o, (RowType) type);
            default:
                return o;
        }
    }

    public static UTF8String fromFlink(StringData string) {
        return UTF8String.fromBytes(string.toBytes());
    }

    public static Decimal fromFlink(DecimalData decimal) {
        return Decimal.apply(decimal.toBigDecimal());
    }

    public static InternalRow fromFlink(RowData row, RowType rowType) {
        return new SparkInternalRow(rowType).replace(row);
    }

    public static long fromFlink(TimestampData timestamp) {
        return DateTimeUtils.fromJavaTimestamp(timestamp.toTimestamp());
    }

    public static ArrayData fromFlink(
            org.apache.flink.table.data.ArrayData array, ArrayType arrayType) {
        return fromFlinkArrayElementType(array, arrayType.getElementType());
    }

    private static ArrayData fromFlinkArrayElementType(
            org.apache.flink.table.data.ArrayData array, LogicalType elementType) {
        return new SparkArrayData(elementType).replace(array);
    }

    public static MapData fromFlink(org.apache.flink.table.data.MapData map, LogicalType mapType) {
        LogicalType keyType;
        LogicalType valueType;
        if (mapType instanceof MapType) {
            keyType = ((MapType) mapType).getKeyType();
            valueType = ((MapType) mapType).getValueType();
        } else if (mapType instanceof MultisetType) {
            keyType = ((MultisetType) mapType).getElementType();
            valueType = new IntType();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + mapType);
        }

        return new ArrayBasedMapData(
                fromFlinkArrayElementType(map.keyArray(), keyType),
                fromFlinkArrayElementType(map.valueArray(), valueType));
    }
}
