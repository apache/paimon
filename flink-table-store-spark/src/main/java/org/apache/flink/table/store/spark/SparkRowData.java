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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.store.utils.DateTimeUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.spark.sql.Row;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A {@link RowData} wraps spark {@link Row}. */
public class SparkRowData implements RowData {

    private final RowType type;
    private final Row row;

    public SparkRowData(RowType type, Row row) {
        this.type = type;
        this.row = row;
    }

    @Override
    public int getArity() {
        return row.size();
    }

    @Override
    public RowKind getRowKind() {
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        if (rowKind == RowKind.INSERT) {
            return;
        }

        throw new UnsupportedOperationException("Can not set row kind for this row except INSERT.");
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return row.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return row.getShort(i);
    }

    @Override
    public int getInt(int i) {
        if (type.getTypeAt(i) instanceof DateType) {
            return toFlinkDate(row.get(i));
        }
        return row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return row.getDouble(i);
    }

    @Override
    public StringData getString(int i) {
        return StringData.fromString(row.getString(i));
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        return DecimalData.fromBigDecimal(row.getDecimal(i), precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        return toFlinkTimestamp(row.get(i));
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        throw new UnsupportedOperationException(
                "Raw value is not supported in Spark, please use SQL types.");
    }

    @Override
    public byte[] getBinary(int i) {
        return row.getAs(i);
    }

    @Override
    public ArrayData getArray(int i) {
        return new FlinkArrayData(((ArrayType) type.getTypeAt(i)).getElementType(), row.getList(i));
    }

    @Override
    public MapData getMap(int i) {
        return toFlinkMap((MapType) type.getTypeAt(i), row.getJavaMap(i));
    }

    @Override
    public RowData getRow(int i, int i1) {
        return new SparkRowData((RowType) type.getTypeAt(i), row.getStruct(i));
    }

    private static int toFlinkDate(Object object) {
        if (object instanceof Date) {
            return DateTimeUtils.toInternal((Date) object);
        } else {
            return DateTimeUtils.toInternal((LocalDate) object);
        }
    }

    private static TimestampData toFlinkTimestamp(Object object) {
        if (object instanceof Timestamp) {
            return TimestampData.fromTimestamp((Timestamp) object);
        } else {
            return TimestampData.fromLocalDateTime((LocalDateTime) object);
        }
    }

    private static MapData toFlinkMap(MapType mapType, Map<Object, Object> map) {
        List<Object> keys = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        map.forEach(
                (k, v) -> {
                    keys.add(k);
                    values.add(v);
                });

        FlinkArrayData key = new FlinkArrayData(mapType.getKeyType(), keys);
        FlinkArrayData value = new FlinkArrayData(mapType.getValueType(), values);
        return new MapData() {
            @Override
            public int size() {
                return map.size();
            }

            @Override
            public ArrayData keyArray() {
                return key;
            }

            @Override
            public ArrayData valueArray() {
                return value;
            }
        };
    }

    private static class FlinkArrayData implements ArrayData {

        private final LogicalType elementType;
        private final List<Object> list;

        private FlinkArrayData(LogicalType elementType, List<Object> list) {
            this.list = list;
            this.elementType = elementType;
        }

        @Override
        public int size() {
            return list.size();
        }

        @Override
        public boolean isNullAt(int i) {
            return list.get(i) == null;
        }

        @SuppressWarnings("unchecked")
        private <T> T getAs(int i) {
            return (T) list.get(i);
        }

        @Override
        public boolean getBoolean(int i) {
            return getAs(i);
        }

        @Override
        public byte getByte(int i) {
            return getAs(i);
        }

        @Override
        public short getShort(int i) {
            return getAs(i);
        }

        @Override
        public int getInt(int i) {
            if (elementType instanceof DateType) {
                return toFlinkDate(getAs(i));
            }
            return getAs(i);
        }

        @Override
        public long getLong(int i) {
            return getAs(i);
        }

        @Override
        public float getFloat(int i) {
            return getAs(i);
        }

        @Override
        public double getDouble(int i) {
            return getAs(i);
        }

        @Override
        public StringData getString(int i) {
            return StringData.fromString(getAs(i));
        }

        @Override
        public DecimalData getDecimal(int i, int precision, int scale) {
            return DecimalData.fromBigDecimal(getAs(i), precision, scale);
        }

        @Override
        public TimestampData getTimestamp(int i, int precision) {
            return toFlinkTimestamp(getAs(i));
        }

        @Override
        public <T> RawValueData<T> getRawValue(int i) {
            throw new UnsupportedOperationException(
                    "Raw value is not supported in Spark, please use SQL types.");
        }

        @Override
        public byte[] getBinary(int i) {
            return getAs(i);
        }

        @Override
        public ArrayData getArray(int i) {
            return new FlinkArrayData(((ArrayType) elementType).getElementType(), getAs(i));
        }

        @Override
        public MapData getMap(int i) {
            return toFlinkMap((MapType) elementType, getAs(i));
        }

        @Override
        public RowData getRow(int i, int i1) {
            return new SparkRowData((RowType) elementType, getAs(i));
        }

        @Override
        public boolean[] toBooleanArray() {
            boolean[] res = new boolean[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getBoolean(i);
            }
            return res;
        }

        @Override
        public byte[] toByteArray() {
            byte[] res = new byte[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getByte(i);
            }
            return res;
        }

        @Override
        public short[] toShortArray() {
            short[] res = new short[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getShort(i);
            }
            return res;
        }

        @Override
        public int[] toIntArray() {
            int[] res = new int[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getInt(i);
            }
            return res;
        }

        @Override
        public long[] toLongArray() {
            long[] res = new long[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getLong(i);
            }
            return res;
        }

        @Override
        public float[] toFloatArray() {
            float[] res = new float[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getFloat(i);
            }
            return res;
        }

        @Override
        public double[] toDoubleArray() {
            double[] res = new double[size()];
            for (int i = 0; i < size(); i++) {
                res[i] = getDouble(i);
            }
            return res;
        }
    }
}
