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

import org.apache.flink.table.store.data.BinaryString;
import org.apache.flink.table.store.data.Decimal;
import org.apache.flink.table.store.data.InternalArray;
import org.apache.flink.table.store.data.InternalMap;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.Timestamp;
import org.apache.flink.table.store.types.ArrayType;
import org.apache.flink.table.store.types.DataType;
import org.apache.flink.table.store.types.DateType;
import org.apache.flink.table.store.types.MapType;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.DateTimeUtils;

import org.apache.spark.sql.Row;

import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** A {@link InternalRow} wraps spark {@link Row}. */
public class SparkRow implements InternalRow {

    private final RowType type;
    private final Row row;

    public SparkRow(RowType type, Row row) {
        this.type = type;
        this.row = row;
    }

    @Override
    public int getFieldCount() {
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
    public BinaryString getString(int i) {
        return BinaryString.fromString(row.getString(i));
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        return Decimal.fromBigDecimal(row.getDecimal(i), precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        return toFlinkTimestamp(row.get(i));
    }

    @Override
    public byte[] getBinary(int i) {
        return row.getAs(i);
    }

    @Override
    public InternalArray getArray(int i) {
        return new FlinkArray(((ArrayType) type.getTypeAt(i)).getElementType(), row.getList(i));
    }

    @Override
    public InternalMap getMap(int i) {
        return toFlinkMap((MapType) type.getTypeAt(i), row.getJavaMap(i));
    }

    @Override
    public InternalRow getRow(int i, int i1) {
        return new SparkRow((RowType) type.getTypeAt(i), row.getStruct(i));
    }

    private static int toFlinkDate(Object object) {
        if (object instanceof Date) {
            return DateTimeUtils.toInternal((Date) object);
        } else {
            return DateTimeUtils.toInternal((LocalDate) object);
        }
    }

    private static Timestamp toFlinkTimestamp(Object object) {
        if (object instanceof java.sql.Timestamp) {
            return Timestamp.fromSQLTimestamp((java.sql.Timestamp) object);
        } else {
            return Timestamp.fromLocalDateTime((LocalDateTime) object);
        }
    }

    private static InternalMap toFlinkMap(MapType mapType, Map<Object, Object> map) {
        List<Object> keys = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        map.forEach(
                (k, v) -> {
                    keys.add(k);
                    values.add(v);
                });

        FlinkArray key = new FlinkArray(mapType.getKeyType(), keys);
        FlinkArray value = new FlinkArray(mapType.getValueType(), values);
        return new InternalMap() {
            @Override
            public int size() {
                return map.size();
            }

            @Override
            public InternalArray keyArray() {
                return key;
            }

            @Override
            public InternalArray valueArray() {
                return value;
            }
        };
    }

    private static class FlinkArray implements InternalArray {

        private final DataType elementType;
        private final List<Object> list;

        private FlinkArray(DataType elementType, List<Object> list) {
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
        public BinaryString getString(int i) {
            return BinaryString.fromString(getAs(i));
        }

        @Override
        public Decimal getDecimal(int i, int precision, int scale) {
            return Decimal.fromBigDecimal(getAs(i), precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int i, int precision) {
            return toFlinkTimestamp(getAs(i));
        }

        @Override
        public byte[] getBinary(int i) {
            return getAs(i);
        }

        @Override
        public InternalArray getArray(int i) {
            return new FlinkArray(((ArrayType) elementType).getElementType(), getAs(i));
        }

        @Override
        public InternalMap getMap(int i) {
            return toFlinkMap((MapType) elementType, getAs(i));
        }

        @Override
        public InternalRow getRow(int i, int i1) {
            return new SparkRow((RowType) elementType, getAs(i));
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
