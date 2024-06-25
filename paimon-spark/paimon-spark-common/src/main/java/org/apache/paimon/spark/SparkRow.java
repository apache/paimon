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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

/** A {@link InternalRow} wraps spark {@link Row}. */
public class SparkRow implements InternalRow, Serializable {

    private final RowType type;
    private final Row row;
    private final RowKind rowKind;

    public SparkRow(RowType type, Row row) {
        this(type, row, RowKind.INSERT);
    }

    public SparkRow(RowType type, Row row, RowKind rowkind) {
        this.type = type;
        this.row = row;
        this.rowKind = rowkind;
    }

    @Override
    public int getFieldCount() {
        return type.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        throw new UnsupportedOperationException(
                "Spark row does not support modifying rowkind field");
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
            return toPaimonDate(row.get(i));
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
        return toPaimonTimestamp(row.get(i));
    }

    @Override
    public byte[] getBinary(int i) {
        return row.getAs(i);
    }

    @Override
    public InternalArray getArray(int i) {
        return new PaimonArray(((ArrayType) type.getTypeAt(i)).getElementType(), row.getList(i));
    }

    @Override
    public InternalMap getMap(int i) {
        return toPaimonMap((MapType) type.getTypeAt(i), row.getJavaMap(i));
    }

    @Override
    public InternalRow getRow(int i, int i1) {
        return new SparkRow((RowType) type.getTypeAt(i), row.getStruct(i));
    }

    private static int toPaimonDate(Object object) {
        if (object instanceof Date) {
            return DateTimeUtils.toInternal((Date) object);
        } else {
            return DateTimeUtils.toInternal((LocalDate) object);
        }
    }

    private static Timestamp toPaimonTimestamp(Object object) {
        if (object instanceof java.sql.Timestamp) {
            return Timestamp.fromSQLTimestamp((java.sql.Timestamp) object);
        } else if (object instanceof java.time.Instant) {
            LocalDateTime localDateTime =
                    LocalDateTime.ofInstant((Instant) object, ZoneId.systemDefault());
            return Timestamp.fromLocalDateTime(localDateTime);
        } else {
            return Timestamp.fromLocalDateTime((LocalDateTime) object);
        }
    }

    private static InternalMap toPaimonMap(MapType mapType, Map<Object, Object> map) {
        List<Object> keys = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        map.forEach(
                (k, v) -> {
                    keys.add(k);
                    values.add(v);
                });

        PaimonArray key = new PaimonArray(mapType.getKeyType(), keys);
        PaimonArray value = new PaimonArray(mapType.getValueType(), values);
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

    private static class PaimonArray implements InternalArray {

        private final DataType elementType;
        private final List<Object> list;

        private PaimonArray(DataType elementType, List<Object> list) {
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
                return toPaimonDate(getAs(i));
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
            return toPaimonTimestamp(getAs(i));
        }

        @Override
        public byte[] getBinary(int i) {
            return getAs(i);
        }

        @Override
        public InternalArray getArray(int i) {
            Object array = getAs(i);
            if (array instanceof WrappedArray) {
                List<Object> result = Lists.newArrayList();
                ((WrappedArray) array).iterator().foreach(x -> result.add(x));
                return new PaimonArray(((ArrayType) elementType).getElementType(), result);
            }
            return new PaimonArray(
                    ((ArrayType) elementType).getElementType(), (List<Object>) array);
        }

        @Override
        public InternalMap getMap(int i) {
            Object map = getAs(i);
            if (map instanceof scala.collection.immutable.Map) {
                return toPaimonMap(
                        (MapType) elementType,
                        JavaConverters.mapAsJavaMap(
                                (scala.collection.immutable.Map<Object, Object>) map));
            }
            return toPaimonMap((MapType) elementType, (Map<Object, Object>) map);
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
