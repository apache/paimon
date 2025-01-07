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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.paimon.flink.FlinkRowData.toFlinkRowKind;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;

/** Convert from Flink row data. */
public class FlinkRowWrapper implements InternalRow {

    private final org.apache.flink.table.data.RowData row;

    public FlinkRowWrapper(org.apache.flink.table.data.RowData row) {
        this.row = row;
    }

    @Override
    public int getFieldCount() {
        return row.getArity();
    }

    @Override
    public RowKind getRowKind() {
        return fromFlinkRowKind(row.getRowKind());
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(toFlinkRowKind(kind));
    }

    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return row.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return row.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return row.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return row.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return row.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return row.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return row.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return fromFlinkString(row.getString(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return fromFlinkDecimal(row.getDecimal(pos, precision, scale));
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return fromFlinkTimestamp(row.getTimestamp(pos, precision));
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalArray getArray(int pos) {
        return new FlinkArrayWrapper(row.getArray(pos));
    }

    @Override
    public InternalMap getMap(int pos) {
        return new FlinkMapWrapper(row.getMap(pos));
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new FlinkRowWrapper(row.getRow(pos, numFields));
    }

    private static class FlinkArrayWrapper implements InternalArray {

        private final org.apache.flink.table.data.ArrayData array;

        private FlinkArrayWrapper(org.apache.flink.table.data.ArrayData array) {
            this.array = array;
        }

        @Override
        public int size() {
            return array.size();
        }

        @Override
        public boolean isNullAt(int pos) {
            return array.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return array.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return array.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return array.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return array.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return array.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return array.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return array.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return fromFlinkString(array.getString(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return fromFlinkDecimal(array.getDecimal(pos, precision, scale));
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return fromFlinkTimestamp(array.getTimestamp(pos, precision));
        }

        @Override
        public byte[] getBinary(int pos) {
            return array.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalArray getArray(int pos) {
            return new FlinkArrayWrapper(array.getArray(pos));
        }

        @Override
        public InternalMap getMap(int pos) {
            return new FlinkMapWrapper(array.getMap(pos));
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return new FlinkRowWrapper(array.getRow(pos, numFields));
        }

        @Override
        public boolean[] toBooleanArray() {
            return array.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return array.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return array.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return array.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return array.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return array.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return array.toDoubleArray();
        }
    }

    private static class FlinkMapWrapper implements InternalMap {

        private final org.apache.flink.table.data.MapData map;

        private FlinkMapWrapper(org.apache.flink.table.data.MapData map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public InternalArray keyArray() {
            return new FlinkArrayWrapper(map.keyArray());
        }

        @Override
        public InternalArray valueArray() {
            return new FlinkArrayWrapper(map.valueArray());
        }
    }

    public static BinaryString fromFlinkString(StringData str) {
        return BinaryString.fromBytes(str.toBytes());
    }

    public static Timestamp fromFlinkTimestamp(
            org.apache.flink.table.data.TimestampData timestamp) {
        return Timestamp.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    public static Decimal fromFlinkDecimal(DecimalData decimal) {
        if (decimal.isCompact()) {
            return Decimal.fromUnscaledLong(
                    decimal.toUnscaledLong(), decimal.precision(), decimal.scale());
        } else {
            return Decimal.fromBigDecimal(
                    decimal.toBigDecimal(), decimal.precision(), decimal.scale());
        }
    }

    public static RowKind fromFlinkRowKind(org.apache.flink.types.RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return RowKind.INSERT;
            case UPDATE_BEFORE:
                return RowKind.UPDATE_BEFORE;
            case UPDATE_AFTER:
                return RowKind.UPDATE_AFTER;
            case DELETE:
                return RowKind.DELETE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public static Object fromFlinkObject(Object o, LogicalType type) {
        if (o == null) {
            return null;
        }

        return InternalRow.createFieldGetter(toDataType(type), 0)
                .getFieldOrNull(new FlinkRowWrapper(GenericRowData.of(o)));
    }
}
