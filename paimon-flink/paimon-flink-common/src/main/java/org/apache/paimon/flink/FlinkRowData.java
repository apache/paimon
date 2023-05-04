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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import static org.apache.paimon.flink.FlinkRowWrapper.fromFlinkRowKind;

/** Convert to Flink row data. */
public class FlinkRowData implements RowData {

    private InternalRow row;

    public FlinkRowData(InternalRow row) {
        this.row = row;
    }

    public FlinkRowData replace(InternalRow row) {
        this.row = row;
        return this;
    }

    @Override
    public int getArity() {
        return row.getFieldCount();
    }

    @Override
    public RowKind getRowKind() {
        return toFlinkRowKind(row.getRowKind());
    }

    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(fromFlinkRowKind(kind));
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
    public StringData getString(int pos) {
        return toFlinkString(row.getString(pos));
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        return toFlinkDecimal(row.getDecimal(pos, precision, scale));
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return toFlinkTimestamp(row.getTimestamp(pos, precision));
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int pos) {
        return row.getBinary(pos);
    }

    @Override
    public ArrayData getArray(int pos) {
        return new FlinkArrayData(row.getArray(pos));
    }

    @Override
    public MapData getMap(int pos) {
        return new FlinkMapData(row.getMap(pos));
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        return new FlinkRowData(row.getRow(pos, numFields));
    }

    private static class FlinkArrayData implements ArrayData {

        private final InternalArray array;

        private FlinkArrayData(InternalArray array) {
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
        public StringData getString(int pos) {
            return toFlinkString(array.getString(pos));
        }

        @Override
        public DecimalData getDecimal(int pos, int precision, int scale) {
            return toFlinkDecimal(array.getDecimal(pos, precision, scale));
        }

        @Override
        public TimestampData getTimestamp(int pos, int precision) {
            return toFlinkTimestamp(array.getTimestamp(pos, precision));
        }

        @Override
        public <T> RawValueData<T> getRawValue(int pos) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getBinary(int pos) {
            return array.getBinary(pos);
        }

        @Override
        public ArrayData getArray(int pos) {
            return new FlinkArrayData(array.getArray(pos));
        }

        @Override
        public MapData getMap(int pos) {
            return new FlinkMapData(array.getMap(pos));
        }

        @Override
        public RowData getRow(int pos, int numFields) {
            return new FlinkRowData(array.getRow(pos, numFields));
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

    private static class FlinkMapData implements MapData {

        private final InternalMap map;

        private FlinkMapData(InternalMap map) {
            this.map = map;
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public ArrayData keyArray() {
            return new FlinkArrayData(map.keyArray());
        }

        @Override
        public ArrayData valueArray() {
            return new FlinkArrayData(map.valueArray());
        }
    }

    public static StringData toFlinkString(BinaryString str) {
        return StringData.fromBytes(str.toBytes());
    }

    public static TimestampData toFlinkTimestamp(Timestamp timestamp) {
        return TimestampData.fromEpochMillis(
                timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    public static DecimalData toFlinkDecimal(Decimal decimal) {
        if (decimal.isCompact()) {
            return DecimalData.fromUnscaledLong(
                    decimal.toUnscaledLong(), decimal.precision(), decimal.scale());
        } else {
            return DecimalData.fromBigDecimal(
                    decimal.toBigDecimal(), decimal.precision(), decimal.scale());
        }
    }

    public static RowKind toFlinkRowKind(org.apache.paimon.types.RowKind rowKind) {
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
}
