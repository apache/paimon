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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.spark.util.shim.TypeUtils$;
import org.apache.paimon.types.RowKind;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.paimon.shims.SparkShimLoader;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

import java.io.Serializable;
import java.math.BigDecimal;

/** Wrapper to fetch value from the spark internal row. */
public class SparkInternalRowWrapper implements InternalRow, Serializable {

    private transient org.apache.spark.sql.catalyst.InternalRow internalRow;
    private final int length;
    private final int rowKindIdx;
    private final StructType structType;

    public SparkInternalRowWrapper(
            org.apache.spark.sql.catalyst.InternalRow internalRow,
            int rowKindIdx,
            StructType structType,
            int length) {
        this.internalRow = internalRow;
        this.rowKindIdx = rowKindIdx;
        this.length = length;
        this.structType = structType;
    }

    public SparkInternalRowWrapper(int rowKindIdx, StructType structType, int length) {
        this.rowKindIdx = rowKindIdx;
        this.length = length;
        this.structType = structType;
    }

    public SparkInternalRowWrapper replace(org.apache.spark.sql.catalyst.InternalRow internalRow) {
        this.internalRow = internalRow;
        return this;
    }

    @Override
    public int getFieldCount() {
        return length;
    }

    @Override
    public RowKind getRowKind() {
        if (rowKindIdx != -1) {
            return RowKind.fromByteValue(internalRow.getByte(rowKindIdx));
        } else {
            return RowKind.INSERT;
        }
    }

    @Override
    public void setRowKind(RowKind kind) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int pos) {
        return internalRow.isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return internalRow.getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return internalRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return internalRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return internalRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return internalRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return internalRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return internalRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return BinaryString.fromBytes(internalRow.getUTF8String(pos).getBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        org.apache.spark.sql.types.Decimal decimal = internalRow.getDecimal(pos, precision, scale);
        BigDecimal bigDecimal = decimal.toJavaBigDecimal();
        return Decimal.fromBigDecimal(bigDecimal, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return convertToTimestamp(structType.fields()[pos].dataType(), internalRow.getLong(pos));
    }

    @Override
    public byte[] getBinary(int pos) {
        return internalRow.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return SparkShimLoader.shim().toPaimonVariant(internalRow, pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(internalRow.getBinary(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        return new SparkInternalArray(
                internalRow.getArray(pos),
                ((ArrayType) (structType.fields()[pos].dataType())).elementType());
    }

    @Override
    public InternalMap getMap(int pos) {
        MapType mapType = (MapType) structType.fields()[pos].dataType();
        return new SparkInternalMap(
                internalRow.getMap(pos), mapType.keyType(), mapType.valueType());
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return new SparkInternalRowWrapper(
                internalRow.getStruct(pos, numFields),
                -1,
                (StructType) structType.fields()[pos].dataType(),
                numFields);
    }

    private static Timestamp convertToTimestamp(DataType dataType, long micros) {
        if (dataType instanceof TimestampType) {
            if (TypeUtils$.MODULE$.treatPaimonTimestampTypeAsSparkTimestampType()) {
                return Timestamp.fromSQLTimestamp(DateTimeUtils.toJavaTimestamp(micros));
            } else {
                return Timestamp.fromMicros(micros);
            }
        } else if (dataType instanceof TimestampNTZType) {
            return Timestamp.fromMicros(micros);
        } else {
            throw new UnsupportedOperationException("Unsupported data type:" + dataType);
        }
    }

    /** adapt to spark internal array. */
    public static class SparkInternalArray implements InternalArray {

        private final ArrayData arrayData;
        private final DataType elementType;

        public SparkInternalArray(ArrayData arrayData, DataType elementType) {
            this.arrayData = arrayData;
            this.elementType = elementType;
        }

        @Override
        public int size() {
            return arrayData.numElements();
        }

        @Override
        public boolean[] toBooleanArray() {
            return arrayData.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return arrayData.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return arrayData.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return arrayData.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return arrayData.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return arrayData.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return arrayData.toDoubleArray();
        }

        @Override
        public boolean isNullAt(int pos) {
            return arrayData.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return arrayData.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return arrayData.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return arrayData.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return arrayData.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return arrayData.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return arrayData.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return arrayData.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return BinaryString.fromBytes(arrayData.getUTF8String(pos).getBytes());
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            org.apache.spark.sql.types.Decimal decimal =
                    arrayData.getDecimal(pos, precision, scale);
            return Decimal.fromBigDecimal(decimal.toJavaBigDecimal(), precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return convertToTimestamp(elementType, arrayData.getLong(pos));
        }

        @Override
        public byte[] getBinary(int pos) {
            return arrayData.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            return SparkShimLoader.shim().toPaimonVariant(arrayData, pos);
        }

        @Override
        public Blob getBlob(int pos) {
            return new BlobData(arrayData.getBinary(pos));
        }

        @Override
        public InternalArray getArray(int pos) {
            return new SparkInternalArray(
                    arrayData.getArray(pos), ((ArrayType) elementType).elementType());
        }

        @Override
        public InternalMap getMap(int pos) {
            MapType mapType = (MapType) elementType;
            return new SparkInternalMap(
                    arrayData.getMap(pos), mapType.keyType(), mapType.valueType());
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return new SparkInternalRowWrapper(
                    arrayData.getStruct(pos, numFields), -1, (StructType) elementType, numFields);
        }
    }

    /** adapt to spark internal map. */
    public static class SparkInternalMap implements InternalMap {

        private final MapData mapData;
        private final DataType keyType;
        private final DataType valueType;

        public SparkInternalMap(MapData mapData, DataType keyType, DataType valueType) {
            this.mapData = mapData;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public int size() {
            return mapData.numElements();
        }

        @Override
        public InternalArray keyArray() {
            return new SparkInternalArray(mapData.keyArray(), keyType);
        }

        @Override
        public InternalArray valueArray() {
            return new SparkInternalArray(mapData.valueArray(), valueType);
        }
    }
}
