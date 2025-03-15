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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

import java.io.Serializable;

/** Wrapper of Spark {@link InternalRow}. */
public class SparkInternalRowWrapper implements org.apache.paimon.data.InternalRow, Serializable {
    private final RowType paimonRowType;
    private final InternalRow sparkInternalRow;
    private final RowKind rowKind;

    public SparkInternalRowWrapper(RowType paimonType, InternalRow sparkInternalRow) {
        this(paimonType, RowKind.INSERT, sparkInternalRow);
    }

    public SparkInternalRowWrapper(
            RowType paimonType, RowKind rowKind, InternalRow sparkInternalRow) {
        this.paimonRowType = paimonType;
        this.rowKind = rowKind;
        this.sparkInternalRow = sparkInternalRow;
    }

    @Override
    public int getFieldCount() {
        return sparkInternalRow.numFields();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        throw new UnsupportedOperationException(
                "SparkInternalRowWrapper does not support modifying row kind");
    }

    @Override
    public boolean isNullAt(int i) {
        return sparkInternalRow.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return sparkInternalRow.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return sparkInternalRow.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return sparkInternalRow.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return sparkInternalRow.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return sparkInternalRow.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return sparkInternalRow.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return sparkInternalRow.getDouble(i);
    }

    @Override
    public BinaryString getString(int i) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : BinaryString.fromBytes(sparkInternalRow.getUTF8String(i).getBytes());
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : Decimal.fromBigDecimal(
                        sparkInternalRow.getDecimal(i, precision, scale).toJavaBigDecimal(),
                        precision,
                        scale);
    }

    @Override
    public Timestamp getTimestamp(int i, int precision) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : Timestamp.fromMicros(sparkInternalRow.getLong(i));
    }

    @Override
    public byte[] getBinary(int i) {
        return sparkInternalRow.getBinary(i);
    }

    @Override
    public Variant getVariant(int pos) {
        //        return SparkShimLoader.getSparkShim().toPaimonVariant(row.getAs(i));
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalArray getArray(int i) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : new SparkArrayDataWrapper(
                        ((ArrayType) paimonRowType.getTypeAt(i)).getElementType(),
                        sparkInternalRow.getArray(i));
    }

    @Override
    public InternalMap getMap(int i) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : toPaimonInternalMap(
                        (MapType) paimonRowType.getTypeAt(i), sparkInternalRow.getMap(i));
    }

    @Override
    public org.apache.paimon.data.InternalRow getRow(int i, int numFields) {
        return sparkInternalRow.isNullAt(i)
                ? null
                : new SparkInternalRowWrapper(
                        (RowType) paimonRowType.getTypeAt(i),
                        sparkInternalRow.getStruct(i, numFields));
    }

    private static InternalMap toPaimonInternalMap(MapType mapType, MapData sparkMapData) {
        SparkArrayDataWrapper keyArrayWrapper =
                new SparkArrayDataWrapper(mapType.getKeyType(), sparkMapData.keyArray());
        SparkArrayDataWrapper valueArrayWrapper =
                new SparkArrayDataWrapper(mapType.getValueType(), sparkMapData.valueArray());
        return new InternalMap() {
            @Override
            public int size() {
                return sparkMapData.numElements();
            }

            @Override
            public InternalArray keyArray() {
                return keyArrayWrapper;
            }

            @Override
            public InternalArray valueArray() {
                return valueArrayWrapper;
            }
        };
    }

    private static class SparkArrayDataWrapper implements InternalArray {

        private final DataType elementPaimonType;
        private final ArrayData sparkArray;

        private SparkArrayDataWrapper(DataType elementPaimonType, ArrayData sparkArray) {
            this.sparkArray = sparkArray;
            this.elementPaimonType = elementPaimonType;
        }

        @Override
        public int size() {
            return sparkArray.numElements();
        }

        @Override
        public boolean isNullAt(int i) {
            return sparkArray.isNullAt(i);
        }

        @Override
        public boolean getBoolean(int i) {
            return sparkArray.getBoolean(i);
        }

        @Override
        public byte getByte(int i) {
            return sparkArray.getByte(i);
        }

        @Override
        public short getShort(int i) {
            return sparkArray.getShort(i);
        }

        @Override
        public int getInt(int i) {
            return sparkArray.getInt(i);
        }

        @Override
        public long getLong(int i) {
            return sparkArray.getLong(i);
        }

        @Override
        public float getFloat(int i) {
            return sparkArray.getFloat(i);
        }

        @Override
        public double getDouble(int i) {
            return sparkArray.getDouble(i);
        }

        @Override
        public BinaryString getString(int i) {
            return sparkArray.isNullAt(i)
                    ? null
                    : BinaryString.fromBytes(sparkArray.getUTF8String(i).getBytes());
        }

        @Override
        public Decimal getDecimal(int i, int precision, int scale) {
            return sparkArray.isNullAt(i)
                    ? null
                    : Decimal.fromBigDecimal(
                            sparkArray.getDecimal(i, precision, scale).toJavaBigDecimal(),
                            precision,
                            scale);
        }

        @Override
        public Timestamp getTimestamp(int i, int precision) {
            return sparkArray.isNullAt(i) ? null : Timestamp.fromMicros(sparkArray.getLong(i));
        }

        @Override
        public byte[] getBinary(int i) {
            return sparkArray.getBinary(i);
        }

        @Override
        public Variant getVariant(int pos) {
            // TODO
            throw new UnsupportedOperationException();
        }

        @Override
        public InternalArray getArray(int i) {
            return sparkArray.isNullAt(i)
                    ? null
                    : new SparkArrayDataWrapper(
                            ((ArrayType) elementPaimonType).getElementType(),
                            sparkArray.getArray(i));
        }

        @Override
        public InternalMap getMap(int i) {
            return sparkArray.isNullAt(i)
                    ? null
                    : toPaimonInternalMap((MapType) elementPaimonType, sparkArray.getMap(i));
        }

        @Override
        public org.apache.paimon.data.InternalRow getRow(int i, int numFields) {
            return sparkArray.isNullAt(i)
                    ? null
                    : new SparkInternalRowWrapper(
                            (RowType) elementPaimonType, sparkArray.getStruct(i, numFields));
        }

        @Override
        public boolean[] toBooleanArray() {
            return sparkArray.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return sparkArray.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return sparkArray.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return sparkArray.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return sparkArray.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return sparkArray.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return sparkArray.toDoubleArray();
        }
    }
}
