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
import java.util.HashMap;
import java.util.Map;

/** Wrapper to fetch value from the spark internal row. */
public class SparkInternalRowWrapper implements InternalRow, Serializable {

    private transient org.apache.spark.sql.catalyst.InternalRow internalRow;
    private final int length;
    private final int rowKindIdx;
    private final StructType tableSchema;
    private int[] fieldIndexMap = null;

    public SparkInternalRowWrapper(
            org.apache.spark.sql.catalyst.InternalRow internalRow,
            int rowKindIdx,
            StructType tableSchema,
            int length) {
        this.internalRow = internalRow;
        this.rowKindIdx = rowKindIdx;
        this.length = length;
        this.tableSchema = tableSchema;
    }

    public SparkInternalRowWrapper(int rowKindIdx, StructType tableSchema, int length) {
        this.rowKindIdx = rowKindIdx;
        this.length = length;
        this.tableSchema = tableSchema;
    }

    public SparkInternalRowWrapper(
            int rowKindIdx, StructType tableSchema, StructType dataSchema, int length) {
        this.rowKindIdx = rowKindIdx;
        this.length = length;
        this.tableSchema = tableSchema;
        this.fieldIndexMap = buildFieldIndexMap(tableSchema, dataSchema);
    }

    public SparkInternalRowWrapper replace(org.apache.spark.sql.catalyst.InternalRow internalRow) {
        this.internalRow = internalRow;
        return this;
    }

    private int[] buildFieldIndexMap(StructType schemaStruct, StructType dataSchema) {
        int[] mapping = new int[schemaStruct.size()];

        Map<String, Integer> rowFieldIndexMap = new HashMap<>();
        for (int i = 0; i < dataSchema.size(); i++) {
            rowFieldIndexMap.put(dataSchema.fields()[i].name(), i);
        }

        for (int i = 0; i < schemaStruct.size(); i++) {
            String fieldName = schemaStruct.fields()[i].name();
            Integer index = rowFieldIndexMap.get(fieldName);
            mapping[i] = (index != null) ? index : -1;
        }

        return mapping;
    }

    private int getActualFieldPosition(int pos) {
        if (fieldIndexMap == null) {
            return pos;
        } else {
            if (pos < 0 || pos >= fieldIndexMap.length) {
                return -1;
            }
            return fieldIndexMap[pos];
        }
    }

    private int validateAndGetActualPosition(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1) {
            throw new ArrayIndexOutOfBoundsException("Field index out of bounds: " + pos);
        }
        return actualPos;
    }

    @Override
    public int getFieldCount() {
        return length;
    }

    @Override
    public RowKind getRowKind() {
        if (rowKindIdx != -1) {
            int actualPos = getActualFieldPosition(rowKindIdx);
            if (actualPos != -1) {
                return RowKind.fromByteValue(internalRow.getByte(actualPos));
            }
        }
        return RowKind.INSERT;
    }

    @Override
    public void setRowKind(RowKind kind) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1) {
            return true;
        }
        return internalRow.isNullAt(actualPos);
    }

    @Override
    public boolean getBoolean(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getBoolean(actualPos);
    }

    @Override
    public byte getByte(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getByte(actualPos);
    }

    @Override
    public short getShort(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getShort(actualPos);
    }

    @Override
    public int getInt(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getInt(actualPos);
    }

    @Override
    public long getLong(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getLong(actualPos);
    }

    @Override
    public float getFloat(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getFloat(actualPos);
    }

    @Override
    public double getDouble(int pos) {
        int actualPos = validateAndGetActualPosition(pos);
        return internalRow.getDouble(actualPos);
    }

    @Override
    public BinaryString getString(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return BinaryString.fromBytes(internalRow.getUTF8String(actualPos).getBytes());
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        org.apache.spark.sql.types.Decimal decimal =
                internalRow.getDecimal(actualPos, precision, scale);
        BigDecimal bigDecimal = decimal.toJavaBigDecimal();
        return Decimal.fromBigDecimal(bigDecimal, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return convertToTimestamp(
                tableSchema.fields()[pos].dataType(), internalRow.getLong(actualPos));
    }

    @Override
    public byte[] getBinary(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return internalRow.getBinary(actualPos);
    }

    @Override
    public Variant getVariant(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return SparkShimLoader.shim().toPaimonVariant(internalRow, actualPos);
    }

    @Override
    public Blob getBlob(int pos) {
        return new BlobData(internalRow.getBinary(pos));
    }

    @Override
    public InternalArray getArray(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return new SparkInternalArray(
                internalRow.getArray(actualPos),
                ((ArrayType) (tableSchema.fields()[pos].dataType())).elementType());
    }

    @Override
    public InternalMap getMap(int pos) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        MapType mapType = (MapType) tableSchema.fields()[pos].dataType();
        return new SparkInternalMap(
                internalRow.getMap(actualPos), mapType.keyType(), mapType.valueType());
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        int actualPos = getActualFieldPosition(pos);
        if (actualPos == -1 || internalRow.isNullAt(actualPos)) {
            return null;
        }
        return new SparkInternalRowWrapper(
                internalRow.getStruct(actualPos, numFields),
                -1,
                (StructType) tableSchema.fields()[actualPos].dataType(),
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
