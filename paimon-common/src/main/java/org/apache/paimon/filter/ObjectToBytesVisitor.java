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

package org.apache.paimon.filter;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

/** Convert different type object to bytes. */
public class ObjectToBytesVisitor implements DataTypeVisitor<Function<Object, byte[]>> {

    public static final ObjectToBytesVisitor INSTANCE = new ObjectToBytesVisitor();

    public static final byte[] NULL_BYTES = new byte[1];

    static {
        Arrays.fill(NULL_BYTES, (byte) 0x00);
    }

    @Override
    public Function<Object, byte[]> visit(CharType charType) {
        return o -> o == null ? NULL_BYTES : ((BinaryString) o).toBytes();
    }

    @Override
    public Function<Object, byte[]> visit(VarCharType varCharType) {
        return o -> o == null ? NULL_BYTES : ((BinaryString) o).toBytes();
    }

    @Override
    public Function<Object, byte[]> visit(BooleanType booleanType) {
        return o -> o == null ? NULL_BYTES : ((Boolean) o) ? new byte[] {0x01} : new byte[] {0x00};
    }

    @Override
    public Function<Object, byte[]> visit(BinaryType binaryType) {
        return o -> o == null ? NULL_BYTES : (byte[]) o;
    }

    @Override
    public Function<Object, byte[]> visit(VarBinaryType varBinaryType) {
        return o -> o == null ? NULL_BYTES : (byte[]) o;
    }

    @Override
    public Function<Object, byte[]> visit(DecimalType decimalType) {
        return o -> o == null ? NULL_BYTES : ((Decimal) o).toUnscaledBytes();
    }

    @Override
    public Function<Object, byte[]> visit(TinyIntType tinyIntType) {
        return o -> o == null ? NULL_BYTES : new byte[] {(byte) o};
    }

    @Override
    public Function<Object, byte[]> visit(SmallIntType smallIntType) {
        return o ->
                o == null
                        ? NULL_BYTES
                        : new byte[] {(byte) ((short) o & 0xff), (byte) ((short) o >> 8 & 0xff)};
    }

    @Override
    public Function<Object, byte[]> visit(IntType intType) {
        return o -> o == null ? NULL_BYTES : intToBytes((int) o);
    }

    @Override
    public Function<Object, byte[]> visit(BigIntType bigIntType) {
        return o -> o == null ? NULL_BYTES : longToBytes((long) o);
    }

    @Override
    public Function<Object, byte[]> visit(FloatType floatType) {
        return o -> o == null ? NULL_BYTES : intToBytes(Float.floatToIntBits((float) o));
    }

    @Override
    public Function<Object, byte[]> visit(DoubleType doubleType) {
        return o -> o == null ? NULL_BYTES : longToBytes(Double.doubleToLongBits((double) o));
    }

    @Override
    public Function<Object, byte[]> visit(DateType dateType) {
        return o -> o == null ? NULL_BYTES : intToBytes((int) o);
    }

    @Override
    public Function<Object, byte[]> visit(TimeType timeType) {
        return o -> o == null ? NULL_BYTES : intToBytes((int) o);
    }

    @Override
    public Function<Object, byte[]> visit(TimestampType timestampType) {
        return o -> o == null ? NULL_BYTES : longToBytes(((Timestamp) o).getMillisecond());
    }

    @Override
    public Function<Object, byte[]> visit(LocalZonedTimestampType localZonedTimestampType) {
        return o -> o == null ? NULL_BYTES : longToBytes(((Timestamp) o).getMillisecond());
    }

    @Override
    public Function<Object, byte[]> visit(ArrayType arrayType) {
        BiFunction<DataGetters, Integer, byte[]> function =
                arrayType.getElementType().accept(InternalRowToBytesVisitor.INSTANCE);
        return o -> {
            if (o == null) {
                return NULL_BYTES;
            }
            InternalArray internalArray = (InternalArray) o;
            int count = 0;
            byte[][] bytes = new byte[internalArray.size()][];
            for (int i = 0; i < internalArray.size(); i++) {
                bytes[i] = function.apply(internalArray, i);
                count += bytes[i].length;
            }

            byte[] result = new byte[count];
            int position = 0;
            for (int i = 0; i < internalArray.size(); i++) {
                System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                position += bytes[i].length;
            }
            return result;
        };
    }

    @Override
    public Function<Object, byte[]> visit(MultisetType multisetType) {
        BiFunction<DataGetters, Integer, byte[]> function =
                multisetType.getElementType().accept(InternalRowToBytesVisitor.INSTANCE);
        return o -> {
            if (o == null) {
                return NULL_BYTES;
            }
            InternalMap map = (InternalMap) o;

            int count = 0;
            byte[][] bytes = new byte[map.size()][];
            for (int i = 0; i < map.size(); i++) {
                bytes[i] = function.apply(map.keyArray(), i);
                count += bytes[i].length;
            }

            byte[] result = new byte[count];
            int position = 0;
            for (int i = 0; i < map.size(); i++) {
                System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                position += bytes[i].length;
            }
            return result;
        };
    }

    @Override
    public Function<Object, byte[]> visit(MapType mapType) {
        BiFunction<DataGetters, Integer, byte[]> keyFunction =
                mapType.getKeyType().accept(new InternalRowToBytesVisitor());
        BiFunction<DataGetters, Integer, byte[]> valueFunction =
                mapType.getValueType().accept(new InternalRowToBytesVisitor());

        return o -> {
            if (o == null) {
                return NULL_BYTES;
            }
            InternalMap map = (InternalMap) o;

            int count = 0;
            byte[][] keyBytes = new byte[map.size()][];
            for (int i = 0; i < map.size(); i++) {
                keyBytes[i] = keyFunction.apply(map.keyArray(), i);
                count += keyBytes[i].length;
            }

            byte[][] valueBytes = new byte[map.size()][];
            for (int i = 0; i < map.size(); i++) {
                valueBytes[i] = valueFunction.apply(map.valueArray(), i);
                count += valueBytes[i].length;
            }

            byte[] result = new byte[count];
            int position = 0;
            for (int i = 0; i < map.size(); i++) {
                System.arraycopy(keyBytes[i], 0, result, position, keyBytes[i].length);
                position += keyBytes[i].length;
                System.arraycopy(valueBytes[i], 0, result, position, valueBytes[i].length);
                position += valueBytes[i].length;
            }
            return result;
        };
    }

    @Override
    public Function<Object, byte[]> visit(RowType rowType) {
        BiFunction<DataGetters, Integer, byte[]> function =
                rowType.accept(new InternalRowToBytesVisitor());
        return o -> {
            if (o == null) {
                return NULL_BYTES;
            }
            InternalRow secondRow = (InternalRow) o;

            int count = 0;
            byte[][] bytes = new byte[rowType.getFieldCount()][];
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                bytes[i] = function.apply(secondRow, i);
                count += bytes[i].length;
            }

            byte[] result = new byte[count];
            int position = 0;
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                position += bytes[i].length;
            }
            return result;
        };
    }

    @VisibleForTesting
    static byte[] longToBytes(long x) {
        return new byte[] {
            (byte) (x & 0xff),
            (byte) (x >> 8 & 0xff),
            (byte) (x >> 16 & 0xff),
            (byte) (x >> 24 & 0xff),
            (byte) (x >> 32 & 0xff),
            (byte) (x >> 40 & 0xff),
            (byte) (x >> 48 & 0xff),
            (byte) (x >> 56 & 0xff)
        };
    }

    @VisibleForTesting
    static byte[] intToBytes(int x) {
        return new byte[] {
            (byte) (x & 0xff),
            (byte) (x >> 8 & 0xff),
            (byte) (x >> 16 & 0xff),
            (byte) (x >> 24 & 0xff)
        };
    }
}
