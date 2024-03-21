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

package org.apache.paimon.fileindex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalRow;
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

/** Generate bytes from {@link InternalRow}. */
public class InternalRowToBytesVisitor
        implements DataTypeVisitor<BiFunction<DataGetters, Integer, byte[]>> {

    public static final InternalRowToBytesVisitor INSTANCE = new InternalRowToBytesVisitor();

    public static final byte[] NULL_BYTES = new byte[1];

    static {
        Arrays.fill(NULL_BYTES, (byte) 0x00);
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(CharType charType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(charType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getString(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(VarCharType varCharType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(varCharType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getString(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BooleanType booleanType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(booleanType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getBoolean(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BinaryType binaryType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(binaryType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getBinary(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(VarBinaryType varBinaryType) {
        final Function<Object, byte[]> convertor =
                ObjectToBytesVisitor.INSTANCE.visit(varBinaryType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getBinary(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DecimalType decimalType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(decimalType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(
                        row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale()));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TinyIntType tinyIntType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(tinyIntType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getByte(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(SmallIntType smallIntType) {
        final Function<Object, byte[]> convertor =
                ObjectToBytesVisitor.INSTANCE.visit(smallIntType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getShort(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(IntType intType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(intType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(BigIntType bigIntType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(bigIntType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getLong(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(FloatType floatType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(floatType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getFloat(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DoubleType doubleType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(doubleType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getDouble(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(DateType dateType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(dateType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TimeType timeType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(timeType);
        return (row, index) ->
                row.isNullAt(index) ? NULL_BYTES : convertor.apply(row.getInt(index));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(TimestampType timestampType) {
        final Function<Object, byte[]> convertor =
                ObjectToBytesVisitor.INSTANCE.visit(timestampType);
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : convertor.apply(row.getTimestamp(index, timestampType.getPrecision()));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(
            LocalZonedTimestampType localZonedTimestampType) {
        final Function<Object, byte[]> convertor =
                ObjectToBytesVisitor.INSTANCE.visit(localZonedTimestampType);
        return (row, index) ->
                row.isNullAt(index)
                        ? NULL_BYTES
                        : convertor.apply(
                                row.getTimestamp(index, localZonedTimestampType.getPrecision()));
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(ArrayType arrayType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(arrayType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getArray(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(MultisetType multisetType) {
        final Function<Object, byte[]> convertor =
                ObjectToBytesVisitor.INSTANCE.visit(multisetType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getMap(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(MapType mapType) {
        final Function<Object, byte[]> convertor = ObjectToBytesVisitor.INSTANCE.visit(mapType);
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                return convertor.apply(row.getMap(index));
            }
        };
    }

    @Override
    public BiFunction<DataGetters, Integer, byte[]> visit(RowType rowType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_BYTES;
            } else {
                List<BiFunction<DataGetters, Integer, byte[]>> functions = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    functions.add(rowType.getTypeAt(i).accept(this));
                }
                InternalRow secondRow = row.getRow(index, rowType.getFieldCount());

                int count = 0;
                byte[][] bytes = new byte[rowType.getFieldCount()][];
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    bytes[i] = functions.get(i).apply(secondRow, i);
                    count += bytes[i].length;
                }

                byte[] result = new byte[count];
                int position = 0;
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    System.arraycopy(bytes[i], 0, result, position, bytes[i].length);
                    position += bytes[i].length;
                }
                return result;
            }
        };
    }
}
