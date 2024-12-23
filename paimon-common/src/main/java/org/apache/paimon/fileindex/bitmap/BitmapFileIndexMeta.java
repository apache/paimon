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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
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
import org.apache.paimon.types.VariantType;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 *
 * <pre>
 * Bitmap file index format (V1)
 * +-------------------------------------------------+-----------------
 * ｜ version (1 byte)                               ｜
 * +-------------------------------------------------+
 * ｜ row count (4 bytes int)                        ｜
 * +-------------------------------------------------+
 * ｜ non-null value bitmap number (4 bytes int)     ｜
 * +-------------------------------------------------+
 * ｜ has null value (1 byte)                        ｜
 * +-------------------------------------------------+
 * ｜ null value offset (4 bytes if has null value)  ｜       HEAD
 * +-------------------------------------------------+
 * ｜ value 1 | offset 1                             ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2                             ｜
 * +-------------------------------------------------+
 * ｜ value 3 | offset 3                             ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 * ｜ serialized bitmap 1                            ｜
 * +-------------------------------------------------+
 * ｜ serialized bitmap 2                            ｜
 * +-------------------------------------------------+       BODY
 * ｜ serialized bitmap 3                            ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 *
 * value x:                       var bytes for any data type (as bitmap identifier)
 * offset:                        4 bytes int (when it is negative, it represents that there is only one value
 *                                  and its position is the inverse of the negative value)
 * </pre>
 */
public class BitmapFileIndexMeta {

    private final DataType dataType;

    private int rowCount;
    private int nonNullBitmapNumber;
    private boolean hasNullValue;
    private int nullValueOffset;
    private LinkedHashMap<Object, Integer> bitmapOffsets;

    public BitmapFileIndexMeta(DataType dataType) {
        this.dataType = dataType;
    }

    public BitmapFileIndexMeta(
            DataType dataType,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            LinkedHashMap<Object, Integer> bitmapOffsets) {
        this(dataType);
        this.rowCount = rowCount;
        this.nonNullBitmapNumber = nonNullBitmapNumber;
        this.hasNullValue = hasNullValue;
        this.nullValueOffset = nullValueOffset;
        this.bitmapOffsets = bitmapOffsets;
    }

    public int getRowCount() {
        return rowCount;
    }

    public boolean contains(Object bitmapId) {
        if (bitmapId == null) {
            return hasNullValue;
        }
        return bitmapOffsets.containsKey(bitmapId);
    }

    public int getOffset(Object bitmapId) {
        if (bitmapId == null) {
            return nullValueOffset;
        }
        return bitmapOffsets.get(bitmapId);
    }

    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter =
                dataType.accept(
                        new DataTypeVisitorAdapter<ThrowableConsumer>() {
                            @Override
                            public ThrowableConsumer visitBinaryString() {
                                return o -> {
                                    byte[] bytes = ((BinaryString) o).toBytes();
                                    out.writeInt(bytes.length);
                                    out.write(bytes);
                                };
                            }

                            @Override
                            public ThrowableConsumer visitByte() {
                                return o -> out.writeByte((byte) o);
                            }

                            @Override
                            public ThrowableConsumer visitShort() {
                                return o -> out.writeShort((short) o);
                            }

                            @Override
                            public ThrowableConsumer visitInt() {
                                return o -> out.writeInt((int) o);
                            }

                            @Override
                            public ThrowableConsumer visitLong() {
                                return o -> out.writeLong((long) o);
                            }

                            @Override
                            public ThrowableConsumer visitFloat() {
                                return o -> out.writeFloat((float) o);
                            }

                            @Override
                            public ThrowableConsumer visitDouble() {
                                return o -> out.writeDouble((double) o);
                            }
                        });

        out.writeInt(rowCount);
        out.writeInt(nonNullBitmapNumber);
        out.writeBoolean(hasNullValue);
        if (hasNullValue) {
            out.writeInt(nullValueOffset);
        }
        for (Map.Entry<Object, Integer> entry : bitmapOffsets.entrySet()) {
            valueWriter.accept(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    public void deserialize(DataInput in) throws Exception {

        ThrowableSupplier valueReader =
                dataType.accept(
                        new DataTypeVisitorAdapter<ThrowableSupplier>() {
                            @Override
                            public ThrowableSupplier visitBinaryString() {
                                return () -> {
                                    int length = in.readInt();
                                    byte[] bytes = new byte[length];
                                    in.readFully(bytes);
                                    return BinaryString.fromBytes(bytes);
                                };
                            }

                            @Override
                            public ThrowableSupplier visitByte() {
                                return in::readByte;
                            }

                            @Override
                            public ThrowableSupplier visitShort() {
                                return in::readShort;
                            }

                            @Override
                            public ThrowableSupplier visitInt() {
                                return in::readInt;
                            }

                            @Override
                            public ThrowableSupplier visitLong() {
                                return in::readLong;
                            }

                            @Override
                            public ThrowableSupplier visitFloat() {
                                return in::readFloat;
                            }

                            @Override
                            public ThrowableSupplier visitDouble() {
                                return in::readDouble;
                            }
                        });

        rowCount = in.readInt();
        nonNullBitmapNumber = in.readInt();
        hasNullValue = in.readBoolean();
        if (hasNullValue) {
            nullValueOffset = in.readInt();
        }
        bitmapOffsets = new LinkedHashMap<>();
        for (int i = 0; i < nonNullBitmapNumber; i++) {
            bitmapOffsets.put(valueReader.get(), in.readInt());
        }
    }

    /** functional interface. */
    public interface ThrowableConsumer {
        void accept(Object o) throws Exception;
    }

    /** functional interface. */
    public interface ThrowableSupplier {
        Object get() throws Exception;
    }

    /** simplified visitor. */
    public abstract static class DataTypeVisitorAdapter<R> implements DataTypeVisitor<R> {

        public abstract R visitBinaryString();

        public abstract R visitByte();

        public abstract R visitShort();

        public abstract R visitInt();

        public abstract R visitLong();

        public abstract R visitFloat();

        public abstract R visitDouble();

        @Override
        public final R visit(CharType charType) {
            return visitBinaryString();
        }

        @Override
        public final R visit(VarCharType varCharType) {
            return visitBinaryString();
        }

        @Override
        public final R visit(BooleanType booleanType) {
            return visitByte();
        }

        @Override
        public final R visit(BinaryType binaryType) {
            throw new UnsupportedOperationException("Does not support type binary");
        }

        @Override
        public final R visit(VarBinaryType varBinaryType) {
            throw new UnsupportedOperationException("Does not support type binary");
        }

        @Override
        public final R visit(DecimalType decimalType) {
            throw new UnsupportedOperationException("Does not support decimal");
        }

        @Override
        public final R visit(TinyIntType tinyIntType) {
            return visitByte();
        }

        @Override
        public final R visit(SmallIntType smallIntType) {
            return visitShort();
        }

        @Override
        public final R visit(IntType intType) {
            return visitInt();
        }

        @Override
        public final R visit(BigIntType bigIntType) {
            return visitLong();
        }

        @Override
        public final R visit(FloatType floatType) {
            return visitFloat();
        }

        @Override
        public final R visit(DoubleType doubleType) {
            return visitDouble();
        }

        @Override
        public final R visit(DateType dateType) {
            return visitInt();
        }

        @Override
        public final R visit(TimeType timeType) {
            return visitInt();
        }

        @Override
        public final R visit(ArrayType arrayType) {
            throw new UnsupportedOperationException("Does not support type array");
        }

        @Override
        public final R visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Does not support type mutiset");
        }

        @Override
        public final R visit(TimestampType timestampType) {
            return visitLong();
        }

        @Override
        public final R visit(LocalZonedTimestampType localZonedTimestampType) {
            return visitLong();
        }

        @Override
        public final R visit(MapType mapType) {
            throw new UnsupportedOperationException("Does not support type map");
        }

        @Override
        public final R visit(RowType rowType) {
            throw new UnsupportedOperationException("Does not support type row");
        }

        @Override
        public final R visit(VariantType rowType) {
            throw new UnsupportedOperationException("Does not support type variant");
        }
    }
}
