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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.util.Comparator;

/** This interface provides core methods to ser/de and compare btree index keys. */
public interface KeySerializer {

    byte[] serialize(Object key);

    Object deserialize(MemorySlice data);

    Comparator<Object> createComparator();

    static KeySerializer create(DataType type) {
        return type.accept(
                new DataTypeDefaultVisitor<KeySerializer>() {
                    @Override
                    public KeySerializer defaultMethod(DataType dataType) {
                        throw new UnsupportedOperationException(
                                "DataType: " + dataType + " is not supported by btree index now.");
                    }

                    @Override
                    public KeySerializer visit(CharType charType) {
                        return new StringSerializer();
                    }

                    @Override
                    public KeySerializer visit(VarCharType varCharType) {
                        return new StringSerializer();
                    }

                    @Override
                    public KeySerializer visit(TinyIntType tinyIntType) {
                        return new TinyIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(SmallIntType smallIntType) {
                        return new SmallIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(IntType intType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(BigIntType bigIntType) {
                        return new BigIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(BooleanType booleanType) {
                        return new BooleanSerializer();
                    }

                    @Override
                    public KeySerializer visit(DecimalType decimalType) {
                        return new DecimalSerializer(
                                decimalType.getPrecision(), decimalType.getScale());
                    }

                    @Override
                    public KeySerializer visit(FloatType floatType) {
                        return new FloatSerializer();
                    }

                    @Override
                    public KeySerializer visit(DoubleType doubleType) {
                        return new DoubleSerializer();
                    }

                    @Override
                    public KeySerializer visit(DateType dateType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(TimeType timeType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(TimestampType timestampType) {
                        return new TimestampSerializer(timestampType.getPrecision());
                    }

                    @Override
                    public KeySerializer visit(LocalZonedTimestampType localZonedTimestampType) {
                        return new TimestampSerializer(localZonedTimestampType.getPrecision());
                    }
                });
    }

    /** Serializer for int type. */
    class IntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(4);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeInt((Integer) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readInt(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Integer) o);
        }
    }

    /** Serializer for long type. */
    class BigIntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeLong((Long) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readLong(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Long) o);
        }
    }

    /** Serializer for tiny int type. */
    class TinyIntSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(byte) key};
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readByte(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Byte) o);
        }
    }

    /** Serializer for small int type. */
    class SmallIntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(2);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeShort((Short) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readShort(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Short) o);
        }
    }

    /** Serializer for boolean type. */
    class BooleanSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(Boolean) key ? (byte) 1 : (byte) 0};
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readByte(0) == (byte) 1 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Boolean) o);
        }
    }

    /** Serializer for float type. */
    class FloatSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(4);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeInt(Float.floatToIntBits((Float) key));
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return Float.intBitsToFloat(data.readInt(0));
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Float) o);
        }
    }

    /** Serializer for double type. */
    class DoubleSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeLong(Double.doubleToLongBits((Double) key));
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return Double.longBitsToDouble(data.readLong(0));
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Double) o);
        }
    }

    /** Serializer for decimal type. */
    class DecimalSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);
        private final int precision;
        private final int scale;

        public DecimalSerializer(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public byte[] serialize(Object key) {
            if (Decimal.isCompact(precision)) {
                keyOut.reset();
                keyOut.writeLong(((Decimal) key).toUnscaledLong());
                return keyOut.toSlice().copyBytes();
            }
            return ((Decimal) key).toUnscaledBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            if (Decimal.isCompact(precision)) {
                return Decimal.fromUnscaledLong(data.readLong(0), precision, scale);
            }
            return Decimal.fromUnscaledBytes(data.copyBytes(), precision, scale);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Decimal) o);
        }
    }

    /** Serializer for STRING type. */
    class StringSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return ((BinaryString) key).toBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return BinaryString.fromBytes(data.copyBytes());
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (BinaryString) o);
        }
    }

    /** Serializer for timestamp. */
    class TimestampSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);
        private final int precision;

        public TimestampSerializer(int precision) {
            this.precision = precision;
        }

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            if (Timestamp.isCompact(precision)) {
                keyOut.writeLong(((Timestamp) key).getMillisecond());
            } else {
                keyOut.writeLong(((Timestamp) key).getMillisecond());
                keyOut.writeVarLenInt(((Timestamp) key).getNanoOfMillisecond());
            }
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            if (Timestamp.isCompact(precision)) {
                return Timestamp.fromEpochMillis(data.readLong(0));
            }
            MemorySliceInput input = data.toInput();
            long millis = input.readLong();
            int nanos = input.readVarLenInt();
            return Timestamp.fromEpochMillis(millis, nanos);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Timestamp) o);
        }
    }
}
