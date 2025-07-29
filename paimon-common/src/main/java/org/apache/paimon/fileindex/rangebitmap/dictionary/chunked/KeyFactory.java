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

package org.apache.paimon.fileindex.rangebitmap.dictionary.chunked;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndexFactory;
import org.apache.paimon.fs.SeekableInputStream;
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
import org.apache.paimon.utils.Preconditions;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.Function;

/** Common implementation of {@link Chunk}. */
public interface KeyFactory {

    static KeyFactory create(DataType type) {
        return type.accept(
                new DataTypeDefaultVisitor<KeyFactory>() {

                    @Override
                    public KeyFactory visit(CharType charType) {
                        return new StringKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(VarCharType varCharType) {
                        return new StringKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(BooleanType booleanType) {
                        return new BooleanKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(DecimalType decimalType) {
                        return new DecimalKeyFactory(decimalType.getPrecision());
                    }

                    @Override
                    public KeyFactory visit(TinyIntType tinyIntType) {
                        return new TinyIntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(SmallIntType smallIntType) {
                        return new SmallIntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(IntType intType) {
                        return new IntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(BigIntType bigIntType) {
                        return new BigIntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(FloatType floatType) {
                        return new FloatKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(DoubleType doubleType) {
                        return new DoubleKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(DateType dateType) {
                        return new IntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(TimeType timeType) {
                        return new IntKeyFactory();
                    }

                    @Override
                    public KeyFactory visit(TimestampType timestampType) {
                        return new TimestampKeyFactory(timestampType.getPrecision());
                    }

                    @Override
                    public KeyFactory visit(LocalZonedTimestampType localZonedTimestampType) {
                        return new TimestampKeyFactory(localZonedTimestampType.getPrecision());
                    }

                    @Override
                    protected KeyFactory defaultMethod(DataType dataType) {
                        throw new UnsupportedOperationException(
                                "data type " + dataType + "is not supported.");
                    }
                });
    }

    default Function<Object, Object> createConverter() {
        return Function.identity();
    }

    default String defaultChunkSize() {
        return "16kb";
    }

    Comparator<Object> createComparator();

    KeySerializer createSerializer();

    KeyDeserializer createDeserializer();

    Chunk createChunk(Object key, int code, int offset, int limitedSerializedSizeInBytes);

    Chunk mmapChunk(ByteBuffer buffer, int offset, SeekableInputStream in);

    /** The key serializer. */
    interface KeySerializer {
        void serialize(ByteBuffer buffer, Object key);

        int serializedSizeInBytes(Object key);
    }

    /** The key deserializer. */
    interface KeyDeserializer {
        Object deserialize(ByteBuffer buffer);
    }

    /** Abstract KeyFactory for fixed length chunk. */
    abstract class FixedLengthKeyFactory implements KeyFactory {

        @Override
        public Chunk createChunk(
                Object key, int code, int offset, int limitedSerializedSizeInBytes) {
            return new FixedLengthChunk(
                    key,
                    code,
                    offset,
                    limitedSerializedSizeInBytes,
                    createSerializer(),
                    createComparator());
        }

        @Override
        public Chunk mmapChunk(ByteBuffer buffer, int offset, SeekableInputStream in) {
            return new FixedLengthChunk(
                    buffer, offset, in, createDeserializer(), createComparator());
        }
    }

    /** Abstract KeyFactory for variable length chunk. */
    abstract class VariableLengthKeyFactory implements KeyFactory {

        @Override
        public Chunk createChunk(
                Object key, int code, int offset, int limitedSerializedSizeInBytes) {
            return new VariableLengthChunk(
                    key,
                    code,
                    offset,
                    limitedSerializedSizeInBytes,
                    createSerializer(),
                    createComparator());
        }

        @Override
        public Chunk mmapChunk(ByteBuffer buffer, int offset, SeekableInputStream in) {
            return new VariableLengthChunk(
                    buffer, offset, in, createDeserializer(), createComparator());
        }
    }

    /** KeyFactory for INT & DATE & TIME. */
    class IntKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Integer) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object input) {
                    buffer.putInt((Integer) input);
                }

                @Override
                public int serializedSizeInBytes(Object input) {
                    return Integer.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {

                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.getInt();
                }
            };
        }
    }

    /** KeyFactory for BIGINT. */
    class BigIntKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Long) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object input) {
                    buffer.putLong((Long) input);
                }

                @Override
                public int serializedSizeInBytes(Object input) {
                    return Long.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {

                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.getLong();
                }
            };
        }
    }

    /** KeyFactory for BOOLEAN. */
    class BooleanKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Boolean) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object input) {
                    buffer.put(Boolean.TRUE.equals(input) ? (byte) 1 : (byte) 0);
                }

                @Override
                public int serializedSizeInBytes(Object input) {
                    return Byte.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {

                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.get() == (byte) 1;
                }
            };
        }

        @Override
        public String defaultChunkSize() {
            // no chunking required
            return "0b";
        }
    }

    /** KeyFactory for TINYINT. */
    class TinyIntKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Byte) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object key) {
                    buffer.put((Byte) key);
                }

                @Override
                public int serializedSizeInBytes(Object key) {
                    return Byte.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {
                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.get();
                }
            };
        }

        @Override
        public String defaultChunkSize() {
            // no chunking required
            return "0b";
        }
    }

    /** KeyFactory for SMALLINT. */
    class SmallIntKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Short) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object key) {
                    buffer.putShort((Short) key);
                }

                @Override
                public int serializedSizeInBytes(Object key) {
                    return Short.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {
                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.getShort();
                }
            };
        }

        @Override
        public String defaultChunkSize() {
            // no chunking required
            return "0b";
        }
    }

    /** KeyFactory for FLOAT. */
    class FloatKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Float) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object key) {
                    buffer.putFloat((Float) key);
                }

                @Override
                public int serializedSizeInBytes(Object key) {
                    return Float.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {
                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.getFloat();
                }
            };
        }
    }

    /** KeyFactory for DOUBLE. */
    class DoubleKeyFactory extends FixedLengthKeyFactory {

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((Double) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object key) {
                    buffer.putDouble((Double) key);
                }

                @Override
                public int serializedSizeInBytes(Object key) {
                    return Double.BYTES;
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {
                @Override
                public Object deserialize(ByteBuffer buffer) {
                    return buffer.getDouble();
                }
            };
        }
    }

    /** KeyFactory for DECIMAL. */
    class DecimalKeyFactory extends BigIntKeyFactory {

        public DecimalKeyFactory(int precision) {
            Preconditions.checkArgument(
                    precision <= 18,
                    String.format(
                            "The %s index only supports DECIMAL with a precision in [0, 18].",
                            RangeBitmapFileIndexFactory.RANGE_BITMAP));
        }

        @Override
        public Function<Object, Object> createConverter() {
            return o -> o == null ? null : ((Decimal) o).toUnscaledLong();
        }
    }

    /** KeyFactory for TIMESTAMP & TIMESTAMP_LTZ. */
    class TimestampKeyFactory extends BigIntKeyFactory {

        private final int precision;

        public TimestampKeyFactory(int precision) {
            Preconditions.checkArgument(
                    precision <= 6,
                    String.format(
                            "The %s index only supports TIMESTAMP with a precision in [0, 6].",
                            RangeBitmapFileIndexFactory.RANGE_BITMAP));
            this.precision = precision;
        }

        @Override
        public Function<Object, Object> createConverter() {
            return o -> {
                if (o == null) {
                    return null;
                } else if (precision <= 3) {
                    return ((Timestamp) o).getMillisecond();
                } else {
                    return ((Timestamp) o).toMicros();
                }
            };
        }
    }

    /** KeyFactory for STRING & VARCHAR & CHAR. */
    class StringKeyFactory extends VariableLengthKeyFactory {

        @Override
        public Function<Object, Object> createConverter() {
            return o -> o == null ? null : ((BinaryString) o).copy();
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> ((BinaryString) o));
        }

        @Override
        public KeySerializer createSerializer() {
            return new KeySerializer() {
                @Override
                public void serialize(ByteBuffer buffer, Object input) {
                    byte[] bytes = ((BinaryString) input).toBytes();
                    buffer.putInt(bytes.length);
                    buffer.put(bytes);
                }

                @Override
                public int serializedSizeInBytes(Object input) {
                    return Integer.BYTES + ((BinaryString) input).getSizeInBytes();
                }
            };
        }

        @Override
        public KeyDeserializer createDeserializer() {
            return new KeyDeserializer() {
                @Override
                public Object deserialize(ByteBuffer buffer) {
                    int length = buffer.getInt();
                    byte[] bytes = new byte[length];
                    buffer.get(bytes);
                    return BinaryString.fromBytes(bytes);
                }
            };
        }
    }
}
