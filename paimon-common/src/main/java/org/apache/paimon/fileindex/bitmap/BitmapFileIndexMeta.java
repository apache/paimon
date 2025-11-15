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
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 *
 *
 * <pre>
 * Bitmap file index format (V1)
 * +-------------------------------------------------+-----------------
 * | version (1 byte)                                |
 * +-------------------------------------------------+
 * | row count (4 bytes int)                         |
 * +-------------------------------------------------+
 * | non-null value bitmap number (4 bytes int)      |
 * +-------------------------------------------------+
 * | has null value (1 byte)                         |
 * +-------------------------------------------------+
 * | null value offset (4 bytes if has null value)   |       HEAD
 * +-------------------------------------------------+
 * | value 1 | offset 1                              |
 * +-------------------------------------------------+
 * | value 2 | offset 2                              |
 * +-------------------------------------------------+
 * | value 3 | offset 3                              |
 * +-------------------------------------------------+
 * | ...                                             |
 * +-------------------------------------------------+-----------------
 * | serialized bitmap 1                             |
 * +-------------------------------------------------+
 * | serialized bitmap 2                             |
 * +-------------------------------------------------+       BODY
 * | serialized bitmap 3                             |
 * +-------------------------------------------------+
 * | ...                                             |
 * +-------------------------------------------------+-----------------
 *
 * value x:                       var bytes for any data type (as bitmap identifier)
 * offset:                        4 bytes int (when it is negative, it represents that there is only one value
 *                                  and its position is the inverse of the negative value)
 * </pre>
 */
public class BitmapFileIndexMeta {

    protected final DataType dataType;
    protected final Options options;
    protected int rowCount;
    protected int nonNullBitmapNumber;
    protected boolean hasNullValue;
    protected int nullValueOffset;
    protected LinkedHashMap<Object, Integer> bitmapOffsets;
    protected Map<Object, Integer> bitmapLengths;
    protected long bodyStart;

    public BitmapFileIndexMeta(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    public BitmapFileIndexMeta(
            DataType dataType,
            Options options,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            LinkedHashMap<Object, Integer> bitmapOffsets) {
        this(dataType, options);
        this.rowCount = rowCount;
        this.nonNullBitmapNumber = nonNullBitmapNumber;
        this.hasNullValue = hasNullValue;
        this.nullValueOffset = nullValueOffset;
        this.bitmapOffsets = bitmapOffsets;
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getBodyStart() {
        return bodyStart;
    }

    /**
     * Find entry for bitmap.
     *
     * @param bitmapId the bitmap identifier to be searched.
     * @return an {@link Entry} which contains offset and length of bitmap if it is contained in the
     *     index meta; otherwise `null`.
     */
    public Entry findEntry(Object bitmapId) {
        int length = bitmapLengths == null ? -1 : bitmapLengths.getOrDefault(bitmapId, -1);
        if (bitmapId == null) {
            if (hasNullValue) {
                return new Entry(null, nullValueOffset, length);
            }
        } else {
            if (bitmapOffsets.containsKey(bitmapId)) {
                return new Entry(bitmapId, bitmapOffsets.get(bitmapId), length);
            }
        }
        return null;
    }

    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter = getValueWriter(out);

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

    public void deserialize(SeekableInputStream seekableInputStream) throws Exception {
        bodyStart = seekableInputStream.getPos();
        InputStream inputStream = new BufferedInputStream(seekableInputStream);
        bitmapLengths = new HashMap<>();
        DataInput in = new DataInputStream(inputStream);
        ThrowableSupplier valueReader = getValueReader(in);
        Function<Object, Integer> measure = getSerializeSizeMeasure();

        rowCount = in.readInt();
        bodyStart += Integer.BYTES;

        nonNullBitmapNumber = in.readInt();
        bodyStart += Integer.BYTES;

        hasNullValue = in.readBoolean();
        bodyStart++;

        if (hasNullValue) {
            nullValueOffset = in.readInt();
            bodyStart += Integer.BYTES;
        }

        bitmapOffsets = new LinkedHashMap<>();
        Object lastValue = null;
        int lastOffset = nullValueOffset;
        for (int i = 0; i < nonNullBitmapNumber; i++) {
            Object value = valueReader.get();
            int offset = in.readInt();
            bitmapOffsets.put(value, offset);
            bodyStart += measure.apply(value) + Integer.BYTES;
            if (offset >= 0) {
                if (lastOffset >= 0) {
                    int length = offset - lastOffset;
                    bitmapLengths.put(lastValue, length);
                }
                lastValue = value;
                lastOffset = offset;
            }
        }
    }

    protected Function<Object, Integer> getSerializeSizeMeasure() {
        return dataType.accept(
                new BitmapTypeVisitor<Function<Object, Integer>>() {
                    @Override
                    public Function<Object, Integer> visitBinaryString() {
                        return o -> Integer.BYTES + ((BinaryString) o).getSizeInBytes();
                    }

                    @Override
                    public Function<Object, Integer> visitByte() {
                        return o -> Byte.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitShort() {
                        return o -> Short.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitInt() {
                        return o -> Integer.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitLong() {
                        return o -> Long.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitFloat() {
                        return o -> Float.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitDouble() {
                        return o -> Double.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitBoolean() {
                        return o -> 1;
                    }
                });
    }

    protected ThrowableConsumer getValueWriter(DataOutput out) {
        return dataType.accept(
                new BitmapTypeVisitor<ThrowableConsumer>() {
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

                    @Override
                    public ThrowableConsumer visitBoolean() {
                        return o -> out.writeBoolean((Boolean) o);
                    }
                });
    }

    protected ThrowableSupplier getValueReader(DataInput in) {
        return dataType.accept(
                new BitmapTypeVisitor<ThrowableSupplier>() {
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

                    @Override
                    public ThrowableSupplier visitBoolean() {
                        return in::readBoolean;
                    }
                });
    }

    /** functional interface. */
    public interface ThrowableConsumer {
        void accept(Object o) throws Exception;
    }

    /** functional interface. */
    public interface ThrowableSupplier {
        Object get() throws Exception;
    }

    /** Bitmap entry. */
    public static class Entry {

        Object key;
        public int offset;
        public int length;

        public Entry(Object key, int offset, int length) {
            this.key = key;
            this.offset = offset;
            this.length = length;
        }
    }
}
