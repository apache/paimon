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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * When the bitmap-indexed column cardinality is high, using the first version of the bitmap index
 * format will take a lot of time to read the entire dictionary. But in fact we don't need a full
 * dictionary when dealing with a small number of predicates, the performance of predicate hits on
 * the bitmap can be improved by creating a secondary index on the dictionary.
 *
 * <pre>
 * Bitmap file index format (V2)
 * +-------------------------------------------------+-----------------
 * ｜ version (1 byte) = 2                           ｜
 * +-------------------------------------------------+
 * ｜ row count (4 bytes int)                        ｜
 * +-------------------------------------------------+
 * ｜ non-null value bitmap number (4 bytes int)     ｜
 * +-------------------------------------------------+
 * ｜ has null value (1 byte)                        ｜
 * +-------------------------------------------------+
 * ｜ null value offset (4 bytes if has null value)  ｜       HEAD
 * +-------------------------------------------------+
 * ｜ bitmap index block number (4 bytes int)        ｜
 * +-------------------------------------------------+
 * ｜ value 1 | offset 1                             ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2                             ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+
 * ｜ bitmap blocks offset (4 bytes int)             ｜
 * +-------------------------------------------------+-----------------
 * ｜ bitmap index block 1                           ｜
 * +-------------------------------------------------+
 * ｜ bitmap index block 2                           ｜  INDEX BLOCKS
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 * ｜ serialized bitmap 1                            ｜
 * +-------------------------------------------------+
 * ｜ serialized bitmap 2                            ｜
 * +-------------------------------------------------+  BITMAP BLOCKS
 * ｜ serialized bitmap 3                            ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 *
 * index block format:
 * +-------------------------------------------------+
 * ｜ entry number (4 bytes int)                     ｜
 * +-------------------------------------------------+
 * ｜ value 1 | offset 1                             ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2                             ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+
 * </pre>
 */
public class BitmapFileIndexMetaV2 extends BitmapFileIndexMeta {

    private int blockSizeLimit = 32 * 1024;

    private LinkedList<BitmapIndexBlock> indexBlocks;
    private long indexBlockStart;

    public BitmapFileIndexMetaV2(DataType dataType, Options options) {
        super(dataType, options);
    }

    public BitmapFileIndexMetaV2(
            DataType dataType,
            Options options,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            LinkedHashMap<Object, Integer> bitmapOffsets) {
        super(
                dataType,
                options,
                rowCount,
                nonNullBitmapNumber,
                hasNullValue,
                nullValueOffset,
                bitmapOffsets);
        blockSizeLimit = options.getInteger(BitmapFileIndex.INDEX_BLOCK_SIZE, 16 * 1024);
    }

    public static Comparator<Object> getComparator(DataType dataType) {
        return dataType.accept(
                new DataTypeVisitorAdapter<Comparator<Object>>() {
                    @Override
                    public Comparator<Object> visitBinaryString() {
                        return Comparator.comparing(o -> ((BinaryString) o));
                    }

                    @Override
                    public Comparator<Object> visitByte() {
                        return Comparator.comparing(o -> ((Byte) o));
                    }

                    @Override
                    public Comparator<Object> visitShort() {
                        return Comparator.comparing(o -> ((Short) o));
                    }

                    @Override
                    public Comparator<Object> visitInt() {
                        return Comparator.comparing(o -> ((Integer) o));
                    }

                    @Override
                    public Comparator<Object> visitLong() {
                        return Comparator.comparing(o -> ((Long) o));
                    }

                    @Override
                    public Comparator<Object> visitFloat() {
                        return Comparator.comparing(o -> ((Float) o));
                    }

                    @Override
                    public Comparator<Object> visitDouble() {
                        return Comparator.comparing(o -> ((Double) o));
                    }
                });
    }

    @Override
    public boolean contains(Object bitmapId) {
        if (bitmapId == null) {
            return hasNullValue;
        }
        BitmapIndexBlock partial = findPartial(bitmapId);
        return partial != null && partial.contains(bitmapId);
    }

    @Override
    public int getOffset(Object bitmapId) {
        if (bitmapId == null) {
            return nullValueOffset;
        }
        BitmapIndexBlock partial = findPartial(bitmapId);
        return partial.getOffset(bitmapId);
    }

    private BitmapIndexBlock findPartial(Object bitmapId) {
        Comparator<Object> comparator = getComparator(dataType);
        BitmapIndexBlock prev = null;
        for (BitmapIndexBlock partial : indexBlocks) {
            int cmp = comparator.compare(bitmapId, partial.key);
            if (cmp < 0) {
                return prev;
            }
            prev = partial;
        }
        return prev;
    }

    @Override
    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter = getValueWriter(out);

        out.writeInt(rowCount);
        out.writeInt(nonNullBitmapNumber);
        out.writeBoolean(hasNullValue);
        if (hasNullValue) {
            out.writeInt(nullValueOffset);
        }

        indexBlocks = new LinkedList<>();
        indexBlocks.add(new BitmapIndexBlock(dataType, 0));
        Comparator<Object> comparator = getComparator(dataType);
        bitmapOffsets.entrySet().stream()
                .map(it -> new Entry(it.getKey(), it.getValue()))
                .sorted((e1, e2) -> comparator.compare(e1.key, e2.key))
                .forEach(
                        e -> {
                            BitmapIndexBlock last = indexBlocks.peekLast();
                            if (!last.tryAdd(e)) {
                                BitmapIndexBlock next =
                                        new BitmapIndexBlock(
                                                dataType, last.offset + last.serializedBytes);
                                indexBlocks.add(next);
                                if (!next.tryAdd(e)) {
                                    throw new RuntimeException("index fail");
                                }
                            }
                        });

        // secondary dictionary size
        out.writeInt(indexBlocks.size());

        int bitmapBodyOffset = 0;
        for (BitmapIndexBlock e : indexBlocks) {
            // secondary entry
            valueWriter.accept(e.key);
            out.writeInt(e.offset);
            bitmapBodyOffset += e.serializedBytes;
        }

        // bitmap body offset
        out.writeInt(bitmapBodyOffset);

        // bitmap partial dictionaries
        for (BitmapIndexBlock partial : indexBlocks) {
            out.writeInt(partial.entryList.size());
            for (Entry e : partial.entryList) {
                valueWriter.accept(e.key);
                out.writeInt(e.offset);
            }
        }
    }

    @Override
    public void deserialize(SeekableInputStream seekableInputStream) throws Exception {

        indexBlockStart = seekableInputStream.getPos();

        InputStream inputStream = seekableInputStream;
        if (options.getBoolean(BitmapFileIndex.ENABLE_BUFFERED_INPUT, true)) {
            inputStream = new BufferedInputStream(inputStream);
        }
        DataInput in = new DataInputStream(inputStream);
        ThrowableSupplier valueReader = getValueReader(in);
        Function<Object, Integer> measure = getSerializeSizeMeasure();

        rowCount = in.readInt();
        indexBlockStart += Integer.BYTES;

        nonNullBitmapNumber = in.readInt();
        indexBlockStart += Integer.BYTES;

        hasNullValue = in.readBoolean();
        indexBlockStart++;

        if (hasNullValue) {
            nullValueOffset = in.readInt();
            indexBlockStart += Integer.BYTES;
        }

        bitmapOffsets = new LinkedHashMap<>();

        // secondary dictionary size
        int partialBitmapDictionaryNum = in.readInt();
        indexBlockStart += Integer.BYTES;

        indexBlocks = new LinkedList<>();
        for (int i = 0; i < partialBitmapDictionaryNum; i++) {
            Object key = valueReader.get();
            int offset = in.readInt();
            indexBlocks.add(
                    new BitmapIndexBlock(dataType, options, key, offset, seekableInputStream));
            indexBlockStart += measure.apply(key) + Integer.BYTES;
        }

        // bitmap body offset
        int bitmapBodyOffset = in.readInt();
        indexBlockStart += Integer.BYTES;

        bodyStart = indexBlockStart + bitmapBodyOffset;
    }

    /** Split of all bitmap entries. */
    class BitmapIndexBlock {

        Object key;
        int offset;
        int serializedBytes = Integer.BYTES;
        List<Entry> entryList;
        boolean isConstantKeyBytes;
        int constantKeyBytes;
        Function<Object, Integer> keyBytesMapper;
        DataType dataType;
        SeekableInputStream seekableInputStream;
        Options options;

        void tryDeserialize() {
            if (entryList == null) {
                entryList = new LinkedList<>();
                try {
                    seekableInputStream.seek(indexBlockStart + offset);
                    InputStream inputStream = seekableInputStream;
                    if (options.getBoolean(BitmapFileIndex.ENABLE_BUFFERED_INPUT, true)) {
                        inputStream = new BufferedInputStream(inputStream);
                    }
                    DataInputStream in = new DataInputStream(inputStream);
                    ThrowableSupplier valueReader = getValueReader(in);
                    int entryNum = in.readInt();
                    for (int i = 0; i < entryNum; i++) {
                        entryList.add(new Entry(valueReader.get(), in.readInt()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        boolean contains(Object bitmapId) {
            if (key.equals(bitmapId)) {
                return true;
            }
            tryDeserialize();
            for (Entry entry : entryList) {
                if (entry.key.equals(bitmapId)) {
                    return true;
                }
            }
            return false;
        }

        int getOffset(Object bitmapId) {
            tryDeserialize();
            for (Entry entry : entryList) {
                if (entry.key.equals(bitmapId)) {
                    return entry.offset;
                }
            }
            throw new RuntimeException("not exist");
        }

        boolean tryAdd(Entry entry) {
            if (key == null) {
                key = entry.key;
            }
            int entryBytes =
                    Integer.BYTES
                            + (isConstantKeyBytes
                                    ? constantKeyBytes
                                    : keyBytesMapper.apply(entry.key));
            if (serializedBytes + entryBytes > blockSizeLimit) {
                return false;
            }
            serializedBytes += entryBytes;
            entryList.add(entry);
            return true;
        }

        // for build and serialize
        public BitmapIndexBlock(DataType dataType, int offset) {
            this.offset = offset;
            this.entryList = new LinkedList<>();
            keyBytesMapper =
                    dataType.accept(
                            new DataTypeVisitorAdapter<Function<Object, Integer>>() {

                                @Override
                                public Function<Object, Integer> visitBinaryString() {
                                    return o -> Integer.BYTES + ((BinaryString) o).getSizeInBytes();
                                }

                                @Override
                                public Function<Object, Integer> visitByte() {
                                    return visitFixLength(Byte.BYTES);
                                }

                                @Override
                                public Function<Object, Integer> visitShort() {
                                    return visitFixLength(Short.BYTES);
                                }

                                @Override
                                public Function<Object, Integer> visitInt() {
                                    return visitFixLength(Integer.BYTES);
                                }

                                @Override
                                public Function<Object, Integer> visitLong() {
                                    return visitFixLength(Long.BYTES);
                                }

                                @Override
                                public Function<Object, Integer> visitFloat() {
                                    return visitFixLength(Float.BYTES);
                                }

                                @Override
                                public Function<Object, Integer> visitDouble() {
                                    return visitFixLength(Double.BYTES);
                                }

                                private Function<Object, Integer> visitFixLength(int bytes) {
                                    isConstantKeyBytes = true;
                                    constantKeyBytes = bytes;
                                    return null;
                                }
                            });
        }

        // for deserialize
        public BitmapIndexBlock(
                DataType dataType,
                Options options,
                Object key,
                int offset,
                SeekableInputStream seekableInputStream) {
            this.dataType = dataType;
            this.options = options;
            this.key = key;
            this.offset = offset;
            this.seekableInputStream = seekableInputStream;
        }
    }

    /** Bitmap entry. */
    static class Entry {

        Object key;
        int offset;

        public Entry(Object key, int offset) {
            this.key = key;
            this.offset = offset;
        }
    }
}
