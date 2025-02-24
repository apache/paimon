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
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
 * ｜ null bitmap length (4 bytes if has null value) ｜
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
 * ｜ value 1 | offset 1 | length 1                  ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2 | length 2                  ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+
 * </pre>
 */
public class BitmapFileIndexMetaV2 extends BitmapFileIndexMeta {

    private long blockSizeLimit;

    private List<BitmapIndexBlock> indexBlocks;
    private long indexBlockStart;
    private int nullBitmapLength;

    public BitmapFileIndexMetaV2(DataType dataType, Options options) {
        super(dataType, options);
        this.nullBitmapLength = -1;
    }

    public BitmapFileIndexMetaV2(
            DataType dataType,
            Options options,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            int nullBitmapLength,
            LinkedHashMap<Object, Integer> bitmapOffsets,
            int finalOffset) {
        super(
                dataType,
                options,
                rowCount,
                nonNullBitmapNumber,
                hasNullValue,
                nullValueOffset,
                bitmapOffsets);
        this.nullBitmapLength = nullBitmapLength;
        blockSizeLimit =
                MemorySize.parse(options.getString(BitmapFileIndex.INDEX_BLOCK_SIZE, "16kb"))
                        .getBytes();
        bitmapLengths = new HashMap<>();
        Object lastValue = null;
        int lastOffset = nullValueOffset;
        for (Map.Entry<Object, Integer> entry : bitmapOffsets.entrySet()) {
            Object value = entry.getKey();
            Integer offset = entry.getValue();
            if (offset >= 0) {
                if (lastOffset >= 0) {
                    bitmapLengths.put(lastValue, offset - lastOffset);
                }
                lastValue = value;
                lastOffset = offset;
            }
        }
        bitmapLengths.put(lastValue, finalOffset - lastOffset);
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

                    @Override
                    public Comparator<Object> visitBoolean() {
                        return Comparator.comparing(o -> ((Boolean) o));
                    }
                });
    }

    @Override
    public Entry findEntry(Object bitmapId) {
        if (bitmapId == null) {
            if (hasNullValue) {
                return new Entry(null, nullValueOffset, nullBitmapLength);
            }
        } else {
            BitmapIndexBlock block = findBlock(bitmapId);
            if (block != null) {
                return block.findEntry(bitmapId);
            }
        }
        return null;
    }

    private BitmapIndexBlock findBlock(Object bitmapId) {
        Comparator<Object> comparator = getComparator(dataType);
        int idx =
                Collections.binarySearch(
                        indexBlocks, null, (b1, ignore) -> comparator.compare(b1.key, bitmapId));
        idx = idx < 0 ? -2 - idx : idx;
        return idx < 0 ? null : indexBlocks.get(idx);
    }

    @Override
    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter = getValueWriter(out);

        out.writeInt(rowCount);
        out.writeInt(nonNullBitmapNumber);
        out.writeBoolean(hasNullValue);
        if (hasNullValue) {
            out.writeInt(nullValueOffset);
            out.writeInt(nullBitmapLength);
        }

        LinkedList<BitmapIndexBlock> indexBlocks = new LinkedList<>();
        this.indexBlocks = indexBlocks;
        indexBlocks.add(new BitmapIndexBlock(0));
        Comparator<Object> comparator = getComparator(dataType);
        bitmapOffsets.entrySet().stream()
                .map(
                        it ->
                                new Entry(
                                        it.getKey(),
                                        it.getValue(),
                                        bitmapLengths == null
                                                ? -1
                                                : bitmapLengths.getOrDefault(it.getKey(), -1)))
                .sorted((e1, e2) -> comparator.compare(e1.key, e2.key))
                .forEach(
                        e -> {
                            BitmapIndexBlock last = indexBlocks.peekLast();
                            if (!last.tryAdd(e)) {
                                BitmapIndexBlock next =
                                        new BitmapIndexBlock(last.offset + last.serializedBytes);
                                indexBlocks.add(next);
                                if (!next.tryAdd(e)) {
                                    throw new RuntimeException("index fail");
                                }
                            }
                        });

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

        // bitmap index blocks
        for (BitmapIndexBlock indexBlock : indexBlocks) {
            out.writeInt(indexBlock.entryList.size());
            for (Entry e : indexBlock.entryList) {
                valueWriter.accept(e.key);
                out.writeInt(e.offset);
                out.writeInt(e.length);
            }
        }
    }

    @Override
    public void deserialize(SeekableInputStream seekableInputStream) throws Exception {

        indexBlockStart = seekableInputStream.getPos();

        InputStream inputStream = new BufferedInputStream(seekableInputStream);
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
            nullBitmapLength = in.readInt();
            indexBlockStart += 2 * Integer.BYTES;
        }

        bitmapOffsets = new LinkedHashMap<>();

        int bitmapBlockNumber = in.readInt();
        indexBlockStart += Integer.BYTES;

        indexBlocks = new ArrayList<>(bitmapBlockNumber);
        for (int i = 0; i < bitmapBlockNumber; i++) {
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
        Function<Object, Integer> keyBytesMapper;
        DataType dataType;
        SeekableInputStream seekableInputStream;
        Options options;

        void tryDeserialize() {
            if (entryList == null) {
                try {
                    seekableInputStream.seek(indexBlockStart + offset);
                    InputStream inputStream = new BufferedInputStream(seekableInputStream);
                    DataInputStream in = new DataInputStream(inputStream);
                    ThrowableSupplier valueReader = getValueReader(in);
                    int entryNum = in.readInt();
                    entryList = new ArrayList<>(entryNum);
                    for (int i = 0; i < entryNum; i++) {
                        entryList.add(new Entry(valueReader.get(), in.readInt(), in.readInt()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        Entry findEntry(Object bitmapId) {
            tryDeserialize();
            Comparator<Object> comparator = getComparator(dataType);
            int idx =
                    Collections.binarySearch(
                            entryList, null, (e1, ignore) -> comparator.compare(e1.key, bitmapId));
            if (idx >= 0) {
                return entryList.get(idx);
            }
            return null;
        }

        boolean tryAdd(Entry entry) {
            if (key == null) {
                key = entry.key;
            }
            int entryBytes = 2 * Integer.BYTES + keyBytesMapper.apply(entry.key);
            if (serializedBytes + entryBytes > blockSizeLimit) {
                return false;
            }
            serializedBytes += entryBytes;
            entryList.add(entry);
            return true;
        }

        // for build and serialize
        public BitmapIndexBlock(int offset) {
            this.offset = offset;
            this.entryList = new LinkedList<>();
            keyBytesMapper = getSerializeSizeMeasure();
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
}
