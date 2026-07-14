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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.sst.BlockTrailer;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.VarLengthIntUtils;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.sst.SstFileUtils.crc32c;

/** Shared file format helpers for bitmap global index. */
class BitmapGlobalIndexFormat {

    private static final int MAGIC = 0x42474958;
    private static final int SERIALIZED_KEY_ORDER_VERSION = 1;
    private static final int LOGICAL_KEY_ORDER_VERSION = 2;
    private static final int FOOTER_LENGTH = 48;

    private BitmapGlobalIndexFormat() {}

    static void write(
            PositionOutputStream outputStream,
            RoaringNavigableMap64 nullRows,
            RoaringNavigableMap64 nonNullRows,
            Map<SerializedKey, RoaringNavigableMap64> bitmaps,
            int dictionaryBlockSize,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        Preconditions.checkArgument(
                dictionaryBlockSize > 0, "Bitmap dictionary block size must be greater than 0.");

        DataOutputStream out = new DataOutputStream(outputStream);
        BlockInfo nullRowsBlock = writeBitmapBlock(outputStream, out, nullRows);
        BlockInfo nonNullRowsBlock = writeBitmapBlock(outputStream, out, nonNullRows);
        DictionaryBlocks dictionaryBlocks =
                writeDictionaryAndBitmapBlocks(
                        outputStream, out, bitmaps, dictionaryBlockSize, compressionFactory);
        BlockInfo indexBlock =
                writeIndexBlock(outputStream, out, dictionaryBlocks.blocks, compressionFactory);

        writeFooter(
                out,
                nullRowsBlock,
                nonNullRowsBlock,
                indexBlock,
                dictionaryBlocks.valueCount,
                SERIALIZED_KEY_ORDER_VERSION);
    }

    private static DictionaryBlocks writeDictionaryAndBitmapBlocks(
            PositionOutputStream outputStream,
            DataOutputStream out,
            Map<SerializedKey, RoaringNavigableMap64> bitmaps,
            int dictionaryBlockSize,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        List<Map.Entry<SerializedKey, RoaringNavigableMap64>> entries =
                new ArrayList<>(bitmaps.entrySet());
        Collections.sort(entries, (o1, o2) -> o1.getKey().compareTo(o2.getKey()));

        List<DictionaryBlockMeta> dictionaryBlockMetas = new ArrayList<>();
        DictionaryBlockBuilder current = new DictionaryBlockBuilder();
        int valueCount = 0;
        for (Map.Entry<SerializedKey, RoaringNavigableMap64> entry : entries) {
            BlockInfo bitmapBlock = writeBitmapBlock(outputStream, out, entry.getValue());
            DictionaryEntry dictionaryEntry = new DictionaryEntry(entry.getKey(), bitmapBlock);
            if (current.hasEntries()
                    && current.estimatedSizeAfter(dictionaryEntry) > dictionaryBlockSize) {
                dictionaryBlockMetas.add(
                        writeDictionaryBlock(outputStream, out, current, compressionFactory));
                current = new DictionaryBlockBuilder();
            }
            current.add(dictionaryEntry);
            valueCount++;
        }
        if (current.hasEntries()) {
            dictionaryBlockMetas.add(
                    writeDictionaryBlock(outputStream, out, current, compressionFactory));
        }
        return new DictionaryBlocks(dictionaryBlockMetas, valueCount);
    }

    static class StreamingWriter {

        private final PositionOutputStream outputStream;
        private final DataOutputStream out;
        private final int dictionaryBlockSize;
        @Nullable private final BlockCompressionFactory compressionFactory;
        private final List<DictionaryBlockMeta> dictionaryBlockMetas = new ArrayList<>();

        private DictionaryBlockBuilder currentDictionaryBlock = new DictionaryBlockBuilder();
        private int valueCount;

        StreamingWriter(
                PositionOutputStream outputStream,
                int dictionaryBlockSize,
                @Nullable BlockCompressionFactory compressionFactory) {
            Preconditions.checkArgument(
                    dictionaryBlockSize > 0,
                    "Bitmap dictionary block size must be greater than 0.");
            this.outputStream = outputStream;
            this.out = new DataOutputStream(outputStream);
            this.dictionaryBlockSize = dictionaryBlockSize;
            this.compressionFactory = compressionFactory;
        }

        void write(SerializedKey key, RoaringNavigableMap64 bitmap) throws IOException {
            BlockInfo bitmapBlock = writeBitmapBlock(outputStream, out, bitmap);
            DictionaryEntry dictionaryEntry = new DictionaryEntry(key, bitmapBlock);
            if (currentDictionaryBlock.hasEntries()
                    && currentDictionaryBlock.estimatedSizeAfter(dictionaryEntry)
                            > dictionaryBlockSize) {
                flushDictionaryBlock();
            }
            currentDictionaryBlock.add(dictionaryEntry);
            valueCount++;
        }

        void finish(RoaringNavigableMap64 nullRows, RoaringNavigableMap64 nonNullRows)
                throws IOException {
            flushDictionaryBlock();
            BlockInfo nullRowsBlock = writeBitmapBlock(outputStream, out, nullRows);
            BlockInfo nonNullRowsBlock = writeBitmapBlock(outputStream, out, nonNullRows);
            BlockInfo indexBlock =
                    writeIndexBlock(outputStream, out, dictionaryBlockMetas, compressionFactory);

            writeFooter(
                    out,
                    nullRowsBlock,
                    nonNullRowsBlock,
                    indexBlock,
                    valueCount,
                    LOGICAL_KEY_ORDER_VERSION);
        }

        private void flushDictionaryBlock() throws IOException {
            if (!currentDictionaryBlock.hasEntries()) {
                return;
            }
            dictionaryBlockMetas.add(
                    writeDictionaryBlock(
                            outputStream, out, currentDictionaryBlock, compressionFactory));
            currentDictionaryBlock = new DictionaryBlockBuilder();
        }
    }

    private static void writeFooter(
            DataOutputStream out,
            BlockInfo nullRowsBlock,
            BlockInfo nonNullRowsBlock,
            BlockInfo indexBlock,
            int valueCount,
            int version)
            throws IOException {
        out.writeLong(nullRowsBlock.offset);
        out.writeInt(nullRowsBlock.length);
        out.writeLong(nonNullRowsBlock.offset);
        out.writeInt(nonNullRowsBlock.length);
        out.writeLong(indexBlock.offset);
        out.writeInt(indexBlock.length);
        out.writeInt(valueCount);
        out.writeInt(version);
        out.writeInt(MAGIC);
        out.flush();
    }

    private static BlockInfo writeBitmapBlock(
            PositionOutputStream outputStream, DataOutputStream out, RoaringNavigableMap64 bitmap)
            throws IOException {
        byte[] bytes = bitmap.serialize();
        long offset = outputStream.getPos();
        out.write(bytes);
        return new BlockInfo(offset, bytes.length);
    }

    private static DictionaryBlockMeta writeDictionaryBlock(
            PositionOutputStream outputStream,
            DataOutputStream out,
            DictionaryBlockBuilder block,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(block.estimatedSize());
        DataOutputStream blockOut = new DataOutputStream(bytes);
        writeVarLenInt(blockOut, block.entries.size());
        for (DictionaryEntry entry : block.entries) {
            byte[] keyBytes = entry.key.bytes();
            writeVarLenInt(blockOut, keyBytes.length);
            blockOut.write(keyBytes);
            writeVarLenLong(blockOut, entry.bitmapBlock.offset);
            writeVarLenInt(blockOut, entry.bitmapBlock.length);
        }
        BlockInfo blockInfo =
                writeCompressibleBlock(outputStream, out, bytes.toByteArray(), compressionFactory);
        return new DictionaryBlockMeta(block.firstKey(), blockInfo.offset, blockInfo.length);
    }

    private static BlockInfo writeIndexBlock(
            PositionOutputStream outputStream,
            DataOutputStream out,
            List<DictionaryBlockMeta> blocks,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(estimatedIndexBlockSize(blocks));
        DataOutputStream blockOut = new DataOutputStream(bytes);
        writeVarLenInt(blockOut, blocks.size());
        for (DictionaryBlockMeta block : blocks) {
            byte[] keyBytes = block.firstKey.bytes();
            writeVarLenInt(blockOut, keyBytes.length);
            blockOut.write(keyBytes);
            writeVarLenLong(blockOut, block.offset);
            writeVarLenInt(blockOut, block.length);
        }
        return writeCompressibleBlock(outputStream, out, bytes.toByteArray(), compressionFactory);
    }

    static Footer readFooter(SeekableReader reader, long fileSize) throws IOException {
        Preconditions.checkState(
                fileSize >= FOOTER_LENGTH, "Invalid bitmap global index file size.");
        byte[] bytes = reader.read(fileSize - FOOTER_LENGTH, FOOTER_LENGTH);
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes));
        BlockInfo nullRowsBlock = new BlockInfo(input.readLong(), input.readInt());
        BlockInfo nonNullRowsBlock = new BlockInfo(input.readLong(), input.readInt());
        BlockInfo indexBlock = new BlockInfo(input.readLong(), input.readInt());
        int valueCount = input.readInt();
        int version = input.readInt();
        int magic = input.readInt();
        Preconditions.checkState(
                magic == MAGIC, "File is not a bitmap global index file (bad footer magic).");
        Preconditions.checkState(
                version == SERIALIZED_KEY_ORDER_VERSION || version == LOGICAL_KEY_ORDER_VERSION,
                "Unsupported bitmap global index file version: %s",
                version);
        Preconditions.checkState(valueCount >= 0, "Invalid bitmap value count.");
        return new Footer(nullRowsBlock, nonNullRowsBlock, indexBlock, version);
    }

    private static List<DictionaryBlockMeta> readIndexBlock(
            SeekableReader reader, BlockInfo indexBlock, int version) throws IOException {
        DataInputStream input =
                new DataInputStream(
                        new ByteArrayInputStream(readCompressibleBlock(reader, indexBlock)));
        int blockCount = readVarLenInt(input);
        Preconditions.checkState(blockCount >= 0, "Invalid bitmap dictionary block count.");
        List<DictionaryBlockMeta> blocks = new ArrayList<>(blockCount);
        for (int i = 0; i < blockCount; i++) {
            int keyLength = readVarLenInt(input);
            Preconditions.checkState(keyLength >= 0, "Invalid bitmap key length.");
            byte[] keyBytes = new byte[keyLength];
            input.readFully(keyBytes);
            long offset = readVarLenLong(input);
            int length = readVarLenInt(input);
            blocks.add(new DictionaryBlockMeta(new SerializedKey(keyBytes), offset, length));
        }
        if (version == SERIALIZED_KEY_ORDER_VERSION) {
            Collections.sort(blocks, (o1, o2) -> o1.firstKey.compareTo(o2.firstKey));
        }
        return blocks;
    }

    static DictionaryBlock readDictionaryBlock(SeekableReader reader, DictionaryBlockMeta block)
            throws IOException {
        DataInputStream input =
                new DataInputStream(new ByteArrayInputStream(readCompressibleBlock(reader, block)));
        int entryCount = readVarLenInt(input);
        Preconditions.checkState(entryCount >= 0, "Invalid bitmap dictionary entry count.");
        List<DictionaryEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            int keyLength = readVarLenInt(input);
            Preconditions.checkState(keyLength >= 0, "Invalid bitmap key length.");
            byte[] keyBytes = new byte[keyLength];
            input.readFully(keyBytes);
            long bitmapOffset = readVarLenLong(input);
            int bitmapLength = readVarLenInt(input);
            entries.add(
                    new DictionaryEntry(
                            new SerializedKey(keyBytes),
                            new BlockInfo(bitmapOffset, bitmapLength)));
        }
        return new DictionaryBlock(entries);
    }

    static RoaringNavigableMap64 readBitmap(SeekableReader reader, BlockInfo block)
            throws IOException {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        bitmap.deserialize(reader.read(block));
        return bitmap;
    }

    static RoaringNavigableMap64 readBitmapUnchecked(SeekableReader reader, BlockInfo block) {
        try {
            return readBitmap(reader, block);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read bitmap global index block.", e);
        }
    }

    static List<DictionaryBlockMeta> readIndexBlockUnchecked(
            SeekableReader reader, BlockInfo block, int version) {
        try {
            return readIndexBlock(reader, block, version);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read bitmap dictionary block index.", e);
        }
    }

    private static BlockInfo writeCompressibleBlock(
            PositionOutputStream outputStream,
            DataOutputStream out,
            byte[] uncompressed,
            @Nullable BlockCompressionFactory compressionFactory)
            throws IOException {
        BlockEncoding blockEncoding = encodeBlock(uncompressed, compressionFactory);
        byte[] blockBytes = Arrays.copyOf(blockEncoding.bytes, blockEncoding.length);
        long offset = outputStream.getPos();
        out.write(blockBytes);
        MemorySlice trailer =
                BlockTrailer.writeBlockTrailer(
                        new BlockTrailer(
                                blockEncoding.compressionType,
                                crc32c(
                                        MemorySlice.wrap(blockBytes),
                                        blockEncoding.compressionType)));
        out.write(trailer.getHeapMemory(), trailer.offset(), trailer.length());
        return new BlockInfo(offset, blockBytes.length);
    }

    private static BlockEncoding encodeBlock(
            byte[] uncompressed, @Nullable BlockCompressionFactory compressionFactory) {
        BlockCompressionType compressionType = BlockCompressionType.NONE;
        byte[] bytes = uncompressed;
        int length = uncompressed.length;
        if (compressionFactory != null) {
            BlockCompressor compressor = compressionFactory.getCompressor();
            int maxCompressedSize = compressor.getMaxCompressedSize(uncompressed.length);
            byte[] compressed = new byte[maxCompressedSize + VarLengthIntUtils.MAX_VAR_INT_SIZE];
            int offset = VarLengthIntUtils.encodeInt(compressed, 0, uncompressed.length);
            int compressedSize =
                    offset
                            + compressor.compress(
                                    uncompressed, 0, uncompressed.length, compressed, offset);
            if (compressedSize < uncompressed.length - (uncompressed.length / 8)) {
                bytes = compressed;
                length = compressedSize;
                compressionType = compressionFactory.getCompressionType();
            }
        }
        return new BlockEncoding(bytes, length, compressionType);
    }

    private static byte[] readCompressibleBlock(SeekableReader reader, BlockInfo block)
            throws IOException {
        Preconditions.checkState(
                block.length <= Integer.MAX_VALUE - BlockTrailer.ENCODED_LENGTH,
                "Bitmap block is too large.");
        byte[] blockAndTrailer =
                reader.read(block.offset, block.length + BlockTrailer.ENCODED_LENGTH);
        byte[] blockBytes = Arrays.copyOf(blockAndTrailer, block.length);
        byte[] trailerBytes =
                Arrays.copyOfRange(
                        blockAndTrailer, block.length, block.length + BlockTrailer.ENCODED_LENGTH);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(MemorySlice.wrap(trailerBytes).toInput());

        MemorySegment blockSegment = MemorySegment.wrap(blockBytes);
        int crc32cCode = crc32c(blockSegment, blockTrailer.getCompressionType());
        Preconditions.checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                "Expected CRC32C(%s) but found CRC32C(%s)",
                blockTrailer.getCrc32c(),
                crc32cCode);

        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
        if (compressionFactory == null) {
            return blockBytes;
        }

        MemorySliceInput compressedInput = MemorySlice.wrap(blockSegment).toInput();
        byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
        BlockDecompressor decompressor = compressionFactory.getDecompressor();
        int uncompressedLength =
                decompressor.decompress(
                        blockSegment.getHeapMemory(),
                        compressedInput.position(),
                        compressedInput.available(),
                        uncompressed,
                        0);
        Preconditions.checkArgument(uncompressedLength == uncompressed.length);
        return uncompressed;
    }

    private static void writeVarLenInt(DataOutputStream out, int value) throws IOException {
        VarLengthIntUtils.encodeInt((java.io.DataOutput) out, value);
    }

    private static int readVarLenInt(DataInputStream input) throws IOException {
        return VarLengthIntUtils.decodeInt((java.io.DataInput) input);
    }

    private static void writeVarLenLong(DataOutputStream out, long value) throws IOException {
        VarLengthIntUtils.encodeLong(out, value);
    }

    private static long readVarLenLong(DataInputStream input) throws IOException {
        return VarLengthIntUtils.decodeLong(input);
    }

    private static int estimatedVarLenIntSize(int value) {
        Preconditions.checkArgument(value >= 0, "Invalid negative var length int: %s", value);
        int size = 1;
        while ((value & ~0x7F) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    private static int estimatedVarLenLongSize(long value) {
        Preconditions.checkArgument(value >= 0, "Invalid negative var length long: %s", value);
        int size = 1;
        while ((value & ~0x7FL) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    private static int estimatedIndexBlockSize(List<DictionaryBlockMeta> blocks) {
        int size = estimatedVarLenIntSize(blocks.size());
        for (DictionaryBlockMeta block : blocks) {
            size +=
                    estimatedVarLenIntSize(block.firstKey.bytes().length)
                            + block.firstKey.bytes().length
                            + estimatedVarLenLongSize(block.offset)
                            + estimatedVarLenIntSize(block.length);
        }
        return size;
    }

    static class SerializedKey implements Comparable<SerializedKey> {

        private final byte[] bytes;

        SerializedKey(byte[] bytes) {
            this.bytes = bytes;
        }

        byte[] bytes() {
            return bytes;
        }

        static SerializedKey fromObject(KeySerializer serializer, Object key) {
            return new SerializedKey(serializer.serialize(key));
        }

        @Override
        public int compareTo(SerializedKey other) {
            int length = Math.min(bytes.length, other.bytes.length);
            for (int i = 0; i < length; i++) {
                int diff = (bytes[i] & 0xFF) - (other.bytes[i] & 0xFF);
                if (diff != 0) {
                    return diff;
                }
            }
            return bytes.length - other.bytes.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SerializedKey)) {
                return false;
            }
            SerializedKey that = (SerializedKey) o;
            return Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }

    interface SeekableReader {

        byte[] read(long offset, int length) throws IOException;

        default byte[] read(BlockInfo block) throws IOException {
            return read(block.offset, block.length);
        }
    }

    static class BlockInfo {

        final long offset;
        final int length;

        BlockInfo(long offset, int length) {
            Preconditions.checkState(offset >= 0, "Invalid bitmap block offset.");
            Preconditions.checkState(length >= 0, "Invalid bitmap block length.");
            this.offset = offset;
            this.length = length;
        }
    }

    static class DictionaryBlockMeta extends BlockInfo {

        final SerializedKey firstKey;

        DictionaryBlockMeta(SerializedKey firstKey, long offset, int length) {
            super(offset, length);
            this.firstKey = firstKey;
        }
    }

    static class DictionaryBlock {

        final List<DictionaryEntry> entries;

        DictionaryBlock(List<DictionaryEntry> entries) {
            this.entries = entries;
        }
    }

    static class DictionaryEntry {

        final SerializedKey key;
        final BlockInfo bitmapBlock;

        DictionaryEntry(SerializedKey key, BlockInfo bitmapBlock) {
            this.key = key;
            this.bitmapBlock = bitmapBlock;
        }

        int estimatedSize() {
            return estimatedVarLenIntSize(key.bytes().length)
                    + key.bytes().length
                    + estimatedVarLenLongSize(bitmapBlock.offset)
                    + estimatedVarLenIntSize(bitmapBlock.length);
        }
    }

    private static class DictionaryBlockBuilder {

        private final List<DictionaryEntry> entries = new ArrayList<>();
        private int entriesSize;

        boolean hasEntries() {
            return !entries.isEmpty();
        }

        int estimatedSize() {
            return estimatedVarLenIntSize(entries.size()) + entriesSize;
        }

        int estimatedSizeAfter(DictionaryEntry entry) {
            return estimatedVarLenIntSize(entries.size() + 1) + entriesSize + entry.estimatedSize();
        }

        void add(DictionaryEntry entry) {
            entries.add(entry);
            entriesSize += entry.estimatedSize();
        }

        SerializedKey firstKey() {
            return entries.get(0).key;
        }
    }

    private static class DictionaryBlocks {

        private final List<DictionaryBlockMeta> blocks;
        private final int valueCount;

        private DictionaryBlocks(List<DictionaryBlockMeta> blocks, int valueCount) {
            this.blocks = blocks;
            this.valueCount = valueCount;
        }
    }

    private static class BlockEncoding {

        private final byte[] bytes;
        private final int length;
        private final BlockCompressionType compressionType;

        private BlockEncoding(byte[] bytes, int length, BlockCompressionType compressionType) {
            this.bytes = bytes;
            this.length = length;
            this.compressionType = compressionType;
        }
    }

    static class Footer {

        final BlockInfo nullRowsBlock;
        final BlockInfo nonNullRowsBlock;
        final BlockInfo indexBlock;
        final int version;

        Footer(
                BlockInfo nullRowsBlock,
                BlockInfo nonNullRowsBlock,
                BlockInfo indexBlock,
                int version) {
            this.nullRowsBlock = nullRowsBlock;
            this.nonNullRowsBlock = nonNullRowsBlock;
            this.indexBlock = indexBlock;
            this.version = version;
        }

        boolean logicalKeyOrder() {
            return version == LOGICAL_KEY_ORDER_VERSION;
        }
    }
}
