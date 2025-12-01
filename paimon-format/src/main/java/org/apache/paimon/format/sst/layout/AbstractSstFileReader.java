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

package org.apache.paimon.format.sst.layout;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.sst.compression.BlockCompressionFactory;
import org.apache.paimon.format.sst.compression.BlockDecompressor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

import static org.apache.paimon.format.sst.layout.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An SST FileReader which serves point queries and range queries. The implementation directly
 * processes bytes.
 *
 * <p>Note that this class is NOT thread-safe.
 */
public abstract class AbstractSstFileReader implements Closeable {

    private final long fileSize;
    private final FileInfo fileInfo;
    private final SeekableInputStream input;
    private final BlockCompressionFactory compressionFactory;

    protected final Comparator<MemorySlice> comparator;
    protected final Path filePath;
    protected final Footer footer;
    protected final MemorySlice firstKey;
    protected final BlockIterator indexBlockIterator;
    @Nullable protected final BloomFilter bloomFilter;
    /** Block cache can be useful in lookup or sequential scan with pre-fetch. */
    @Nullable protected final BlockCache blockCache;

    public AbstractSstFileReader(
            SeekableInputStream input,
            Comparator<MemorySlice> comparator,
            long fileSize,
            Path filePath,
            @Nullable BlockCache blockCache)
            throws IOException {
        this.comparator = comparator;
        this.fileSize = fileSize;
        this.filePath = filePath;
        this.input = input;
        this.blockCache = blockCache;

        Footer footer =
                Footer.readFooter(
                        readSlice(fileSize - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH, true)
                                .toInput());
        this.footer = footer;
        this.compressionFactory = BlockCompressionFactory.create(footer.getCompressionType());
        this.indexBlockIterator = readBlock(footer.getIndexBlockHandle()).iterator();
        this.bloomFilter = readBloomFilter(footer.getBloomFilterHandle());
        BlockHandle fileInfoHandle = footer.getFileInfoHandle();
        this.fileInfo =
                FileInfo.readFileInfo(
                        readSlice(fileInfoHandle.offset(), fileInfoHandle.size(), true).toInput());
        this.firstKey =
                this.fileInfo.getFirstKey() == null
                        ? null
                        : MemorySlice.wrap(this.fileInfo.getFirstKey());
    }

    private BloomFilter readBloomFilter(BloomFilterHandle bloomFilterHandle) throws IOException {
        // todo: replace with `FileBasedBloomFilter` to refresh cache on visit
        BloomFilter bloomFilter = null;
        if (bloomFilterHandle != null) {
            MemorySegment memorySegment =
                    readSlice(bloomFilterHandle.offset(), bloomFilterHandle.size(), true).segment();
            bloomFilter =
                    new BloomFilter(bloomFilterHandle.expectedEntries(), memorySegment.size());
            bloomFilter.setMemorySegment(memorySegment, 0);
        }
        return bloomFilter;
    }

    protected BlockIterator getNextBlock(BlockIterator indexBlockIterator) throws IOException {
        IndexEntryHandle entryHandle =
                IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
        BlockReader dataBlock =
                readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()));
        return dataBlock.iterator();
    }

    private MemorySlice readSlice(long offset, int size, boolean isIndex) throws IOException {
        return readSlice(offset, size, Function.identity(), isIndex);
    }

    private MemorySlice readSlice(
            long offset, int size, Function<byte[], byte[]> decompressFunc, boolean isIndex)
            throws IOException {
        if (blockCache != null) {
            return MemorySlice.wrap(blockCache.getBlock(offset, size, decompressFunc, isIndex));
        }
        input.seek(offset);
        byte[] sliceBytes = new byte[size];
        IOUtils.readFully(input, sliceBytes);
        return MemorySlice.wrap(decompressFunc.apply(sliceBytes));
    }

    protected BlockReader readBlock(BlockHandle blockHandle) throws IOException {
        // todo: reuse dataBlock in scan
        // 1. read trailer
        MemorySlice trailerSlice =
                readSlice(
                        blockHandle.offset() + blockHandle.size(),
                        BlockTrailer.ENCODED_LENGTH,
                        true);
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(trailerSlice.toInput());
        // 2. read block
        MemorySlice blockSlice =
                readSlice(
                        blockHandle.offset(),
                        blockHandle.size(),
                        data -> decompressBlock(data, blockTrailer),
                        false);
        return new BlockReader(blockSlice, comparator);
    }

    private byte[] decompressBlock(byte[] compressedBytes, BlockTrailer blockTrailer) {
        MemorySegment compressed = MemorySegment.wrap(compressedBytes);
        int crc32cCode = crc32c(compressed);
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d) for file(%s)",
                        blockTrailer.getCrc32c(), crc32cCode, filePath));

        if (compressionFactory == null) {
            return compressedBytes;
        } else {
            MemorySliceInput compressedInput = MemorySlice.wrap(compressed).toInput();
            byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
            BlockDecompressor decompressor = compressionFactory.getDecompressor();
            int uncompressedLength =
                    decompressor.decompress(
                            compressed.getHeapMemory(),
                            compressedInput.position(),
                            compressedInput.available(),
                            uncompressed,
                            0);
            checkArgument(uncompressedLength == uncompressed.length);
            return uncompressed;
        }
    }

    @Override
    public void close() throws IOException {
        // maybe close cache here.
    }

    @VisibleForTesting
    public BlockIterator getIndexBlockIterator() {
        return indexBlockIterator;
    }
}
