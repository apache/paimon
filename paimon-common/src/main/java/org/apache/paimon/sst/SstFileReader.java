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

package org.apache.paimon.sst;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An SST File Reader which only serves point queries now.
 *
 * <p>Note that this class is NOT thread-safe.
 */
public class SstFileReader implements Closeable {

    private final Comparator<MemorySlice> comparator;
    private final Path filePath;
    private final BlockCache blockCache;
    private final Footer footer;
    private final IndexIterator indexBlockIterator;
    @Nullable private final BlockDecompressor decompressor;
    @Nullable private final FileBasedBloomFilter bloomFilter;

    public SstFileReader(
            Comparator<MemorySlice> comparator,
            long fileSize,
            Path filePath,
            SeekableInputStream input,
            CacheManager cacheManager)
            throws IOException {
        this.comparator = comparator;
        this.filePath = filePath;
        this.blockCache = new BlockCache(filePath, input, cacheManager);
        MemorySegment footerData =
                blockCache.getBlock(
                        fileSize - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH, b -> b, true);
        this.footer = Footer.readFooter(MemorySlice.wrap(footerData).toInput());

        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(footer.getCompressionType());
        if (compressionFactory == null) {
            decompressor = null;
        } else {
            decompressor = compressionFactory.getDecompressor();
        }

        BlockReader rootIndexReader = readBlock(footer.getIndexBlockHandle(), true);
        this.indexBlockIterator =
                new IndexIterator(
                        rootIndexReader,
                        handle -> this.readBlock(handle, true),
                        footer.getIndexLevel());
        BloomFilterHandle handle = footer.getBloomFilterHandle();
        if (handle == null) {
            this.bloomFilter = null;
        } else {
            this.bloomFilter =
                    new FileBasedBloomFilter(
                            input,
                            filePath,
                            cacheManager,
                            handle.expectedEntries(),
                            handle.offset(),
                            handle.size());
        }
    }

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, null if not found.
     */
    @Nullable
    public byte[] lookup(byte[] key) throws IOException {
        if (bloomFilter != null && !bloomFilter.testHash(MurmurHashUtils.hashBytes(key))) {
            return null;
        }

        MemorySlice keySlice = MemorySlice.wrap(key);
        // seek the index to the block containing the key
        indexBlockIterator.seekTo(keySlice);

        // if indexIterator does not have a next, it means the key does not exist in this iterator
        if (indexBlockIterator.hasNext()) {
            // seek the current iterator to the key
            BlockIterator current = getNextBlock();
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }

    /** Read the file info, including some statistics and user-added k-v pairs. */
    public FileInfo readFileInfo() throws IOException {
        BlockHandle fileInfoHandle = footer.getFileInfoHandle();
        BlockReader blockReader = readBlock(fileInfoHandle, false);
        return FileInfo.readFileInfo(blockReader);
    }

    private BlockIterator getNextBlock() {
        // index block handle, point to the key, value position.
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock =
                readBlock(BlockHandle.readBlockHandle(blockHandle.toInput()), false);
        return dataBlock.iterator();
    }

    /**
     * @param blockHandle The block handle.
     * @param index Whether read the block as an index.
     * @return The reader of the target block.
     */
    private BlockReader readBlock(BlockHandle blockHandle, boolean index) {
        // read block trailer
        MemorySegment trailerData =
                blockCache.getBlock(
                        blockHandle.offset() + blockHandle.size(),
                        BlockTrailer.ENCODED_LENGTH,
                        b -> b,
                        true);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(MemorySlice.wrap(trailerData).toInput());

        MemorySegment unCompressedBlock =
                blockCache.getBlock(
                        blockHandle.offset(),
                        blockHandle.size(),
                        bytes -> decompressBlock(bytes, blockTrailer),
                        index);
        return new BlockReader(
                MemorySlice.wrap(unCompressedBlock), comparator, blockTrailer.getBlockType());
    }

    private byte[] decompressBlock(byte[] compressedBytes, BlockTrailer blockTrailer) {
        MemorySegment compressed = MemorySegment.wrap(compressedBytes);
        int crc32cCode =
                crc32c(compressed, blockTrailer.getBlockType(), blockTrailer.isCompressed());
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d) for file(%s)",
                        blockTrailer.getCrc32c(), crc32cCode, filePath));

        // decompress data
        if (decompressor == null || !blockTrailer.isCompressed()) {
            return compressedBytes;
        } else {
            MemorySliceInput compressedInput = MemorySlice.wrap(compressed).toInput();
            byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
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
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        blockCache.close();
        // do not need to close input, since it will be closed by outer classes
    }
}
