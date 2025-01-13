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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockDecompressor;
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.lookup.sort.SortLookupStoreUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link LookupStoreReader} for sort store. */
public class SortLookupStoreReader implements LookupStoreReader {

    private final Comparator<MemorySlice> comparator;
    private final String filePath;
    private final long fileSize;

    private final BlockIterator indexBlockIterator;
    @Nullable private FileBasedBloomFilter bloomFilter;
    private final BlockCache blockCache;
    private final PageFileInput fileInput;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator,
            File file,
            int blockSize,
            SortContext context,
            CacheManager cacheManager)
            throws IOException {
        this.comparator = comparator;
        this.filePath = file.getAbsolutePath();
        this.fileSize = context.fileSize();

        this.fileInput = PageFileInput.create(file, blockSize, null, fileSize, null);
        this.blockCache = new BlockCache(fileInput.file(), cacheManager);
        Footer footer = readFooter();
        this.indexBlockIterator = readBlock(footer.getIndexBlockHandle(), true).iterator();
        BloomFilterHandle handle = footer.getBloomFilterHandle();
        if (handle != null) {
            this.bloomFilter =
                    new FileBasedBloomFilter(
                            fileInput,
                            cacheManager,
                            handle.expectedEntries(),
                            handle.offset(),
                            handle.size());
        }
    }

    private Footer readFooter() throws IOException {
        MemorySegment footerData =
                blockCache.getBlock(
                        fileSize - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH, b -> b, true);
        return Footer.readFooter(MemorySlice.wrap(footerData).toInput());
    }

    @Nullable
    @Override
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
        return new BlockReader(MemorySlice.wrap(unCompressedBlock), comparator);
    }

    private byte[] decompressBlock(byte[] compressedBytes, BlockTrailer blockTrailer) {
        MemorySegment compressed = MemorySegment.wrap(compressedBytes);
        int crc32cCode = crc32c(compressed, blockTrailer.getCompressionType());
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d) for file(%s)",
                        blockTrailer.getCrc32c(), crc32cCode, filePath));

        // decompress data
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
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
        if (bloomFilter != null) {
            bloomFilter.close();
        }
        blockCache.close();
        fileInput.close();
    }
}
