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
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FileBasedBloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An SST File Reader which serves point queries and range queries. Users can call {@code
 * createIterator} to create a file iterator and then use seek and read methods to do range queries.
 *
 * <p>Note that this class is NOT thread-safe.
 */
public class SstFileReader implements Closeable {

    private final Comparator<MemorySlice> comparator;
    private final BlockCache blockCache;
    private final BlockReader indexBlock;
    @Nullable private final FileBasedBloomFilter bloomFilter;

    private boolean hasCompressedBlocks;
    private boolean loadBloomOnMiss;
    private long dataBytesWithoutBloom;

    public SstFileReader(
            Comparator<MemorySlice> comparator,
            BlockCache blockCache,
            BlockHandle indexBlockHandle,
            @Nullable FileBasedBloomFilter bloomFilter) {
        this.comparator = comparator;
        this.blockCache = blockCache;
        this.bloomFilter = bloomFilter;
        this.loadBloomOnMiss = bloomFilter != null && bloomFilter.isCacheable();
        this.indexBlock = readBlock(indexBlockHandle, true);
    }

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, null if not found.
     */
    @Nullable
    public byte[] lookup(byte[] key) throws IOException {
        int hash = 0;
        Boolean bloomMatch = null;
        if (bloomFilter != null) {
            hash = MurmurHashUtils.hashBytes(key);
            bloomMatch = bloomFilter.testHashIfPresent(hash);
            if (bloomMatch != null) {
                // Another reader may have admitted a previously rejected filter.
                loadBloomOnMiss = true;
                dataBytesWithoutBloom = 0;
                if (!bloomMatch) {
                    return null;
                }
            }
        }

        MemorySlice keySlice = MemorySlice.wrap(key);
        // seek the index to the block containing the key
        BlockIterator indexBlockIterator = indexBlock.iterator();
        indexBlockIterator.seekTo(keySlice);

        // if indexIterator does not have a next, it means the key does not exist in this iterator
        if (indexBlockIterator.hasNext()) {
            BlockHandle handle =
                    BlockHandle.readBlockHandle(indexBlockIterator.next().getValue().toInput());
            MemorySlice cachedData = null;
            boolean bloomProbed = bloomMatch != null;
            if (bloomFilter != null && bloomMatch == null) {
                // A missing filter must not cause I/O when the exact data is already resident.
                cachedData =
                        blockCache.getBlockSliceIfPresent(
                                handle.offset(), handle.getFullBlockSize(), false);
                if (cachedData == null
                        && (loadBloomOnMiss
                                || hasCompressedBlocks
                                || bloomFilter.size() < handle.getFullBlockSize())) {
                    boolean matches = bloomFilter.testHash(hash);
                    // Try to warm a filter that fits, but after rejection only reload it when
                    // its read cost or the saved decompression justifies bypassing the data.
                    loadBloomOnMiss = bloomFilter.isCached();
                    dataBytesWithoutBloom = 0;
                    bloomProbed = true;
                    if (!matches) {
                        return null;
                    }
                }
            }
            BlockReader dataBlock =
                    cachedData == null
                            ? readBlock(handle, false)
                            : createBlockReader(handle, cachedData);
            if (!bloomProbed
                    && cachedData == null
                    && bloomFilter != null
                    && bloomFilter.isCacheable()) {
                // Retry after cold data accesses cost as much as a filter read. This allows
                // recovery from transient rejection without loading Bloom on every miss.
                dataBytesWithoutBloom += handle.getFullBlockSize();
                if (dataBytesWithoutBloom >= bloomFilter.size()) {
                    loadBloomOnMiss = true;
                    dataBytesWithoutBloom = 0;
                }
            }
            // seek the current iterator to the key
            BlockIterator current = dataBlock.iterator();
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }

    public SstFileIterator createIterator() {
        return new SstFileIterator(indexBlock.iterator());
    }

    public SstFileReverseIterator createReverseIterator() {
        return new SstFileReverseIterator(indexBlock.reverseIterator());
    }

    private BlockIterator getNextBlock(BlockIterator indexBlockIterator) {
        // index block handle, point to the key, value position.
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock =
                readBlock(BlockHandle.readBlockHandle(blockHandle.toInput()), false);
        return dataBlock.iterator();
    }

    private ReverseBlockIterator getPreviousBlock(ReverseBlockIterator indexBlockIterator) {
        // index block handle, point to the key, value position.
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock =
                readBlock(BlockHandle.readBlockHandle(blockHandle.toInput()), false);
        return dataBlock.reverseIterator();
    }

    /**
     * @param blockHandle The block handle.
     * @param index Whether read the block as an index.
     * @return The reader of the target block.
     */
    private BlockReader readBlock(BlockHandle blockHandle, boolean index) {
        MemorySlice unCompressedBlock =
                blockCache.getBlockSlice(
                        blockHandle.offset(),
                        blockHandle.getFullBlockSize(),
                        this::decompressBlock,
                        index);
        return createBlockReader(blockHandle, unCompressedBlock);
    }

    private BlockReader createBlockReader(BlockHandle blockHandle, MemorySlice unCompressedBlock) {
        // The writer uses compression only when it reduces the payload size. Comparing sizes
        // also detects compressed blocks obtained from the shared cache, including the index.
        if (bloomFilter != null && unCompressedBlock.length() != blockHandle.size()) {
            hasCompressedBlocks = true;
        }
        return BlockReader.create(unCompressedBlock, comparator);
    }

    private MemorySlice decompressBlock(byte[] blockBytes) {
        MemorySlice fullBlock = MemorySlice.wrap(blockBytes);
        int blockSize = blockBytes.length - BlockTrailer.ENCODED_LENGTH;
        MemorySlice compressed = fullBlock.slice(0, blockSize);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(
                        fullBlock.slice(blockSize, BlockTrailer.ENCODED_LENGTH).toInput());
        int crc32cCode = crc32c(compressed, blockTrailer.getCompressionType());
        if (blockTrailer.getCrc32c() != crc32cCode) {
            throw new IllegalArgumentException(
                    String.format(
                            "Expected CRC32C(%d) but found CRC32C(%d)",
                            blockTrailer.getCrc32c(), crc32cCode));
        }

        // decompress data
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
        if (compressionFactory == null) {
            return compressed;
        } else {
            MemorySliceInput compressedInput = compressed.toInput();
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
            return MemorySlice.wrap(uncompressed);
        }
    }

    @Override
    public void close() throws IOException {
        // A failing bloom filter must not take the block cache down with it: both hold pages in
        // the shared cache manager, and the caller above closes the file handle after this.
        Throwable collected = null;
        if (bloomFilter != null) {
            try {
                bloomFilter.close();
            } catch (Throwable t) {
                collected = ExceptionUtils.firstOrSuppressed(t, collected);
            }
        }
        try {
            blockCache.close();
        } catch (Throwable t) {
            collected = ExceptionUtils.firstOrSuppressed(t, collected);
        }
        if (collected != null) {
            rethrowAsIOException(collected);
        }
    }

    private static void rethrowAsIOException(Throwable failure) throws IOException {
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        throw new IOException(failure);
    }

    /** An Iterator for range queries. */
    public class SstFileIterator {

        private final BlockIterator indexIterator;
        private @Nullable BlockIterator seekedDataBlock = null;

        SstFileIterator(BlockIterator indexBlockIterator) {
            this.indexIterator = indexBlockIterator;
        }

        /**
         * Seek to the position of the record whose key is exactly equal to or greater than the
         * specified key.
         */
        public void seekTo(byte[] key) {
            MemorySlice keySlice = MemorySlice.wrap(key);

            indexIterator.seekTo(keySlice);
            if (indexIterator.hasNext()) {
                seekedDataBlock = getNextBlock(indexIterator);
                // The index block entry key is the last key of the corresponding data block.
                // If there is some index entry key >= targetKey, the related data block must
                // also contain some key >= target key, which means seekedDataBlock.hasNext()
                // must be true
                seekedDataBlock.seekTo(keySlice);
                Preconditions.checkState(seekedDataBlock.hasNext());
            } else {
                seekedDataBlock = null;
            }
        }

        /**
         * Read a batch of records from this SST File and move current record position to the next
         * batch.
         *
         * @return current batch of records, null if reaching file end.
         */
        public BlockIterator readBatch() throws IOException {
            if (seekedDataBlock != null) {
                BlockIterator result = seekedDataBlock;
                seekedDataBlock = null;
                return result;
            }

            if (!indexIterator.hasNext()) {
                return null;
            }

            return getNextBlock(indexIterator);
        }
    }

    /** An iterator which reads an SST file from the largest key to the smallest key. */
    public class SstFileReverseIterator {

        private final ReverseBlockIterator indexIterator;

        SstFileReverseIterator(ReverseBlockIterator indexIterator) {
            this.indexIterator = indexIterator;
        }

        /**
         * Read a batch of records from this SST File and move current record position to the
         * previous batch.
         *
         * @return current batch of records, null if reaching file beginning.
         */
        @Nullable
        public ReverseBlockIterator readBatch() throws IOException {
            if (!indexIterator.hasNext()) {
                return null;
            }
            return getPreviousBlock(indexIterator);
        }
    }
}
