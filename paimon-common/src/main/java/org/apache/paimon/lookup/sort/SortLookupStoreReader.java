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
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

import static org.apache.paimon.lookup.sort.SortLookupStoreUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link LookupStoreReader} for sort store.
 *
 * <p>TODO add block cache support.
 *
 * <p>TODO separate index cache and block cache.
 */
public class SortLookupStoreReader implements LookupStoreReader {

    private final Comparator<MemorySlice> comparator;
    private final FileChannel fileChannel;
    private final String filePath;
    private final long fileSize;

    private final BlockIterator indexBlockIterator;
    @Nullable private final BloomFilter bloomFilter;

    public SortLookupStoreReader(
            Comparator<MemorySlice> comparator,
            File file,
            SortContext context,
            CacheManager cacheManager)
            throws IOException {
        this.comparator = comparator;
        //noinspection resource
        this.fileChannel = new FileInputStream(file).getChannel();
        this.filePath = file.getAbsolutePath();
        this.fileSize = context.fileSize();

        Footer footer = readFooter();
        this.indexBlockIterator = readBlock(footer.getIndexBlockHandle()).iterator();
        this.bloomFilter = readBloomFilter(footer.getBloomFilterHandle());
    }

    private BloomFilter readBloomFilter(@Nullable BloomFilterHandle bloomFilterHandle)
            throws IOException {
        BloomFilter bloomFilter = null;
        if (bloomFilterHandle != null) {
            MemorySegment segment = read(bloomFilterHandle.offset(), bloomFilterHandle.size());
            bloomFilter = new BloomFilter(bloomFilterHandle.expectedEntries(), segment.size());
            bloomFilter.setMemorySegment(segment, 0);
        }
        return bloomFilter;
    }

    private Footer readFooter() throws IOException {
        MemorySegment footerData = read(fileSize - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
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

    private BlockIterator getNextBlock() throws IOException {
        MemorySlice blockHandle = indexBlockIterator.next().getValue();
        BlockReader dataBlock = openBlock(blockHandle);
        return dataBlock.iterator();
    }

    private BlockReader openBlock(MemorySlice blockEntry) throws IOException {
        BlockHandle blockHandle = BlockHandle.readBlockHandle(blockEntry.toInput());
        return readBlock(blockHandle);
    }

    private MemorySegment read(long offset, int length) throws IOException {
        // TODO use cache
        // TODO cache uncompressed block
        // TODO separate index and data cache
        byte[] buffer = new byte[length];
        int read = fileChannel.read(ByteBuffer.wrap(buffer), offset);
        if (read != length) {
            throw new IOException("Could not read all the data");
        }
        return MemorySegment.wrap(buffer);
    }

    private BlockReader readBlock(BlockHandle blockHandle) throws IOException {
        // read block trailer
        MemorySegment trailerData =
                read(blockHandle.offset() + blockHandle.size(), BlockTrailer.ENCODED_LENGTH);
        BlockTrailer blockTrailer =
                BlockTrailer.readBlockTrailer(MemorySlice.wrap(trailerData).toInput());

        MemorySegment block = read(blockHandle.offset(), blockHandle.size());
        int crc32cCode = crc32c(block, blockTrailer.getCompressionType());
        checkArgument(
                blockTrailer.getCrc32c() == crc32cCode,
                String.format(
                        "Expected CRC32C(%d) but found CRC32C(%d) for file(%s)",
                        blockTrailer.getCrc32c(), crc32cCode, filePath));

        // decompress data
        MemorySlice uncompressedData;
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(blockTrailer.getCompressionType());
        if (compressionFactory == null) {
            uncompressedData = MemorySlice.wrap(block);
        } else {
            MemorySliceInput compressedInput = MemorySlice.wrap(block).toInput();
            byte[] uncompressed = new byte[compressedInput.readVarLenInt()];
            BlockDecompressor decompressor = compressionFactory.getDecompressor();
            int uncompressedLength =
                    decompressor.decompress(
                            block.getHeapMemory(),
                            compressedInput.position(),
                            compressedInput.available(),
                            uncompressed,
                            0);
            checkArgument(uncompressedLength == uncompressed.length);
            uncompressedData = MemorySlice.wrap(uncompressed);
        }

        return new BlockReader(uncompressedData, comparator);
    }

    @Override
    public void close() throws IOException {
        this.fileChannel.close();
        // TODO clear cache too
    }
}
