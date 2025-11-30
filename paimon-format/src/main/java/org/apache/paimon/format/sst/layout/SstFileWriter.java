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

import org.apache.paimon.format.sst.compression.BlockCompressionFactory;
import org.apache.paimon.format.sst.compression.BlockCompressionType;
import org.apache.paimon.format.sst.compression.BlockCompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.format.sst.layout.SstFileUtils.crc32c;
import static org.apache.paimon.memory.MemorySegmentUtils.allocateReuseBytes;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * An SST FileWriter which directly accepts keys and values as bytes. Please refer to {@link
 * org.apache.paimon.format.sst.SstFileFormat SST File Format} for more information.
 */
public class SstFileWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class.getName());

    public static final int MAGIC_NUMBER = 1481571681;
    public static final int VERSION = 1;

    private final PositionOutputStream out;
    private final int blockSize;
    private final BlockWriter dataBlockWriter;
    private final BlockWriter indexBlockWriter;
    private final BlockWriter metaBlockWriter;
    @Nullable private final BloomFilter.Builder bloomFilter;
    private final BlockCompressionType compressionType;
    @Nullable private final BlockCompressor blockCompressor;
    private final FileInfo.Builder fileInfoBuilder;

    private byte[] lastKey;
    private long position;

    private long recordCount;
    private long totalUncompressedSize;
    private long totalCompressedSize;

    public SstFileWriter(
            PositionOutputStream out,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.out = out;
        this.blockSize = blockSize;
        this.dataBlockWriter = new BlockWriter((int) (blockSize * 1.1));
        int expectedNumberOfBlocks = 1024;
        this.indexBlockWriter =
                new BlockWriter(BlockHandle.MAX_ENCODED_LENGTH * expectedNumberOfBlocks);
        this.metaBlockWriter = new BlockWriter(blockSize);
        this.bloomFilter = bloomFilter;
        if (compressionFactory == null) {
            this.compressionType = BlockCompressionType.NONE;
            this.blockCompressor = null;
        } else {
            this.compressionType = compressionFactory.getCompressionType();
            this.blockCompressor = compressionFactory.getCompressor();
        }
        this.fileInfoBuilder = new FileInfo.Builder();
    }

    public void put(byte[] key, byte[] value) throws IOException {
        dataBlockWriter.add(key, value);
        recordCount++;

        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }

        lastKey = key;

        if (dataBlockWriter.memory() > blockSize) {
            flush();
        }

        fileInfoBuilder.update(key, value);
    }

    public void putMetaData(byte[] metaKey, byte[] metaValue) throws IOException {
        metaBlockWriter.add(metaKey, metaValue);
    }

    private void flush() throws IOException {
        if (dataBlockWriter.size() == 0) {
            return;
        }

        int lastRecordPosition = (int) (recordCount - 1);
        BlockHandle blockHandle = writeBlock(dataBlockWriter);
        MemorySlice handleEncoding =
                IndexEntryHandle.write(
                        new IndexEntryHandle(
                                blockHandle.offset(), blockHandle.size(), lastRecordPosition));
        indexBlockWriter.add(lastKey, handleEncoding.copyBytes());
    }

    private BlockHandle writeBlock(BlockWriter blockWriter) throws IOException {
        // close the block
        MemorySlice block = blockWriter.finish();

        totalUncompressedSize += block.length();

        // attempt to compress the block
        if (blockCompressor != null) {
            int maxCompressedSize = blockCompressor.getMaxCompressedSize(block.length());
            byte[] compressed = allocateReuseBytes(maxCompressedSize + 5);
            int offset = encodeInt(compressed, 0, block.length());
            int compressedSize =
                    offset
                            + blockCompressor.compress(
                                    block.getHeapMemory(),
                                    block.offset(),
                                    block.length(),
                                    compressed,
                                    offset);

            block = new MemorySlice(MemorySegment.wrap(compressed), 0, compressedSize);
        }

        totalCompressedSize += block.length();

        // create block trailer
        BlockTrailer blockTrailer = new BlockTrailer(crc32c(block));
        MemorySlice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, block.length());

        // write data
        writeSlice(block);

        // write trailer: 4 bytes
        writeSlice(trailer);

        // clean up state
        blockWriter.reset();

        return blockHandle;
    }

    @Override
    public void close() throws IOException {
        // flush current data block
        flush();

        LOG.info("Number of record: {}", recordCount);

        // 1. write meta block if present
        @Nullable BlockHandle metaBlockHandle = null;
        if (metaBlockWriter.size() > 0) {
            metaBlockHandle = writeBlock(metaBlockWriter);
        }

        // 2. write index block
        BlockHandle indexBlockHandle = writeBlock(indexBlockWriter);

        // 3. write bloom filter
        @Nullable BloomFilterHandle bloomFilterHandle = null;
        if (bloomFilter != null) {
            MemorySegment buffer = bloomFilter.getBuffer();
            bloomFilterHandle =
                    new BloomFilterHandle(position, buffer.size(), bloomFilter.expectedEntries());
            writeSlice(MemorySlice.wrap(buffer));
            LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
        }

        // 4. write file info block
        FileInfo fileInfo = fileInfoBuilder.build();
        MemorySlice fileInfoEncoding = FileInfo.writeFileInfo(fileInfo);
        BlockHandle fileInfoHandle = new BlockHandle(position, fileInfoEncoding.length());
        writeSlice(fileInfoEncoding);

        // 5. write footer
        Footer footer =
                new Footer(
                        VERSION,
                        totalUncompressedSize,
                        (int) recordCount,
                        compressionType,
                        indexBlockHandle,
                        fileInfoHandle,
                        bloomFilterHandle,
                        metaBlockHandle);
        MemorySlice footerEncoding = Footer.writeFooter(footer);
        writeSlice(footerEncoding);

        LOG.info("totalUncompressedSize: {}", MemorySize.ofBytes(totalUncompressedSize));
        LOG.info("totalCompressedSize: {}", MemorySize.ofBytes(totalCompressedSize));
    }

    private void writeSlice(MemorySlice slice) throws IOException {
        out.write(slice.getHeapMemory(), slice.offset(), slice.length());
        position += slice.length();
    }

    public long getTotalCompressedSize() {
        return totalUncompressedSize;
    }
}
