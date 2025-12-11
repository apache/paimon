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
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.compression.BlockCompressor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.paimon.memory.MemorySegmentUtils.allocateReuseBytes;
import static org.apache.paimon.sst.BlockHandle.writeBlockHandle;
import static org.apache.paimon.sst.SstFileUtils.crc32c;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * The writer for writing SST Files. SST Files are row-oriented and designed to serve frequent point
 * queries and range queries by key. The SST File layout is as below: (For layouts of each block
 * type, please refer to corresponding classes)
 *
 * <pre>
 *     +-----------------------------------+------+
 *     |              Footer               |      |
 *     +-----------------------------------+      |
 *     |         Root Index Block          |      +--> Loaded on open
 *     +-----------------------------------+      |
 *     |        Bloom Filter Block         |      |
 *     +-----------------------------------+------+
 *     |     Intermediate Index Block      |      |
 *     +-----------------------------------+      |
 *     |              ......               |      |
 *     +-----------------------------------+      |
 *     |     Intermediate Index Block      |      |
 *     +-----------------------------------+      |
 *     |          File Info Block          |      |
 *     +-----------------------------------+      +--> Loaded on requested
 *     |            Data Block             |      |
 *     +-----------------------------------+      |
 *     |              ......               |      |
 *     +-----------------------------------+      |
 *     |         Leaf Index Block          |      |
 *     +-----------------------------------+      |
 *     |              ......               |      |
 *     +-----------------------------------+      |
 *     |            Data Block             |      |
 *     +-----------------------------------+------+
 * </pre>
 */
public class SstFileWriter implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SstFileWriter.class.getName());

    public static final int MAGIC_NUMBER = 1481571681;
    public static final int VERSION = 1;

    private final PositionOutputStream out;
    private final int blockSize;
    private final BlockWriter dataBlockWriter;
    private final IndexWriter indexWriter;
    private final FileInfo.Builder infoBuilder;
    @Nullable private final BloomFilter.Builder bloomFilter;
    @Nullable private final BlockCompressor blockCompressor;
    private final BlockCompressionType compressionType;

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
        this(
                out,
                blockSize,
                bloomFilter,
                compressionFactory,
                (int) MemorySize.ofMebiBytes(8).getBytes(),
                16);
    }

    public SstFileWriter(
            PositionOutputStream out,
            int blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            @Nullable BlockCompressionFactory compressionFactory,
            int maxIndexBlockSize,
            int minIndexBlockEntryNum) {
        this.out = out;
        this.blockSize = blockSize;
        this.dataBlockWriter = new BlockWriter((int) (blockSize * 1.1));
        this.indexWriter =
                new IndexWriter(this::writeBlock, maxIndexBlockSize, minIndexBlockEntryNum);
        this.bloomFilter = bloomFilter;
        this.infoBuilder = new FileInfo.Builder();
        if (compressionFactory == null) {
            this.blockCompressor = null;
            this.compressionType = BlockCompressionType.NONE;
        } else {
            this.blockCompressor = compressionFactory.getCompressor();
            this.compressionType = compressionFactory.getCompressionType();
        }
    }

    /**
     * Put the serialized key and value into this SST File. The caller must guarantee that the input
     * key is monotonically incremental according to {@link SstFileReader}'s comparator. Otherwise,
     * the lookup and range query result will be undefined.
     *
     * @param key serialized key
     * @param value serialized value
     */
    public void put(byte[] key, byte[] value) throws IOException {
        dataBlockWriter.add(key, value);
        infoBuilder.update(key, value);
        if (bloomFilter != null) {
            bloomFilter.addHash(MurmurHashUtils.hashBytes(key));
        }

        lastKey = key;

        if (dataBlockWriter.memory() > blockSize) {
            flush();
        }

        recordCount++;
    }

    public void addExtraFileInfo(byte[] key, byte[] value) throws IOException {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        infoBuilder.addExtraValue(
                Arrays.copyOf(key, key.length), Arrays.copyOf(value, value.length));
    }

    private void flush() throws IOException {
        if (dataBlockWriter.size() == 0) {
            return;
        }

        BlockHandle blockHandle = writeBlock(dataBlockWriter, BlockType.DATA);
        MemorySlice handleEncoding = writeBlockHandle(blockHandle);
        indexWriter.addEntry(lastKey, handleEncoding.copyBytes());
    }

    private BlockHandle writeBlock(BlockWriter blockWriter, BlockType blockType)
            throws IOException {
        // close the block
        MemorySlice block = blockWriter.finish();

        totalUncompressedSize += block.length();

        // attempt to compress the block
        boolean dataCompressed = false;
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

            // Don't use the compressed data if compressed less than 12.5%,
            if (compressedSize < block.length() - (block.length() / 8)) {
                block = new MemorySlice(MemorySegment.wrap(compressed), 0, compressedSize);
                dataCompressed = true;
            }
        }

        totalCompressedSize += block.length();

        // create block trailer
        BlockTrailer blockTrailer =
                new BlockTrailer(
                        blockType, crc32c(block, blockType, dataCompressed), dataCompressed);
        MemorySlice trailer = BlockTrailer.writeBlockTrailer(blockTrailer);

        // create a handle to this block
        BlockHandle blockHandle = new BlockHandle(position, block.length());

        // write data
        writeSlice(block);

        // write trailer: 6 bytes
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

        // write fileInfo
        FileInfo fileInfo = infoBuilder.build();
        BlockWriter fileInfoWriter = new BlockWriter(fileInfo.memory());
        FileInfo.writeFileInfo(fileInfo, fileInfoWriter);
        BlockHandle fileInfoHandle = writeBlock(fileInfoWriter, BlockType.FILE_INFO);

        // write index block
        BlockHandle indexBlockHandle = indexWriter.finish();

        // write bloom filter
        @Nullable BloomFilterHandle bloomFilterHandle = null;
        if (bloomFilter != null) {
            MemorySegment buffer = bloomFilter.getBuffer();
            bloomFilterHandle =
                    new BloomFilterHandle(position, buffer.size(), bloomFilter.expectedEntries());
            writeSlice(MemorySlice.wrap(buffer));
            LOG.info("Bloom filter size: {} bytes", bloomFilter.getBuffer().size());
        }

        // write footer
        Footer footer =
                new Footer(
                        fileInfoHandle,
                        bloomFilterHandle,
                        indexBlockHandle,
                        indexWriter.getEntryNum(),
                        indexWriter.getLevelNum(),
                        indexWriter.getUncompressedSize(),
                        (int) recordCount,
                        totalUncompressedSize,
                        compressionType);

        MemorySlice footerEncoding = Footer.writeFooter(footer);
        writeSlice(footerEncoding);

        // do not need to close outputStream, since it will be closed by outer classes

        LOG.info("totalUncompressedSize: {}", MemorySize.ofBytes(totalUncompressedSize));
        LOG.info("totalCompressedSize: {}", MemorySize.ofBytes(totalCompressedSize));
    }

    private void writeSlice(MemorySlice slice) throws IOException {
        out.write(slice.getHeapMemory(), slice.offset(), slice.length());
        position += slice.length();
    }
}
