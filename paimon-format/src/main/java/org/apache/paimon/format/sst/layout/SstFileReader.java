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
import org.apache.paimon.utils.MurmurHashUtils;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.paimon.format.sst.layout.SstFileUtils.crc32c;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An SST FileReader which serves point queries and range queries. The implementation directly
 * processes bytes.
 *
 * <p>Note that this class is NOT thread-safe.
 */
public class SstFileReader implements Closeable {

    private final Comparator<MemorySlice> comparator;
    private final long fileSize;
    private final Path filePath;
    private final Footer footer;
    private final FileInfo fileInfo;
    private final MemorySlice firstKey;
    private final SeekableInputStream input;
    private final BlockIterator indexBlockIterator;
    private BlockIterator currentDataIterator;
    private final BlockCompressionFactory compressionFactory;
    @Nullable private final BloomFilter bloomFilter;

    public SstFileReader(
            SeekableInputStream input,
            Comparator<MemorySlice> comparator,
            long fileSize,
            Path filePath)
            throws IOException {
        this.comparator = comparator;
        this.fileSize = fileSize;
        this.filePath = filePath;
        this.input = input;

        Footer footer =
                Footer.readFooter(
                        readSlice(fileSize - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH)
                                .toInput());
        this.footer = footer;
        this.compressionFactory = BlockCompressionFactory.create(footer.getCompressionType());
        this.indexBlockIterator = readBlock(footer.getIndexBlockHandle()).iterator();
        this.bloomFilter = readBloomFilter(footer.getBloomFilterHandle());
        BlockHandle fileInfoHandle = footer.getFileInfoHandle();
        this.fileInfo =
                FileInfo.readFileInfo(
                        readSlice(fileInfoHandle.offset(), fileInfoHandle.size()).toInput());
        this.firstKey =
                this.fileInfo.getFirstKey() == null
                        ? null
                        : MemorySlice.wrap(this.fileInfo.getFirstKey());
    }

    private BloomFilter readBloomFilter(BloomFilterHandle bloomFilterHandle) throws IOException {
        BloomFilter bloomFilter = null;
        if (bloomFilterHandle != null) {
            MemorySegment memorySegment =
                    readSlice(bloomFilterHandle.offset(), bloomFilterHandle.size()).segment();
            bloomFilter =
                    new BloomFilter(bloomFilterHandle.expectedEntries(), memorySegment.size());
            bloomFilter.setMemorySegment(memorySegment, 0);
        }
        return bloomFilter;
    }

    /**
     * Lookup the specified key in the file.
     *
     * @param key serialized key
     * @return corresponding serialized value, null if not found.
     */
    public byte[] lookup(byte[] key) throws IOException {
        if (bloomFilter != null && !bloomFilter.testHash(MurmurHashUtils.hashBytes(key))) {
            return null;
        }

        MemorySlice keySlice = MemorySlice.wrap(key);
        if (firstKey == null || comparator.compare(firstKey, keySlice) > 0) {
            return null;
        }
        // lookup should not affect file read position
        BlockIterator detachedIndexIter = indexBlockIterator.detach();
        // seek the index to the block containing the key
        detachedIndexIter.seekTo(keySlice);

        // if indexIterator does not have a next, it means the key does not exist in this iterator
        if (detachedIndexIter.hasNext()) {
            // seek the current iterator to the key
            BlockIterator current = getNextBlock(detachedIndexIter);
            if (current.seekTo(keySlice)) {
                return current.next().getValue().copyBytes();
            }
        }
        return null;
    }

    /**
     * Seek to the position of the record whose key is equal to the key or is the smallest element
     * greater than the given key.
     *
     * @param key the key to seek
     * @return record position of the seeked record, -1 if not found.
     */
    public int seekTo(byte[] key) throws IOException {
        if (footer.getRowCount() == 0) {
            return -1;
        }
        MemorySlice keySlice = MemorySlice.wrap(key);
        // find candidate index block
        indexBlockIterator.reset();
        indexBlockIterator.seekTo(keySlice);
        if (indexBlockIterator.hasNext()) {
            // avoid fetching data block if the key is smaller than firstKey
            if (comparator.compare(firstKey, keySlice) > 0) {
                return 0;
            }
            IndexEntryHandle entryHandle =
                    IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
            currentDataIterator =
                    readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()))
                            .iterator();
            currentDataIterator.seekTo(keySlice);
            int recordCount = currentDataIterator.getRecordCount();
            int positionInBlock = currentDataIterator.getRecordPosition();
            Preconditions.checkState(
                    positionInBlock >= 0, "Position inside data block should >= 0, it's a bug.");
            return entryHandle.getLastRecordPosition() - (recordCount - positionInBlock - 1);
        }
        return -1;
    }

    /**
     * Read a batch of records from current position.
     *
     * @return a batch of entries, null if reaching file end
     */
    public BlockIterator readBatch() throws IOException {
        BlockIterator result = null;
        if (currentDataIterator == null || !currentDataIterator.hasNext()) {
            // reach file end
            if (!indexBlockIterator.hasNext()) {
                return null;
            }
            currentDataIterator = getNextBlock(indexBlockIterator);
        }
        result = currentDataIterator;
        currentDataIterator = null;
        return result;
    }

    /**
     * Seek to the specified record position.
     *
     * @param position position
     */
    public void seekTo(int position) throws IOException {
        if (position < 0 || position >= footer.getRowCount()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Index out of range, file %s only have %s rows, but trying to seek to %s",
                            filePath, footer.getRowCount(), position));
        }

        // 1. seekTo the block entry exactly containing the position
        indexBlockIterator.reset();
        indexBlockIterator.seekTo(
                position,
                Comparator.naturalOrder(),
                entry -> IndexEntryHandle.read(entry.getValue().toInput()).getLastRecordPosition());

        // 2. fetch the data block
        Preconditions.checkState(indexBlockIterator.hasNext());
        IndexEntryHandle entryHandle =
                IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
        currentDataIterator =
                readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()))
                        .iterator();

        // 3. seek to the inner position of data block
        int positionInBlock =
                position
                        - (entryHandle.getLastRecordPosition()
                                - currentDataIterator.getRecordCount()
                                + 1);
        currentDataIterator.seekTo(positionInBlock);
    }

    private BlockIterator getNextBlock(BlockIterator indexBlockIterator) throws IOException {
        IndexEntryHandle entryHandle =
                IndexEntryHandle.read(indexBlockIterator.next().getValue().toInput());
        BlockReader dataBlock =
                readBlock(new BlockHandle(entryHandle.getOffset(), entryHandle.getSize()));
        return dataBlock.iterator();
    }

    private MemorySlice readSlice(long offset, int size) throws IOException {
        // todo: support page cache
        input.seek(offset);
        byte[] sliceBytes = new byte[size];
        IOUtils.readFully(input, sliceBytes);
        return MemorySlice.wrap(sliceBytes);
    }

    private BlockReader readBlock(BlockHandle blockHandle) throws IOException {
        // todo: reuse dataBlock in scan
        // 1. read trailer
        MemorySlice trailerSlice =
                readSlice(blockHandle.offset() + blockHandle.size(), BlockTrailer.ENCODED_LENGTH);
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(trailerSlice.toInput());
        // 2. read block
        MemorySlice blockSlice = readSlice(blockHandle.offset(), blockHandle.size());
        byte[] decompressed = decompressBlock(blockSlice.getHeapMemory(), blockTrailer);
        return new BlockReader(MemorySlice.wrap(decompressed), comparator);
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
