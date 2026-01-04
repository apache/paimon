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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.sst.BlockHandle;
import org.apache.paimon.sst.BloomFilterHandle;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The {@link GlobalIndexSingletonWriter} implementation for BTree index. Note that users must keep
 * written keys monotonically incremental. All null keys are stored in a separate bitmap, which will
 * be serialized and appended to the file end on close. The layout is as below:
 *
 * <pre>
 *    +-----------------------------------+------+
 *    |             Footer                |      |
 *    +-----------------------------------+      |
 *    |           Index Block             |      +--> Loaded on open
 *    +-----------------------------------+      |
 *    |        Bloom Filter Block         |      |
 *    +-----------------------------------+------+
 *    |         Null Bitmap Block         |      |
 *    +-----------------------------------+      |
 *    |            Data Block             |      |
 *    +-----------------------------------+      +--> Loaded on requested
 *    |              ......               |      |
 *    +-----------------------------------+      |
 *    |            Data Block             |      |
 *    +-----------------------------------+------+
 * </pre>
 *
 * <p>For efficiency, we combine entries with the same keys and store a compact list of row ids for
 * each key.
 */
public class BTreeIndexWriter implements GlobalIndexParallelWriter {

    private final String fileName;
    private final PositionOutputStream out;

    private final SstFileWriter writer;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final List<Long> currentRowIds = new ArrayList<>();
    private final LazyField<RoaringNavigableMap64> nullBitmap =
            new LazyField<>(RoaringNavigableMap64::new);

    private Object firstKey = null;
    private Object lastKey = null;
    private long rowCount = 0;

    public BTreeIndexWriter(
            GlobalIndexFileWriter indexFileWriter,
            KeySerializer keySerializer,
            int blockSize,
            BlockCompressionFactory compressionFactory)
            throws IOException {
        this.fileName = indexFileWriter.newFileName(BTreeGlobalIndexerFactory.IDENTIFIER);
        this.out = indexFileWriter.newOutputStream(this.fileName);
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        // todo: we may enable bf to accelerate equal and in predicate in the future
        this.writer = new SstFileWriter(out, blockSize, null, compressionFactory);
    }

    @Override
    public void write(@Nullable Object key, long rowId) {
        rowCount++;
        if (key == null) {
            nullBitmap.get().add(rowId);
            return;
        }

        if (lastKey != null && comparator.compare(key, lastKey) != 0) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException("Error in writing btree index files.", e);
            }
        }
        lastKey = key;
        currentRowIds.add(rowId);

        // update stats
        if (firstKey == null) {
            firstKey = key;
        }
    }

    private void flush() throws IOException {
        if (currentRowIds.isEmpty()) {
            return;
        }

        // serialize row id list
        MemorySliceOutput sliceOutput = new MemorySliceOutput(currentRowIds.size() * 9 + 5);
        sliceOutput.writeVarLenInt(currentRowIds.size());
        for (long currentRowId : currentRowIds) {
            sliceOutput.writeVarLenLong(currentRowId);
        }
        currentRowIds.clear();

        writer.put(keySerializer.serialize(lastKey), sliceOutput.toSlice().copyBytes());
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            // write remaining row ids
            flush();

            // flush writer remaining data blocks
            writer.flush();

            // write null bitmap
            BlockHandle nullBitmapHandle = writeNullBitmap();

            // write bloom filter (currently is always null, but we could add it for equal
            // and in condition.)
            BloomFilterHandle bloomFilterHandle = writer.writeBloomFilter();

            // write index block
            BlockHandle indexBlockHandle = writer.writeIndexBlock();

            // write footer
            BTreeFileFooter footer =
                    new BTreeFileFooter(bloomFilterHandle, indexBlockHandle, nullBitmapHandle);
            MemorySlice footerEncoding = BTreeFileFooter.writeFooter(footer);
            writer.writeSlice(footerEncoding);

            out.close();
        } catch (IOException e) {
            throw new RuntimeException("Error in closing BTree index writer", e);
        }

        if (firstKey == null && !nullBitmap.initialized()) {
            throw new RuntimeException("Should never write an empty btree index file.");
        }

        byte[] metaBytes =
                new BTreeIndexMeta(
                                firstKey == null ? null : keySerializer.serialize(firstKey),
                                lastKey == null ? null : keySerializer.serialize(lastKey),
                                nullBitmap.initialized())
                        .serialize();
        return Collections.singletonList(new ResultEntry(fileName, rowCount, metaBytes));
    }

    @Nullable
    private BlockHandle writeNullBitmap() throws IOException {
        if (!nullBitmap.initialized()) {
            return null;
        }

        CRC32 crc32 = new CRC32();
        byte[] serializedBitmap = nullBitmap.get().serialize();
        int length = serializedBitmap.length;
        crc32.update(serializedBitmap, 0, length);

        // serialized bitmap + crc value
        MemorySliceOutput sliceOutput = new MemorySliceOutput(length + 4);
        sliceOutput.writeBytes(serializedBitmap);
        sliceOutput.writeInt((int) crc32.getValue());

        BlockHandle nullBitmapHandle = new BlockHandle(out.getPos(), length);
        writer.writeSlice(sliceOutput.toSlice());

        return nullBitmapHandle;
    }
}
