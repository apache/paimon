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
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The {@link GlobalIndexWriter} implementation for BTree index. Note that users must keep written
 * keys monotonically incremental. All null keys are stored in a separate bitmap, which will be
 * serialized and appended to the file end on close.
 */
public class BTreeIndexWriter implements GlobalIndexWriter {
    public static final int MAGIC_NUMBER = 198732882;

    private final String fileName;
    private final PositionOutputStream out;

    private byte[] firstKeyBytes;
    private byte[] lastKeyBytes;
    private long maxRowId = Long.MIN_VALUE;
    private long minRowId = Long.MAX_VALUE;

    private final SstFileWriter writer;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;

    private Object lastKey = null;
    private final List<Long> currentRowIds = new ArrayList<>();

    // for nulls
    LazyField<RoaringNavigableMap64> nullBitmap = new LazyField<>(RoaringNavigableMap64::new);

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
        this.writer = new SstFileWriter(out, blockSize, null, compressionFactory);
    }

    @Override
    public void write(@Nullable Object key) {
        throw new UnsupportedOperationException(
                "BTree index writer should explicitly specify row id for each key");
    }

    @Override
    public void write(@Nullable Object key, long rowId) {
        if (key == null) {
            nullBitmap.get().add(rowId);
            return;
        }

        byte[] keyBytes = keySerializer.serialize(key);

        if (firstKeyBytes == null) {
            firstKeyBytes = keyBytes;
        }
        lastKeyBytes = keyBytes;
        minRowId = Math.min(minRowId, rowId);
        maxRowId = Math.max(maxRowId, rowId);

        if (lastKey != null && comparator.compare(key, lastKey) != 0) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException("Error in writing btree index files.", e);
            }
        }

        lastKey = key;
        currentRowIds.add(rowId);
    }

    private void flush() throws IOException {
        if (currentRowIds.isEmpty()) {
            return;
        }

        MemorySliceOutput sliceOutput = new MemorySliceOutput(currentRowIds.size() * 9 + 5);
        sliceOutput.writeVarLenInt(currentRowIds.size());
        for (long currentRowId : currentRowIds) {
            sliceOutput.writeVarLenLong(currentRowId);
        }
        currentRowIds.clear();

        writer.put(lastKeyBytes, sliceOutput.toSlice().copyBytes());
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            // close the sst file writer
            flush();
            writer.close();

            // write null bitmap
            long nullBitmapOffset = out.getPos();
            int crc32c = writeNullBitmap();

            // write trailer
            writeTrailer(new Trailer(nullBitmapOffset, crc32c));

            out.close();
        } catch (IOException e) {
            throw new RuntimeException("Error in closing BTree index writer", e);
        }

        if (firstKeyBytes == null) {
            throw new RuntimeException("Should never write an empty btree index file.");
        }

        return Collections.singletonList(
                ResultEntry.of(
                        fileName,
                        new BTreeIndexMeta(firstKeyBytes, lastKeyBytes, nullBitmap.initialized())
                                .serialize(),
                        new Range(minRowId, maxRowId)));
    }

    private int writeNullBitmap() throws IOException {
        CRC32 crc32 = new CRC32();
        if (!nullBitmap.initialized()) {
            crc32.update(0);
            out.write(0);
        } else {
            byte[] serializedBitmap = nullBitmap.get().serialize();
            int length = serializedBitmap.length;
            crc32.update(length);
            crc32.update(serializedBitmap, 0, length);
            out.write(length);
            out.write(serializedBitmap);
        }
        return (int) crc32.getValue();
    }

    private void writeTrailer(Trailer trailer) throws IOException {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(Trailer.TRAILER_LENGTH);
        Trailer.write(trailer, sliceOutput);
        out.write(sliceOutput.toSlice().copyBytes());
    }
}
