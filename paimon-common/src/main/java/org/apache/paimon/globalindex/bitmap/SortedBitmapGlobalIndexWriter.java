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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A bitmap index writer for sorted input. Non-null keys must be written in monotonically increasing
 * order so completed bitmaps can be streamed to the output file instead of retained until {@link
 * #finish()}.
 */
class SortedBitmapGlobalIndexWriter implements GlobalIndexSingleColumnWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final RoaringNavigableMap64 currentBitmap;
    private final RoaringNavigableMap64 nullRows;
    private final RoaringNavigableMap64 nonNullRows;

    private String fileName;
    private PositionOutputStream outputStream;
    private BitmapGlobalIndexFormat.StreamingWriter streamingWriter;
    private long rowCount;
    private Object firstKey;
    private Object lastKey;

    SortedBitmapGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            KeySerializer keySerializer,
            int dictionaryBlockSize,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.fileWriter = fileWriter;
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        this.dictionaryBlockSize = dictionaryBlockSize;
        this.compressionFactory = compressionFactory;
        this.currentBitmap = new RoaringNavigableMap64();
        this.nullRows = new RoaringNavigableMap64();
        this.nonNullRows = new RoaringNavigableMap64();
    }

    @Override
    public void write(@Nullable Object key, long relativeRowId) {
        rowCount++;
        if (key == null) {
            nullRows.add(relativeRowId);
            return;
        }

        nonNullRows.add(relativeRowId);
        if (lastKey != null) {
            int comparison = comparator.compare(key, lastKey);
            if (comparison < 0) {
                throw new IllegalArgumentException(
                        "Bitmap index keys must be written in monotonically increasing order.");
            }
            if (comparison > 0) {
                flushCurrentBitmap();
            }
        }
        if (firstKey == null) {
            firstKey = key;
        }
        lastKey = key;
        currentBitmap.add(relativeRowId);
    }

    @Override
    public List<ResultEntry> finish() {
        if (rowCount == 0) {
            return Collections.emptyList();
        }

        try {
            flushCurrentBitmap();
            streamingWriter().finish(nullRows, nonNullRows);
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Error in closing bitmap index writer.", e);
        }

        byte[] meta =
                new SortedIndexFileMeta(
                                firstKey == null ? null : keySerializer.serialize(firstKey),
                                lastKey == null ? null : keySerializer.serialize(lastKey),
                                !nullRows.isEmpty())
                        .serialize();
        return Collections.singletonList(new ResultEntry(fileName, rowCount, meta));
    }

    private void flushCurrentBitmap() {
        if (currentBitmap.isEmpty()) {
            return;
        }
        try {
            streamingWriter()
                    .write(
                            BitmapGlobalIndexFormat.SerializedKey.fromObject(
                                    keySerializer, lastKey),
                            currentBitmap);
            currentBitmap.clear();
        } catch (IOException e) {
            throw new RuntimeException("Error in writing bitmap index files.", e);
        }
    }

    private BitmapGlobalIndexFormat.StreamingWriter streamingWriter() throws IOException {
        if (streamingWriter == null) {
            fileName = fileWriter.newFileName(BitmapGlobalIndexerFactory.IDENTIFIER);
            outputStream = fileWriter.newOutputStream(fileName);
            streamingWriter =
                    new BitmapGlobalIndexFormat.StreamingWriter(
                            outputStream, dictionaryBlockSize, compressionFactory);
        }
        return streamingWriter;
    }
}
