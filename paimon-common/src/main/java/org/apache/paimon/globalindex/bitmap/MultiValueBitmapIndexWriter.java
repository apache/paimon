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
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** Streams one bitmap posting list per distinct non-null normalized array element. */
public class MultiValueBitmapIndexWriter implements GlobalIndexSingleColumnWriter, Closeable {

    private final GlobalIndexFileWriter fileWriter;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final RoaringNavigableMap64 currentBitmap = new RoaringNavigableMap64();

    private String fileName;
    private PositionOutputStream outputStream;
    private BitmapGlobalIndexFormat.StreamingWriter streamingWriter;
    private BitmapGlobalIndexFormat.SerializedKey currentKey;
    private Object currentKeyObject;
    private byte[] firstKey;
    private byte[] lastKey;

    MultiValueBitmapIndexWriter(
            GlobalIndexFileWriter fileWriter,
            KeySerializer keySerializer,
            int dictionaryBlockSize,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.fileWriter = fileWriter;
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        this.dictionaryBlockSize = dictionaryBlockSize;
        this.compressionFactory = compressionFactory;
    }

    @Override
    public void write(@Nullable Object key, long relativeRowId) {
        Preconditions.checkArgument(
                relativeRowId >= 0,
                "Relative row id must be non-negative, but was %s.",
                relativeRowId);
        if (key == null) {
            return;
        }

        if (currentKeyObject != null) {
            int comparison = comparator.compare(key, currentKeyObject);
            Preconditions.checkArgument(
                    comparison >= 0,
                    "Multivalue index keys must be written in monotonically increasing order.");
            if (comparison > 0) {
                flushCurrentBitmap();
            }
        }
        if (currentKeyObject == null) {
            currentKey = BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, key);
            currentKeyObject = keySerializer.deserialize(MemorySlice.wrap(currentKey.bytes()));
            if (firstKey == null) {
                firstKey = currentKey.bytes();
            }
            lastKey = currentKey.bytes();
        }
        currentBitmap.add(relativeRowId);
    }

    @Override
    public List<ResultEntry> finish() {
        throw new IllegalStateException(
                "Multivalue index writers must be finished with the source row count.");
    }

    @Override
    public List<ResultEntry> finish(long sourceRowCount) {
        Preconditions.checkArgument(
                sourceRowCount >= 0,
                "Source row count must be non-negative, but was %s.",
                sourceRowCount);
        if (sourceRowCount == 0) {
            return Collections.emptyList();
        }

        try {
            flushCurrentBitmap();
            streamingWriter().finish(new RoaringNavigableMap64(), new RoaringNavigableMap64());
            close();
        } catch (IOException e) {
            throw new RuntimeException("Error in closing multivalue index writer.", e);
        }

        byte[] meta = new SortedIndexFileMeta(firstKey, lastKey, false).serialize();
        return Collections.singletonList(new ResultEntry(fileName, sourceRowCount, meta));
    }

    @Override
    public void close() throws IOException {
        PositionOutputStream stream = outputStream;
        outputStream = null;
        if (stream != null) {
            stream.close();
        }
    }

    private void flushCurrentBitmap() {
        if (currentKey == null) {
            return;
        }
        try {
            streamingWriter().write(currentKey, currentBitmap);
            currentBitmap.clear();
            currentKey = null;
            currentKeyObject = null;
        } catch (IOException e) {
            throw new RuntimeException("Error in writing multivalue index files.", e);
        }
    }

    private BitmapGlobalIndexFormat.StreamingWriter streamingWriter() throws IOException {
        if (streamingWriter == null) {
            fileName = fileWriter.newFileName(MultiValueGlobalIndexerFactory.IDENTIFIER);
            outputStream = fileWriter.newOutputStream(fileName);
            streamingWriter =
                    new BitmapGlobalIndexFormat.StreamingWriter(
                            outputStream, dictionaryBlockSize, compressionFactory);
        }
        return streamingWriter;
    }
}
