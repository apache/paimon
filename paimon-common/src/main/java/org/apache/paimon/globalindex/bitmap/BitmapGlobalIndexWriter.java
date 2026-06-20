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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The {@link GlobalIndexSingleColumnWriter} implementation for bitmap index. */
public class BitmapGlobalIndexWriter implements GlobalIndexSingleColumnWriter {

    private final GlobalIndexFileWriter fileWriter;
    private final KeySerializer keySerializer;
    private final Comparator<Object> comparator;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final Map<BitmapGlobalIndexFormat.SerializedKey, RoaringNavigableMap64> bitmaps;
    private final RoaringNavigableMap64 nullRows;
    private final RoaringNavigableMap64 nonNullRows;

    private long rowCount;
    private Object firstKey;
    private Object lastKey;

    BitmapGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            KeySerializer keySerializer,
            int dictionaryBlockSize,
            @Nullable BlockCompressionFactory compressionFactory) {
        this.fileWriter = fileWriter;
        this.keySerializer = keySerializer;
        this.comparator = keySerializer.createComparator();
        this.dictionaryBlockSize = dictionaryBlockSize;
        this.compressionFactory = compressionFactory;
        this.bitmaps = new LinkedHashMap<>();
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
        updateMinMax(key);
        BitmapGlobalIndexFormat.SerializedKey serializedKey =
                BitmapGlobalIndexFormat.SerializedKey.fromObject(keySerializer, key);
        bitmaps.computeIfAbsent(serializedKey, k -> new RoaringNavigableMap64()).add(relativeRowId);
    }

    @Override
    public List<ResultEntry> finish() {
        if (rowCount == 0) {
            return Collections.emptyList();
        }

        String fileName = fileWriter.newFileName(BitmapGlobalIndexerFactory.IDENTIFIER);
        try (PositionOutputStream outputStream = fileWriter.newOutputStream(fileName)) {
            BitmapGlobalIndexFormat.write(
                    outputStream,
                    nullRows,
                    nonNullRows,
                    bitmaps,
                    dictionaryBlockSize,
                    compressionFactory);
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

    private void updateMinMax(Object key) {
        if (firstKey == null || comparator.compare(key, firstKey) < 0) {
            firstKey = key;
        }
        if (lastKey == null || comparator.compare(key, lastKey) > 0) {
            lastKey = key;
        }
    }
}
