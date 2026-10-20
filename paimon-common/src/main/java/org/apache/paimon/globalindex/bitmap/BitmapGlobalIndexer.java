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
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexKeyExtractor;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.SortedGlobalIndexer;
import org.apache.paimon.globalindex.SortedIndexFileMeta;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.LongUnaryOperator;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The {@link GlobalIndexer} for bitmap index. */
public class BitmapGlobalIndexer implements SortedGlobalIndexer {

    private final KeySerializer keySerializer;
    private final GlobalIndexKeyExtractor keyExtractor;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final long fallbackScanMaxSize;

    public BitmapGlobalIndexer(DataField dataField, Options options) {
        this.keySerializer = KeySerializer.create(dataField.type());
        this.keyExtractor = GlobalIndexKeyExtractor.identity(dataField.type());
        this.dictionaryBlockSize =
                (int)
                        options.get(BitmapGlobalIndexOptions.BITMAP_INDEX_DICTIONARY_BLOCK_SIZE)
                                .getBytes();
        CompressOptions compressOptions =
                new CompressOptions(
                        options.get(BitmapGlobalIndexOptions.BITMAP_INDEX_COMPRESSION),
                        options.get(BitmapGlobalIndexOptions.BITMAP_INDEX_COMPRESSION_LEVEL));
        this.compressionFactory = BlockCompressionFactory.create(compressOptions);
        this.fallbackScanMaxSize =
                options.get(BitmapGlobalIndexOptions.BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE)
                        .getBytes();
    }

    @Override
    public GlobalIndexKeyExtractor keyExtractor() {
        return keyExtractor;
    }

    @Override
    public BitmapGlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter)
            throws IOException {
        return new BitmapGlobalIndexWriter(
                fileWriter, keySerializer, dictionaryBlockSize, compressionFactory);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            @Nullable List<Range> rowRanges,
            ExecutorService executor) {
        return new LazyFilteredBitmapReader(
                fileReader, files, keySerializer, fallbackScanMaxSize, totalRowCount, executor);
    }

    @Override
    public List<ResultEntry> rewriteRowIds(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long sourceRowCount,
            long targetRowCount,
            LongUnaryOperator mapping,
            GlobalIndexFileWriter fileWriter)
            throws IOException {
        checkArgument(sourceRowCount >= 0 && targetRowCount >= 0, "Invalid row count.");
        List<ResultEntry> results = new ArrayList<>();
        if (targetRowCount == 0) {
            return results;
        }
        for (GlobalIndexIOMeta file : files) {
            try (SeekableInputStream input = fileReader.getInputStream(file)) {
                BitmapGlobalIndexFormat.SeekableReader reader =
                        (offset, length) -> {
                            input.seek(offset);
                            byte[] bytes = new byte[length];
                            IOUtils.readFully(input, bytes);
                            return bytes;
                        };
                BitmapGlobalIndexFormat.Footer footer =
                        BitmapGlobalIndexFormat.readFooter(reader, file.fileSize());
                RoaringNavigableMap64 nullRows =
                        mapRowIds(
                                BitmapGlobalIndexFormat.readBitmap(reader, footer.nullRowsBlock),
                                sourceRowCount,
                                targetRowCount,
                                mapping);
                RoaringNavigableMap64 nonNullRows =
                        mapRowIds(
                                BitmapGlobalIndexFormat.readBitmap(reader, footer.nonNullRowsBlock),
                                sourceRowCount,
                                targetRowCount,
                                mapping);
                if (nullRows.isEmpty() && nonNullRows.isEmpty()) {
                    continue;
                }
                String fileName = fileWriter.newFileName(BitmapGlobalIndexerFactory.IDENTIFIER);
                byte[] firstKey = null;
                byte[] lastKey = null;
                try (PositionOutputStream output = fileWriter.newOutputStream(fileName)) {
                    BitmapGlobalIndexFormat.StreamingWriter writer =
                            new BitmapGlobalIndexFormat.StreamingWriter(
                                    output, dictionaryBlockSize, compressionFactory);
                    for (BitmapGlobalIndexFormat.DictionaryBlockMeta block :
                            BitmapGlobalIndexFormat.readIndexBlock(reader, footer.indexBlock)) {
                        for (BitmapGlobalIndexFormat.DictionaryEntry entry :
                                BitmapGlobalIndexFormat.readDictionaryBlock(reader, block)
                                        .entries()) {
                            RoaringNavigableMap64 rowIds =
                                    mapRowIds(
                                            BitmapGlobalIndexFormat.readBitmap(
                                                    reader, entry.bitmapBlock()),
                                            sourceRowCount,
                                            targetRowCount,
                                            mapping);
                            if (!rowIds.isEmpty()) {
                                // Dictionary order is unchanged, so encoded keys need no decoding.
                                writer.write(entry.key(), rowIds);
                                if (firstKey == null) {
                                    firstKey = entry.key().bytes();
                                }
                                lastKey = entry.key().bytes();
                            }
                        }
                    }
                    writer.finish(nullRows, nonNullRows);
                }
                results.add(
                        new ResultEntry(
                                fileName,
                                nullRows.getLongCardinality() + nonNullRows.getLongCardinality(),
                                new SortedIndexFileMeta(firstKey, lastKey, !nullRows.isEmpty())
                                        .serialize()));
            }
        }
        return results;
    }

    private static RoaringNavigableMap64 mapRowIds(
            RoaringNavigableMap64 rows,
            long sourceRowCount,
            long targetRowCount,
            LongUnaryOperator mapping) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (long source : rows) {
            checkArgument(
                    source >= 0 && source < sourceRowCount, "Invalid source row ID: %s", source);
            long target = mapping.applyAsLong(source);
            checkArgument(
                    target >= -1 && target < targetRowCount, "Invalid target row ID: %s", target);
            if (target >= 0) {
                result.add(target);
            }
        }
        return result;
    }
}
