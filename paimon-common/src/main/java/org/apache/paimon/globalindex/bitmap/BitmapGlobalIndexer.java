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
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/** The {@link GlobalIndexer} for bitmap index. */
public class BitmapGlobalIndexer implements GlobalIndexer {

    private final KeySerializer keySerializer;
    private final int dictionaryBlockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;
    private final long fallbackScanMaxSize;

    public BitmapGlobalIndexer(DataField dataField, Options options) {
        this.keySerializer = KeySerializer.create(dataField.type());
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
    public BitmapGlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter)
            throws IOException {
        return new BitmapGlobalIndexWriter(
                fileWriter, keySerializer, dictionaryBlockSize, compressionFactory);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            ExecutorService executor) {
        return new LazyFilteredBitmapReader(
                fileReader, files, keySerializer, fallbackScanMaxSize, executor);
    }
}
