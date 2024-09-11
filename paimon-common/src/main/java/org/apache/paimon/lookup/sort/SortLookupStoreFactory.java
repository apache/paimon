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

package org.apache.paimon.lookup.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

/** A {@link LookupStoreFactory} which uses hash to lookup records on disk. */
public class SortLookupStoreFactory implements LookupStoreFactory {

    private final Comparator<MemorySlice> comparator;
    private final CacheManager cacheManager;
    private final int blockSize;
    @Nullable private final BlockCompressionFactory compressionFactory;

    public SortLookupStoreFactory(
            Comparator<MemorySlice> comparator,
            CacheManager cacheManager,
            int blockSize,
            CompressOptions compression) {
        this.comparator = comparator;
        this.cacheManager = cacheManager;
        this.blockSize = blockSize;
        this.compressionFactory = BlockCompressionFactory.create(compression);
    }

    @Override
    public SortLookupStoreReader createReader(File file, Context context) throws IOException {
        return new SortLookupStoreReader(comparator, file, (SortContext) context, cacheManager);
    }

    @Override
    public SortLookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException {
        return new SortLookupStoreWriter(file, blockSize, bloomFilter, compressionFactory);
    }
}
