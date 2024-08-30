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

package org.apache.paimon.lookup.hash;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/** A {@link LookupStoreFactory} which uses hash to lookup records on disk. */
public class HashLookupStoreFactory implements LookupStoreFactory {

    private final CacheManager cacheManager;
    private final int cachePageSize;
    private final double loadFactor;
    @Nullable private final BlockCompressionFactory compressionFactory;

    public HashLookupStoreFactory(
            CacheManager cacheManager,
            int cachePageSize,
            double loadFactor,
            CompressOptions compression) {
        this.cacheManager = cacheManager;
        this.cachePageSize = cachePageSize;
        this.loadFactor = loadFactor;
        this.compressionFactory = BlockCompressionFactory.create(compression);
    }

    @Override
    public HashLookupStoreReader createReader(File file, Context context) throws IOException {
        return new HashLookupStoreReader(
                file, (HashContext) context, cacheManager, cachePageSize, compressionFactory);
    }

    @Override
    public HashLookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException {
        return new HashLookupStoreWriter(
                loadFactor, file, bloomFilter, compressionFactory, cachePageSize);
    }
}
