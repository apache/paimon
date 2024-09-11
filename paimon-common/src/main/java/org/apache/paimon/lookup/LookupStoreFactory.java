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

package org.apache.paimon.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.hash.HashLookupStoreFactory;
import org.apache.paimon.lookup.sort.SortLookupStoreFactory;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.BloomFilter;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

/**
 * A key-value store for lookup, key-value store should be single binary file written once and ready
 * to be used. This factory provide two interfaces:
 *
 * <ul>
 *   <li>Writer: written once to prepare binary file.
 *   <li>Reader: lookup value by key bytes.
 * </ul>
 */
public interface LookupStoreFactory {

    LookupStoreWriter createWriter(File file, @Nullable BloomFilter.Builder bloomFilter)
            throws IOException;

    LookupStoreReader createReader(File file, Context context) throws IOException;

    static Function<Long, BloomFilter.Builder> bfGenerator(Options options) {
        Function<Long, BloomFilter.Builder> bfGenerator = rowCount -> null;
        if (options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_ENABLED)) {
            double bfFpp = options.get(CoreOptions.LOOKUP_CACHE_BLOOM_FILTER_FPP);
            bfGenerator =
                    rowCount -> {
                        if (rowCount > 0) {
                            return BloomFilter.builder(rowCount, bfFpp);
                        }
                        return null;
                    };
        }
        return bfGenerator;
    }

    static LookupStoreFactory create(
            CoreOptions options, CacheManager cacheManager, Comparator<MemorySlice> keyComparator) {
        CompressOptions compression = options.lookupCompressOptions();
        switch (options.lookupLocalFileType()) {
            case SORT:
                return new SortLookupStoreFactory(
                        keyComparator, cacheManager, options.cachePageSize(), compression);
            case HASH:
                return new HashLookupStoreFactory(
                        cacheManager,
                        options.cachePageSize(),
                        options.toConfiguration().get(CoreOptions.LOOKUP_HASH_LOAD_FACTOR),
                        compression);
            default:
                throw new IllegalArgumentException(
                        "Unsupported lookup local file type: " + options.lookupLocalFileType());
        }
    }

    /** Context between writer and reader. */
    interface Context {}
}
