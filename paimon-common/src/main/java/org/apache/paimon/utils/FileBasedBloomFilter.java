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

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.cache.CacheCallback;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheKey.PositionCacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.sst.BloomFilterHandle;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.io.cache.CacheManager.REFRESH_COUNT;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util to apply a built bloom filter . */
public class FileBasedBloomFilter implements Closeable {

    private final SeekableInputStream input;
    private final CacheManager cacheManager;
    private final BloomFilter filter;
    private final PositionCacheKey cacheKey;

    private int accessCount;

    public FileBasedBloomFilter(
            SeekableInputStream input,
            Path filePath,
            CacheManager cacheManager,
            long expectedEntries,
            long readOffset,
            int readLength) {
        this.input = input;
        this.cacheManager = cacheManager;
        checkArgument(expectedEntries >= 0);
        this.filter = new BloomFilter(expectedEntries, readLength);
        this.accessCount = 0;
        this.cacheKey = CacheKey.forPosition(filePath, readOffset, readLength, true);
    }

    @Nullable
    public static FileBasedBloomFilter create(
            SeekableInputStream input,
            Path filePath,
            CacheManager cacheManager,
            @Nullable BloomFilterHandle bloomFilterHandle) {
        if (bloomFilterHandle == null) {
            return null;
        }
        return new FileBasedBloomFilter(
                input,
                filePath,
                cacheManager,
                bloomFilterHandle.expectedEntries(),
                bloomFilterHandle.offset(),
                bloomFilterHandle.size());
    }

    public boolean testHash(int hash) {
        accessCount++;
        // we should refresh cache in LRU, but we cannot refresh everytime, it is costly.
        // so we introduce a refresh count to reduce refresh
        if (accessCount == REFRESH_COUNT || filter.getMemorySegment() == null) {
            MemorySegment segment =
                    cacheManager.getPage(
                            cacheKey, this::readBytes, new BloomFilterCallBack(filter));
            filter.setMemorySegment(segment, 0);
            accessCount = 0;
        }
        return filter.testHash(hash);
    }

    private byte[] readBytes(CacheKey k) throws IOException {
        PositionCacheKey key = (PositionCacheKey) k;
        input.seek(key.position());
        byte[] bytes = new byte[key.length()];
        IOUtils.readFully(input, bytes);
        return bytes;
    }

    @VisibleForTesting
    BloomFilter bloomFilter() {
        return filter;
    }

    @Override
    public void close() throws IOException {
        cacheManager.invalidPage(cacheKey);
    }

    /** Call back for cache manager. */
    private static class BloomFilterCallBack implements CacheCallback {

        private final BloomFilter bloomFilter;

        private BloomFilterCallBack(BloomFilter bloomFilter) {
            this.bloomFilter = bloomFilter;
        }

        @Override
        public void onRemoval(CacheKey key) {
            this.bloomFilter.unsetMemorySegment();
        }
    }
}
