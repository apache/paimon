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
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheKey.PositionCacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.sst.BloomFilterHandle;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util to apply a built bloom filter . */
public class FileBasedBloomFilter implements Closeable {

    private final SeekableInputStream input;
    private final CacheManager cacheManager;
    private final BloomFilter filter;
    private final PositionCacheKey cacheKey;
    private final boolean cacheable;

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
        this.cacheKey = CacheKey.forPosition(filePath, readOffset, readLength, true);
        this.cacheable = cacheManager.canFitPage(readLength, true);
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

    /** Whether the complete filter fits the configured index cache budget. */
    public boolean isCacheable() {
        return cacheable;
    }

    /** Whether the filter is currently resident; fitting the budget does not imply admission. */
    public boolean isCached() {
        return cacheManager.contains(cacheKey);
    }

    /** Number of bytes needed to read the complete filter. */
    public int size() {
        return cacheKey.length();
    }

    /** Tests a resident filter, or returns null without reading the file on a cache miss. */
    @Nullable
    public Boolean testHashIfPresent(int hash) {
        MemorySegment segment = cacheManager.getPageIfPresent(cacheKey);
        return segment == null ? null : filter.testHash(hash, segment);
    }

    public boolean testHash(int hash) {
        MemorySegment segment = cacheManager.getPageIfPresent(cacheKey);
        if (segment == null) {
            segment = cacheManager.getPage(cacheKey, this::readBytes);
        }
        return filter.testHash(hash, segment);
    }

    private byte[] readBytes(CacheKey k) throws IOException {
        PositionCacheKey key = (PositionCacheKey) k;
        byte[] bytes = new byte[key.length()];
        if (input instanceof VectoredReadable) {
            ((VectoredReadable) input).preadFully(key.position(), bytes, 0, key.length());
        } else {
            synchronized (input) {
                input.seek(key.position());
                IOUtils.readFully(input, bytes);
            }
        }
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
}
