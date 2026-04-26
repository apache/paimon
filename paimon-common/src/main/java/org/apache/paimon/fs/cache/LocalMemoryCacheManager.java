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

package org.apache.paimon.fs.cache;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Block-level in-memory cache with LRU eviction. Thread-safe. */
public class LocalMemoryCacheManager implements LocalCacheManager {

    private static final Map<CacheKey, LocalMemoryCacheManager> SHARED_CACHES =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final int blockSize;
    private final Object lock = new Object();
    private final LinkedHashMap<BlockKey, byte[]> cache;

    private long currentSize;

    public LocalMemoryCacheManager(long maxSizeBytes, int blockSize) {
        this.blockSize = blockSize;
        this.currentSize = 0;
        this.cache =
                new LinkedHashMap<BlockKey, byte[]>(64, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<BlockKey, byte[]> eldest) {
                        if (currentSize > maxSizeBytes) {
                            currentSize -= eldest.getValue().length;
                            return true;
                        }
                        return false;
                    }
                };
    }

    public static LocalMemoryCacheManager getOrCreate(long maxSizeBytes, int blockSize) {
        CacheKey key = new CacheKey(maxSizeBytes, blockSize);
        return SHARED_CACHES.computeIfAbsent(
                key, k -> new LocalMemoryCacheManager(maxSizeBytes, blockSize));
    }

    @Override
    public int blockSize() {
        return blockSize;
    }

    @Nullable
    @Override
    public byte[] getBlock(String filePath, int blockIndex) {
        BlockKey key = new BlockKey(filePath, blockIndex);
        synchronized (lock) {
            return cache.get(key);
        }
    }

    @Override
    public void putBlock(String filePath, int blockIndex, byte[] data) {
        BlockKey key = new BlockKey(filePath, blockIndex);
        synchronized (lock) {
            if (!cache.containsKey(key)) {
                currentSize += data.length;
                cache.put(key, data);
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            cache.clear();
            currentSize = 0;
        }
    }

    private static class BlockKey {
        final String filePath;
        final int blockIndex;

        BlockKey(String filePath, int blockIndex) {
            this.filePath = filePath;
            this.blockIndex = blockIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlockKey)) {
                return false;
            }
            BlockKey that = (BlockKey) o;
            return blockIndex == that.blockIndex && Objects.equals(filePath, that.filePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filePath, blockIndex);
        }
    }

    private static class CacheKey {
        final long maxSize;
        final int blockSize;

        CacheKey(long maxSize, int blockSize) {
            this.maxSize = maxSize;
            this.blockSize = blockSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return maxSize == that.maxSize && blockSize == that.blockSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxSize, blockSize);
        }
    }
}
