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

package org.apache.paimon.io.cache;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;
import java.util.function.BiConsumer;

/** Cache manager to cache bytes to paged {@link MemorySegment}s. */
public class CacheManager {

    /**
     * Refreshing the cache comes with some costs, so not every time we visit the CacheManager, but
     * every 10 visits, refresh the LRU strategy.
     */
    public static final int REFRESH_COUNT = 10;

    private final Cache<CacheKey, CacheValue> cache;

    private int fileReadCount;

    public CacheManager(MemorySize maxMemorySize) {
        this.cache =
                Caffeine.newBuilder()
                        .weigher(this::weigh)
                        .maximumWeight(maxMemorySize.getBytes())
                        .removalListener(this::onRemoval)
                        .executor(MoreExecutors.directExecutor())
                        .build();
        this.fileReadCount = 0;
    }

    @VisibleForTesting
    public Cache<CacheKey, CacheValue> cache() {
        return cache;
    }

    public MemorySegment getPage(
            RandomAccessFile file,
            long readOffset,
            int readLength,
            BiConsumer<Long, Integer> cleanCallback) {
        CacheKey key = new CacheKey(file, readOffset, readLength);
        CacheValue value = cache.getIfPresent(key);
        while (value == null || value.isClosed) {
            try {
                value = new CacheValue(key.read(), cleanCallback);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            cache.put(key, value);
        }
        return value.segment;
    }

    public void invalidPage(RandomAccessFile file, long readOffset, int readLength) {
        cache.invalidate(new CacheKey(file, readOffset, readLength));
    }

    private int weigh(CacheKey cacheKey, CacheValue cacheValue) {
        return cacheValue.segment.size();
    }

    private void onRemoval(CacheKey key, CacheValue value, RemovalCause cause) {
        value.isClosed = true;
        value.cleanCallback.accept(key.offset, key.length);
    }

    public int fileReadCount() {
        return fileReadCount;
    }

    private class CacheKey {

        private final RandomAccessFile file;
        private final long offset;
        private final int length;

        private CacheKey(RandomAccessFile file, long offset, int length) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }

        private MemorySegment read() throws IOException {
            byte[] bytes = new byte[length];
            file.seek(offset);
            file.readFully(bytes);
            fileReadCount++;
            return MemorySegment.wrap(bytes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(file, cacheKey.file)
                    && offset == cacheKey.offset
                    && length == cacheKey.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, offset, length);
        }
    }

    private static class CacheValue {

        private final MemorySegment segment;
        private final BiConsumer<Long, Integer> cleanCallback;

        private boolean isClosed = false;

        private CacheValue(MemorySegment segment, BiConsumer<Long, Integer> cleanCallback) {
            this.segment = segment;
            this.cleanCallback = cleanCallback;
        }
    }
}
