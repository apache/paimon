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
import java.util.function.Consumer;

/** Cache manager to cache bytes to paged {@link MemorySegment}s. */
public class CacheManager {

    private final int pageSize;
    private final Cache<CacheKey, CacheValue> cache;

    public CacheManager(int pageSize, MemorySize maxMemorySize) {
        this.pageSize = pageSize;
        this.cache =
                Caffeine.newBuilder()
                        .weigher(this::weigh)
                        .maximumWeight(maxMemorySize.getBytes())
                        .removalListener(this::onRemoval)
                        .executor(MoreExecutors.directExecutor())
                        .build();
    }

    @VisibleForTesting
    Cache<CacheKey, CacheValue> cache() {
        return cache;
    }

    public int pageSize() {
        return pageSize;
    }

    public MemorySegment getPage(
            RandomAccessFile file,
            long fileLength,
            int pageNumber,
            Consumer<Integer> cleanCallback) {
        CacheKey key = new CacheKey(file, pageNumber);
        CacheValue value = cache.getIfPresent(key);
        while (value == null || value.isClosed) {
            try {
                value = createValue(key, fileLength, cleanCallback);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            cache.put(key, value);
        }
        return value.segment;
    }

    public void invalidPage(RandomAccessFile file, int pageNumber) {
        cache.invalidate(new CacheKey(file, pageNumber));
    }

    private int weigh(CacheKey cacheKey, CacheValue cacheValue) {
        return cacheValue.segment.size();
    }

    private void onRemoval(CacheKey key, CacheValue value, RemovalCause cause) {
        value.isClosed = true;
        value.cleanCallback.accept(key.pageNumber);
    }

    private CacheValue createValue(CacheKey key, long fileLength, Consumer<Integer> cleanCallback)
            throws IOException {
        return new CacheValue(key.read(fileLength, pageSize), cleanCallback);
    }

    private static class CacheKey {

        private final RandomAccessFile file;
        private final int pageNumber;

        private CacheKey(RandomAccessFile file, int pageNumber) {
            this.file = file;
            this.pageNumber = pageNumber;
        }

        private MemorySegment read(long fileLength, int pageSize) throws IOException {
            long pageAddress = (long) pageNumber * pageSize;
            int len = (int) Math.min(pageSize, fileLength - pageAddress);
            byte[] bytes = new byte[len];
            file.seek(pageAddress);
            file.readFully(bytes);
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
            return pageNumber == cacheKey.pageNumber && Objects.equals(file, cacheKey.file);
        }

        @Override
        public int hashCode() {
            return Objects.hash(file, pageNumber);
        }
    }

    private static class CacheValue {

        private final MemorySegment segment;
        private final Consumer<Integer> cleanCallback;

        private boolean isClosed = false;

        private CacheValue(MemorySegment segment, Consumer<Integer> cleanCallback) {
            this.segment = segment;
            this.cleanCallback = cleanCallback;
        }
    }
}
