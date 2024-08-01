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

import org.apache.paimon.data.Segments;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;

import static org.apache.paimon.CoreOptions.PAGE_SIZE;

/** Cache {@link Segments}. */
public class SegmentsCache<T> {

    private static final int OBJECT_MEMORY_SIZE = 1000;

    private final int pageSize;
    private final Cache<T, Segments> cache;
    private final long maxElementSize;

    public SegmentsCache(int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        this.pageSize = pageSize;
        this.cache =
                Caffeine.newBuilder()
                        .softValues()
                        .weigher(this::weigh)
                        .maximumWeight(maxMemorySize.getBytes())
                        .executor(Runnable::run)
                        .build();
        this.maxElementSize = maxElementSize;
    }

    public int pageSize() {
        return pageSize;
    }

    public long maxElementSize() {
        return maxElementSize;
    }

    @Nullable
    public Segments getIfPresents(T key) {
        return cache.getIfPresent(key);
    }

    public void put(T key, Segments segments) {
        cache.put(key, segments);
    }

    private int weigh(T cacheKey, Segments segments) {
        return OBJECT_MEMORY_SIZE + segments.segments().size() * pageSize;
    }

    @Nullable
    public static <T> SegmentsCache<T> create(MemorySize maxMemorySize, long maxElementSize) {
        return create((int) PAGE_SIZE.defaultValue().getBytes(), maxMemorySize, maxElementSize);
    }

    @Nullable
    public static <T> SegmentsCache<T> create(
            int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        if (maxMemorySize.getBytes() == 0) {
            return null;
        }

        return new SegmentsCache<>(pageSize, maxMemorySize, maxElementSize);
    }
}
