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

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Cache manager to cache bytes to paged {@link MemorySegment}s. */
public class CacheManager {

    /**
     * Refreshing the cache comes with some costs, so not every time we visit the CacheManager, but
     * every 10 visits, refresh the LRU strategy.
     */
    public static final int REFRESH_COUNT = 10;

    private final Cache cache;

    private int fileReadCount;

    public CacheManager(MemorySize maxMemorySize) {
        this(Cache.CacheType.CAFFEINE, maxMemorySize);
    }

    public CacheManager(Cache.CacheType cacheType, MemorySize maxMemorySize) {
        this.cache = CacheBuilder.newBuilder(cacheType).maximumWeight(maxMemorySize).build();
        this.fileReadCount = 0;
    }

    @VisibleForTesting
    public Cache cache() {
        return cache;
    }

    public MemorySegment getPage(CacheKey key, CacheReader reader, CacheCallback callback) {
        Cache.CacheValue value =
                cache.get(
                        key,
                        k -> {
                            this.fileReadCount++;
                            try {
                                return new Cache.CacheValue(
                                        MemorySegment.wrap(reader.read(key)), callback);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        return checkNotNull(value, String.format("Cache result for key(%s) is null", key)).segment;
    }

    public void invalidPage(CacheKey key) {
        cache.invalidate(key);
    }

    public int fileReadCount() {
        return fileReadCount;
    }
}
