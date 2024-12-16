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
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Cache manager to cache bytes to paged {@link MemorySegment}s. */
public class CacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(CacheManager.class);

    /**
     * Refreshing the cache comes with some costs, so not every time we visit the CacheManager, but
     * every 10 visits, refresh the LRU strategy.
     */
    public static final int REFRESH_COUNT = 10;

    private final Cache dataCache;
    private final Cache indexCache;

    private int fileReadCount;

    @VisibleForTesting
    public CacheManager(MemorySize maxMemorySize) {
        this(Cache.CacheType.GUAVA, maxMemorySize, 0);
    }

    public CacheManager(MemorySize dataMaxMemorySize, double highPriorityPoolRatio) {
        this(Cache.CacheType.GUAVA, dataMaxMemorySize, highPriorityPoolRatio);
    }

    public CacheManager(
            Cache.CacheType cacheType, MemorySize maxMemorySize, double highPriorityPoolRatio) {
        Preconditions.checkArgument(
                highPriorityPoolRatio >= 0 && highPriorityPoolRatio < 1,
                "The high priority pool ratio should in the range [0, 1).");
        MemorySize indexCacheSize =
                MemorySize.ofBytes((long) (maxMemorySize.getBytes() * highPriorityPoolRatio));
        MemorySize dataCacheSize =
                MemorySize.ofBytes((long) (maxMemorySize.getBytes() * (1 - highPriorityPoolRatio)));
        this.dataCache = CacheBuilder.newBuilder(cacheType).maximumWeight(dataCacheSize).build();
        if (highPriorityPoolRatio == 0) {
            this.indexCache = dataCache;
        } else {
            this.indexCache =
                    CacheBuilder.newBuilder(cacheType).maximumWeight(indexCacheSize).build();
        }
        this.fileReadCount = 0;
        LOG.info(
                "Initialize cache manager with data cache of {} and index cache of {}.",
                dataCacheSize,
                indexCacheSize);
    }

    @VisibleForTesting
    public Cache dataCache() {
        return dataCache;
    }

    @VisibleForTesting
    public Cache indexCache() {
        return indexCache;
    }

    public MemorySegment getPage(CacheKey key, CacheReader reader, CacheCallback callback) {
        Cache cache = key.isIndex() ? indexCache : dataCache;
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
        if (key.isIndex()) {
            indexCache.invalidate(key);
        } else {
            dataCache.invalidate(key);
        }
    }

    public int fileReadCount() {
        return fileReadCount;
    }

    /** The container for the segment. */
    public static class SegmentContainer {

        private final MemorySegment segment;

        private int accessCount;

        public SegmentContainer(MemorySegment segment) {
            this.segment = segment;
            this.accessCount = 0;
        }

        public MemorySegment access() {
            this.accessCount++;
            return segment;
        }

        public int getAccessCount() {
            return accessCount;
        }
    }
}
