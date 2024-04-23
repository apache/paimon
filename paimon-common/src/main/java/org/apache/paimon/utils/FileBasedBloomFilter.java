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
import org.apache.paimon.io.PageFileInput;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.io.cache.CacheManager.REFRESH_COUNT;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util to apply a built bloom filter . */
public class FileBasedBloomFilter {

    private final PageFileInput input;
    private final CacheManager cacheManager;
    private final BloomFilter filter;
    private final long readOffset;
    private final int readLength;

    private int accessCount;

    public FileBasedBloomFilter(
            PageFileInput input,
            CacheManager cacheManager,
            long expectedEntries,
            long readOffset,
            int readLength) {
        this.input = input;
        this.cacheManager = cacheManager;
        checkArgument(expectedEntries >= 0);
        this.filter = new BloomFilter(expectedEntries, readLength);
        this.readOffset = readOffset;
        this.readLength = readLength;
        this.accessCount = 0;
    }

    public boolean testHash(int hash) {
        accessCount++;
        // we should refresh cache in LRU, but we cannot refresh everytime, it is costly.
        // so we introduce a refresh count to reduce refresh
        if (accessCount == REFRESH_COUNT || filter.getMemorySegment() == null) {
            MemorySegment segment =
                    cacheManager.getPage(
                            CacheKey.forPosition(input.file(), readOffset, readLength),
                            key -> input.readPosition(readOffset, readLength),
                            key -> filter.unsetMemorySegment());
            filter.setMemorySegment(segment, 0);
            accessCount = 0;
        }
        return filter.testHash(hash);
    }

    @VisibleForTesting
    BloomFilter bloomFilter() {
        return filter;
    }
}
