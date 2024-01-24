/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.utils;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;

import java.io.RandomAccessFile;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util to apply a built bloom filter . */
public class FileBasedBloomFilter {

    private static final int FORCE_REFRESH_CACHE = 30;

    private final RandomAccessFile file;
    private final CacheManager cacheManager;
    private final BloomFilter filter;
    private final long readOffset;
    private final int readLength;

    private int refreshCount;

    public FileBasedBloomFilter(
            RandomAccessFile file,
            CacheManager cacheManager,
            long numRecords,
            long readOffset,
            int readLength) {
        this.file = file;
        this.cacheManager = cacheManager;
        checkArgument(numRecords >= 0);
        this.filter = new BloomFilter(numRecords, readLength);
        this.readOffset = readOffset;
        this.readLength = readLength;
        this.refreshCount = 0;
    }

    public boolean testHash(int hash) {
        refreshCount++;
        // we should refresh cache in LRU, but we cannot refresh everytime, it is costly.
        // so we introduce a refresh count to reduce refresh
        if (refreshCount == FORCE_REFRESH_CACHE || filter.getMemorySegment() == null) {
            MemorySegment segment =
                    cacheManager.getPage(
                            file,
                            readOffset,
                            readLength,
                            (position, length) -> filter.unsetMemorySegment());
            filter.setMemorySegment(segment, 0);
            refreshCount = 0;
        }
        return filter.testHash(hash);
    }

    @VisibleForTesting
    BloomFilter bloomFilter() {
        return filter;
    }
}
