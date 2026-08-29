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

package org.apache.paimon.globalindex.fmindex;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/** Shared bounded LRU and read-concurrency guard for FM blocks. */
final class FMIndexReadContext {

    private static final int DEFAULT_MAX_CONCURRENT_FILE_READS = 8;
    // A full decoded quaternary rank block contains packed words plus four rank-prefix integers
    // per 64 words and one terminal prefix. It is larger than a bit-rank or sampled-value block.
    private static final long QUAD_BLOCK_WORD_BYTES = FMIndexFile.BLOCK_WORDS * Long.BYTES;
    private static final long QUAD_BLOCK_PREFIX_BYTES =
            ((FMIndexFile.BLOCK_WORDS + 63L) / 64L + 1L) * 4L * Integer.BYTES;
    // A locate must at least retain its largest decoded block; otherwise repeated access performs
    // another physical read every time.
    private static final long MIN_LOCATE_CACHE_BYTES =
            QUAD_BLOCK_WORD_BYTES + QUAD_BLOCK_PREFIX_BYTES;

    private final long cacheBudget;
    private final Semaphore fileReadPermits = new Semaphore(DEFAULT_MAX_CONCURRENT_FILE_READS);
    private final LinkedHashMap<BlockKey, CacheEntry> cache = new LinkedHashMap<>(32, 0.75f, true);
    private long cachedBytes;

    FMIndexReadContext(long cacheBudget) {
        this.cacheBudget = cacheBudget;
    }

    int effectiveDemandPageSize(int configuredPageSize) {
        return (int) Math.min(configuredPageSize, Math.min(cacheBudget, Integer.MAX_VALUE));
    }

    boolean supportsLocate() {
        return cacheBudget >= MIN_LOCATE_CACHE_BYTES;
    }

    @Nullable
    synchronized <T> T get(GlobalIndexIOMeta file, FMIndexFile.BlockInfo block, Class<T> type) {
        CacheEntry entry = cache.get(new BlockKey(file, block, type));
        return entry == null ? null : type.cast(entry.value);
    }

    synchronized void put(
            GlobalIndexIOMeta file,
            FMIndexFile.BlockInfo block,
            Class<?> type,
            Object value,
            int retainedBytes) {
        if (retainedBytes > cacheBudget) {
            return;
        }
        BlockKey key = new BlockKey(file, block, type);
        CacheEntry previous = cache.put(key, new CacheEntry(value, retainedBytes));
        if (previous != null) {
            cachedBytes -= previous.retainedBytes;
        }
        cachedBytes += retainedBytes;
        while (cachedBytes > cacheBudget) {
            Map.Entry<BlockKey, CacheEntry> eldest = cache.entrySet().iterator().next();
            cachedBytes -= eldest.getValue().retainedBytes;
            cache.remove(eldest.getKey());
        }
    }

    <T> T withFileReadPermit(Supplier<T> supplier) {
        boolean acquired = false;
        try {
            fileReadPermits.acquire();
            acquired = true;
            return supplier.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting to read an FM index file.", e);
        } finally {
            if (acquired) {
                fileReadPermits.release();
            }
        }
    }

    private static final class CacheEntry {
        private final Object value;
        private final int retainedBytes;

        private CacheEntry(Object value, int retainedBytes) {
            this.value = value;
            this.retainedBytes = retainedBytes;
        }
    }

    private static final class BlockKey {
        private final GlobalIndexIOMeta file;
        private final long offset;
        private final int storedLength;
        private final int uncompressedLength;
        private final int compressionId;
        private final int checksum;
        private final Class<?> type;

        private BlockKey(GlobalIndexIOMeta file, FMIndexFile.BlockInfo block, Class<?> type) {
            this.file = file;
            this.offset = block.offset;
            this.storedLength = block.storedLength;
            this.uncompressedLength = block.uncompressedLength;
            this.compressionId = block.compressionId;
            this.checksum = block.checksum;
            this.type = type;
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
            return offset == that.offset
                    && storedLength == that.storedLength
                    && uncompressedLength == that.uncompressedLength
                    && compressionId == that.compressionId
                    && checksum == that.checksum
                    && Objects.equals(file, that.file)
                    && Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    file, offset, storedLength, uncompressedLength, compressionId, checksum, type);
        }
    }
}
