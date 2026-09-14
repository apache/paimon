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

package org.apache.paimon.sst;

import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.cache.CacheKey;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for shared block ownership and file invalidation. */
class BlockCacheTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(30)
    void testConcurrentLoadAfterFastLookupMissIsReused(boolean slice) throws Exception {
        Path path = new Path("shared-file");
        CountDownLatch missed = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        AtomicInteger loads = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (CacheManager manager =
                        new CacheManager(MemorySize.ofKibiBytes(64), 0) {
                            @Override
                            public MemorySlice getPageSliceIfPresent(CacheKey key) {
                                MemorySlice cached = super.getPageSliceIfPresent(key);
                                if (cached == null && pause.compareAndSet(true, false)) {
                                    missed.countDown();
                                    try {
                                        if (!resume.await(10, TimeUnit.SECONDS)) {
                                            throw new AssertionError("Timed out after cache miss");
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new RuntimeException(e);
                                    }
                                }
                                return cached;
                            }
                        };
                BlockCache first = new BlockCache(path, input(), manager);
                BlockCache second = new BlockCache(path, input(), manager)) {
            Future<MemorySlice> pending = executor.submit(() -> read(first, slice, loads));
            try {
                assertThat(missed.await(10, TimeUnit.SECONDS)).isTrue();
                MemorySlice loaded = read(second, slice, loads);
                resume.countDown();
                MemorySlice reused = pending.get(10, TimeUnit.SECONDS);
                assertThat(reused.copyBytes()).containsExactly(1, 2, 3, 4);
                assertThat(reused.segment()).isSameAs(loaded.segment());
                assertThat(loads).hasValue(1);
            } finally {
                resume.countDown();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testSharedReaderReloadsInvalidatedBlock() throws Exception {
        Path path = new Path("shared-file");
        AtomicInteger loads = new AtomicInteger();
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0);
                BlockCache first = new BlockCache(path, input(), manager);
                BlockCache second = new BlockCache(path, input(), manager)) {
            MemorySegment original =
                    first.getBlock(
                            0,
                            4,
                            bytes -> {
                                loads.incrementAndGet();
                                return bytes;
                            },
                            false);
            assertThat(
                            second.getBlock(
                                    0,
                                    4,
                                    bytes -> {
                                        loads.incrementAndGet();
                                        return bytes;
                                    },
                                    false))
                    .isSameAs(original);
            manager.invalidPage(CacheKey.forPosition(path, 0, 4, false));

            MemorySegment reloaded =
                    second.getBlock(
                            0,
                            4,
                            bytes -> {
                                loads.incrementAndGet();
                                return bytes;
                            },
                            false);
            assertThat(reloaded.getHeapMemory()).containsExactly(1, 2, 3, 4);
            assertThat(loads).hasValue(2);
        }
    }

    @Test
    void testCloseInvalidatesAllReadersPagesForFile() throws Exception {
        Path path = new Path("shared-file");
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0.5);
                BlockCache first = new BlockCache(path, input(), manager);
                BlockCache second = new BlockCache(path, input(), manager);
                BlockCache other = new BlockCache(new Path("other-file"), input(), manager)) {
            first.getBlock(0, 4, bytes -> bytes, false);
            second.getBlock(4, 4, bytes -> bytes, true);
            MemorySegment otherBlock = other.getBlock(0, 4, bytes -> bytes, false);

            first.close();

            assertThat(manager.contains(CacheKey.forPosition(path, 0, 4, false))).isFalse();
            assertThat(manager.contains(CacheKey.forPosition(path, 4, 4, true))).isFalse();
            assertThat(
                            other.getBlock(
                                    0,
                                    4,
                                    bytes -> {
                                        throw new AssertionError("Unexpected reload");
                                    },
                                    false))
                    .isSameAs(otherBlock);
        }
    }

    private static MemorySlice read(BlockCache cache, boolean slice, AtomicInteger loads) {
        if (slice) {
            return cache.getBlockSlice(
                    0,
                    4,
                    bytes -> {
                        loads.incrementAndGet();
                        return MemorySlice.wrap(bytes);
                    },
                    false);
        }
        return MemorySlice.wrap(
                cache.getBlock(
                        0,
                        4,
                        bytes -> {
                            loads.incrementAndGet();
                            return bytes;
                        },
                        false));
    }

    private static ByteArraySeekableStream input() {
        return new ByteArraySeekableStream(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    }
}
