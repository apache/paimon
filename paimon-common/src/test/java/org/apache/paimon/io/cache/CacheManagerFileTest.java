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

import org.apache.paimon.fs.Path;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.MemorySize;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the shared cache's file index and concurrent file invalidation. */
@Timeout(30)
class CacheManagerFileTest {

    @ParameterizedTest
    @ValueSource(longs = {0, 1})
    void testRejectedPagesLeaveNoFileIndex(long capacity) throws Exception {
        try (CacheManager manager = new CacheManager(MemorySize.ofBytes(capacity), 0)) {
            for (int i = 0; i < 100; i++) {
                CacheKey key = key(new Path("file-" + i));
                assertThat(manager.getPage(key, ignored -> new byte[] {1, 2}).getHeapMemory())
                        .containsExactly(1, 2);
                assertThat(manager.dataCache().asMap()).isEmpty();
                assertThat(manager.cachedFileCount()).isZero();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDecodedRangeChargesItsBackingAllocation(boolean offHeap) throws Exception {
        byte[] encoded = new byte[] {99, 1, 2, 3, 4, 99};
        Path file = new Path("decoded-file");
        CacheKey key = CacheKey.forPosition(file, 0, encoded.length, false);
        try (CacheManager manager =
                offHeap
                        ? CacheManager.createOffHeap(MemorySize.ofBytes(5), 0)
                        : new CacheManager(MemorySize.ofBytes(5), 0)) {
            MemorySlice decoded =
                    manager.getPageSlice(
                            key, ignored -> encoded, bytes -> MemorySlice.wrap(bytes).slice(1, 4));
            assertThat(decoded.copyBytes()).containsExactly(1, 2, 3, 4);
            if (offHeap) {
                assertThat(decoded.segment().isOffHeap()).isTrue();
                assertThat(decoded.segment().size()).isEqualTo(4);
            } else {
                assertThat(decoded.segment().getHeapMemory()).isSameAs(encoded);
            }
            // Heap retains all six bytes; off-heap copies only the four-byte decoded range.
            assertThat(manager.contains(key)).isEqualTo(offHeap);
            manager.invalidFile(file);
            assertThat(manager.cachedFileCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWholePageAccessHonorsCachedDecodedRange(boolean offHeap) throws Exception {
        CacheKey key = key(new Path("decoded-file"));
        try (CacheManager manager =
                offHeap
                        ? CacheManager.createOffHeap(MemorySize.ofKibiBytes(64), 0)
                        : new CacheManager(MemorySize.ofKibiBytes(64), 0)) {
            manager.getPageSlice(
                    key,
                    ignored -> new byte[] {99, 1, 2, 99},
                    bytes -> MemorySlice.wrap(bytes).slice(1, 2));
            MemorySegment page =
                    manager.getPage(
                            key,
                            ignored -> {
                                throw new AssertionError("Decoded page should be cached");
                            });
            assertThat(manager.getPageSliceIfPresent(key).copyBytes()).containsExactly(1, 2);
            MemorySegment cached = manager.getPageIfPresent(key);
            byte[] cachedBytes = new byte[cached.size()];
            cached.get(0, cachedBytes);
            assertThat(cachedBytes).containsExactly(1, 2);
            assertThat(cached.isOffHeap()).isEqualTo(offHeap);
            byte[] bytes = new byte[page.size()];
            page.get(0, bytes);
            assertThat(bytes).containsExactly(1, 2);
            assertThat(page.isOffHeap()).isEqualTo(offHeap);
            manager.invalidPage(key);
            assertThat(manager.getPageIfPresent(key)).isNull();
            assertThat(manager.getPageSliceIfPresent(key)).isNull();
        }
    }

    @Test
    void testPresentPagesUseTheirOwnPriorityPool() throws Exception {
        Path file = new Path("priority-file");
        CacheKey dataKey = CacheKey.forPosition(file, 0, 2, false);
        CacheKey indexKey = CacheKey.forPosition(file, 0, 2, true);
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0.5)) {
            assertThat(manager.getPageIfPresent(dataKey)).isNull();
            assertThat(manager.getPageSliceIfPresent(indexKey)).isNull();
            manager.getPage(dataKey, ignored -> new byte[] {1, 2});
            manager.getPage(indexKey, ignored -> new byte[] {3, 4});
            assertThat(manager.getPageIfPresent(dataKey).getHeapMemory()).containsExactly(1, 2);
            assertThat(manager.getPageSliceIfPresent(indexKey).copyBytes()).containsExactly(3, 4);
            manager.invalidFile(file);
            assertThat(manager.getPageIfPresent(dataKey)).isNull();
            assertThat(manager.getPageSliceIfPresent(indexKey)).isNull();
        }
    }

    @Test
    void testEvictionRemovesEmptyFiles() throws Exception {
        try (CacheManager manager = new CacheManager(MemorySize.ofBytes(16), 0)) {
            for (int i = 0; i < 100; i++) {
                manager.getPage(key(new Path("file-" + i)), ignored -> new byte[8]);
                // Each file has one page. No file index may outlive its cached page.
                assertThat(manager.cachedFileCount()).isEqualTo(manager.dataCache().asMap().size());
            }
            manager.close();
            assertThat(manager.cachedFileCount()).isZero();
        }
    }

    @Test
    void testRemovingMiddleAndTailPagesKeepsFileIndexAccurate() throws Exception {
        List<CacheKey> invalidated = new ArrayList<>();
        Path file = new Path("linked-pages");
        CacheKey tail = CacheKey.forPosition(file, 0, 2, false);
        CacheKey middle = CacheKey.forPosition(file, 2, 2, false);
        CacheKey head = CacheKey.forPosition(file, 4, 2, false);
        CacheKey other = key(new Path("other-file"));
        try (CacheManager manager =
                new CacheManager(MemorySize.ofKibiBytes(64), 0) {
                    @Override
                    protected void invalidPage(CacheKey key, Cache.CacheValue expected) {
                        invalidated.add(key);
                        super.invalidPage(key, expected);
                    }
                }) {
            for (CacheKey key : new CacheKey[] {tail, middle, head, other}) {
                manager.getPage(key, ignored -> new byte[2]);
            }
            manager.invalidPage(middle);
            manager.invalidPage(tail);
            manager.invalidFile(file);
            assertThat(invalidated).containsExactly(head);
            assertThat(manager.cachedFileCount()).isOne();
            assertThat(manager.contains(other)).isTrue();
            manager.invalidFile(new Path("other-file"));
            assertThat(manager.cachedFileCount()).isZero();
        }
    }

    @Test
    void testFailedLoadCanRetryWithoutLeavingFileIndex() throws Exception {
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0)) {
            Path file = new Path("retry-file");
            CacheKey key = key(file);
            assertThatThrownBy(
                            () ->
                                    manager.getPage(
                                            key,
                                            ignored -> {
                                                throw new IOException("read failed");
                                            }))
                    .hasRootCauseMessage("read failed");
            assertThat(manager.cachedFileCount()).isZero();
            assertThat(manager.getPage(key, ignored -> new byte[] {7, 8}).getHeapMemory())
                    .containsExactly(7, 8);
            manager.invalidFile(file);
            assertThat(manager.cachedFileCount()).isZero();
            assertThat(manager.contains(key)).isFalse();
        }
    }

    @Test
    void testOldInvalidationDoesNotRemoveReplacement() throws Exception {
        Path file = new Path("shared-file");
        CacheKey key = key(file);
        CountDownLatch detached = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicBoolean pause = new AtomicBoolean(true);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (CacheManager manager =
                new CacheManager(MemorySize.ofKibiBytes(64), 0) {
                    @Override
                    protected void invalidPage(CacheKey k, Cache.CacheValue expected) {
                        if (pause.compareAndSet(true, false)) {
                            detached.countDown();
                            await(resume);
                        }
                        super.invalidPage(k, expected);
                    }
                }) {
            manager.getPage(key, ignored -> new byte[] {1, 2});
            Future<?> invalidation =
                    executor.submit(
                            () -> {
                                manager.invalidFile(file);
                                return null;
                            });
            try {
                assertThat(detached.await(10, TimeUnit.SECONDS)).isTrue();
                manager.invalidPage(key);
                MemorySegment replacement = manager.getPage(key, ignored -> new byte[] {3, 4});
                resume.countDown();
                invalidation.get(10, TimeUnit.SECONDS);

                assertThat(
                                manager.getPage(
                                        key,
                                        ignored -> {
                                            throw new AssertionError("Replacement was invalidated");
                                        }))
                        .isSameAs(replacement);
                assertThat(manager.cachedFileCount()).isOne();
                manager.invalidFile(file);
                assertThat(manager.contains(key)).isFalse();
                assertThat(manager.cachedFileCount()).isZero();
            } finally {
                resume.countDown();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testConcurrentLoadMayRepopulateInvalidatedFile() throws Exception {
        Path file = new Path("loading-file");
        CacheKey key = key(file);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0)) {
            Future<MemorySegment> load =
                    executor.submit(
                            () ->
                                    manager.getPage(
                                            key,
                                            ignored -> {
                                                loading.countDown();
                                                await(resume);
                                                return new byte[] {5, 6};
                                            }));
            try {
                assertThat(loading.await(10, TimeUnit.SECONDS)).isTrue();
                manager.invalidFile(file);
                resume.countDown();
                assertThat(load.get(10, TimeUnit.SECONDS).getHeapMemory()).containsExactly(5, 6);
                assertThat(manager.contains(key)).isTrue();
                manager.invalidFile(file);
                assertThat(manager.cachedFileCount()).isZero();
            } finally {
                resume.countDown();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testDifferentFilesLoadConcurrently() throws Exception {
        CyclicBarrier loading = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (CacheManager manager = new CacheManager(MemorySize.ofKibiBytes(64), 0)) {
            CacheReader loader =
                    ignored -> {
                        try {
                            loading.await(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new IOException(e);
                        }
                        return new byte[] {9, 10};
                    };
            Future<MemorySegment> first =
                    executor.submit(() -> manager.getPage(key(new Path("first")), loader));
            Future<MemorySegment> second =
                    executor.submit(() -> manager.getPage(key(new Path("second")), loader));
            assertThat(first.get(15, TimeUnit.SECONDS).getHeapMemory()).containsExactly(9, 10);
            assertThat(second.get(15, TimeUnit.SECONDS).getHeapMemory()).containsExactly(9, 10);
        } finally {
            executor.shutdownNow();
        }
    }

    private static CacheKey key(Path file) {
        return CacheKey.forPosition(file, 0, 2, false);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for cache operation");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
