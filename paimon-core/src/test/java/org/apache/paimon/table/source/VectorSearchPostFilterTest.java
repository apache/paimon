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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SemaphoredDelegatingExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VectorSearchPostFilterTest {
    private static RoaringNavigableMap64 ids(long... ids) {
        RoaringNavigableMap64 bitmap = new RoaringNavigableMap64();
        for (long id : ids) {
            bitmap.add(id);
        }
        return bitmap;
    }

    private static Optional<ScoredGlobalIndexResult> hit(long id) {
        return Optional.of(ScoredGlobalIndexResult.create(ids(id), ignored -> 1.0f));
    }

    @Test
    void candidateCountUsesOriginalTopKAndCapsWithoutOverflow() {
        assertThat(VectorSearchPostFilter.candidateLimit(2, 10, 6, 1000)).isEqualTo(20);
        assertThat(VectorSearchPostFilter.candidateLimit(2, 1, 6, 1000)).isEqualTo(6);
        assertThat(VectorSearchPostFilter.candidateLimit(2, 10, 6, 12)).isEqualTo(12);
        assertThat(
                        VectorSearchPostFilter.candidateLimit(
                                Integer.MAX_VALUE, Integer.MAX_VALUE, 1, Long.MAX_VALUE))
                .isEqualTo(Integer.MAX_VALUE);
        assertThatThrownBy(() -> VectorSearchPostFilter.candidateLimit(1, 0, 1, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void filtersTheBatchUnionOnceAndDetachesRetainedScores() throws Exception {
        Map<Long, Float> scores = new HashMap<>();
        scores.put(101L, 3.0f);
        scores.put(102L, 2.0f);
        scores.put(103L, 1.0f);
        Optional<ScoredGlobalIndexResult> candidate =
                Optional.of(ScoredGlobalIndexResult.create(ids(101, 102, 103), scores::get));
        AtomicInteger checks = new AtomicInteger();
        AtomicInteger searches = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        List<Optional<ScoredGlobalIndexResult>> result =
                VectorSearchPostFilter.search(
                                new float[][] {{0}, {1}},
                                10,
                                2,
                                new Range(100, 110),
                                ids(101, 102, 103),
                                vectors -> {
                                    searches.incrementAndGet();
                                    return CompletableFuture.completedFuture(
                                            Arrays.asList(candidate, candidate));
                                },
                                rows -> {
                                    assertThat(rows).containsExactly(101L, 102L, 103L);
                                    checks.incrementAndGet();
                                    return ids(102, 999);
                                },
                                closes::incrementAndGet)
                        .get(10, SECONDS);
        scores.clear();
        assertThat(searches.get()).isEqualTo(1);
        assertThat(checks.get()).isEqualTo(1);
        assertThat(closes.get()).isEqualTo(1);
        for (Optional<ScoredGlobalIndexResult> value : result) {
            assertThat(value.get().results()).containsExactly(102L);
            assertThat(value.get().scoreGetter().score(102)).isEqualTo(2.0f);
        }
    }

    @Test
    void chunksLargeBatchesButNeverRetriesQueriesOrDeadlocksOnOnePermit() throws Exception {
        ExecutorService thread = Executors.newSingleThreadExecutor();
        ExecutorService executor = new SemaphoredDelegatingExecutor(thread, 1, false);
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger searches = new AtomicInteger();
        float[][] vectors = {{0}, {1}, {2}, {3}, {4}, {5}};
        int[] seen = new int[6];
        try {
            List<Optional<ScoredGlobalIndexResult>> result =
                    VectorSearchPostFilter.search(
                                    vectors,
                                    200000,
                                    2,
                                    new Range(0, 1000000),
                                    null,
                                    batch ->
                                            CompletableFuture.supplyAsync(
                                                    () -> {
                                                        searches.incrementAndGet();
                                                        assertThat((long) batch.length * 200000)
                                                                .isLessThanOrEqualTo(1 << 20);
                                                        List<Optional<ScoredGlobalIndexResult>>
                                                                values = new ArrayList<>();
                                                        for (float[] vector : batch) {
                                                            int id = (int) vector[0];
                                                            assertThat(vector)
                                                                    .isSameAs(vectors[id]);
                                                            seen[id]++;
                                                            values.add(hit(id));
                                                        }
                                                        return values;
                                                    },
                                                    executor),
                                    rows -> new RoaringNavigableMap64(),
                                    closes::incrementAndGet)
                            .get(10, SECONDS);
            assertThat(searches.get()).isEqualTo(2);
            assertThat(seen).containsExactly(1, 1, 1, 1, 1, 1);
            assertThat(closes.get()).isEqualTo(1);
            assertThat(result).hasSize(6);
            for (Optional<ScoredGlobalIndexResult> value : result) {
                assertThat(value.get().results()).isEmpty();
            }
        } finally {
            thread.shutdownNow();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sync", "async", "scalar", "count", "range"})
    void errorsCompleteTheFutureAndCloseTheReader(String failure) {
        AtomicInteger closes = new AtomicInteger();
        CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> result =
                VectorSearchPostFilter.search(
                        new float[][] {{0}},
                        10,
                        2,
                        new Range(0, 100),
                        null,
                        vectors -> {
                            if (failure.equals("sync")) {
                                throw new IllegalStateException("sync");
                            }
                            if (failure.equals("async")) {
                                CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> failed =
                                        new CompletableFuture<>();
                                failed.completeExceptionally(new IllegalStateException("async"));
                                return failed;
                            }
                            return CompletableFuture.completedFuture(
                                    failure.equals("count")
                                            ? Collections.emptyList()
                                            : Collections.singletonList(
                                                    hit(failure.equals("range") ? 999 : 1)));
                        },
                        rows -> {
                            throw new IllegalStateException("scalar");
                        },
                        closes::incrementAndGet);
        assertThatThrownBy(() -> result.get(10, SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(closes.get()).isEqualTo(1);
    }

    @Test
    void cancellationWaitsForNativeCompletionBeforeClosingAndSkipsFurtherWork() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> nativeResult =
                new CompletableFuture<>();
        AtomicInteger searches = new AtomicInteger();
        CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> result =
                VectorSearchPostFilter.search(
                        new float[][] {{0}, {1}},
                        1 << 20,
                        2,
                        new Range(0, 10000000),
                        null,
                        vectors -> {
                            searches.incrementAndGet();
                            started.countDown();
                            return nativeResult;
                        },
                        rows -> {
                            throw new AssertionError("Cancelled query must not read scalar data");
                        },
                        closed::countDown);
        assertThat(started.await(10, SECONDS)).isTrue();
        assertThat(result.cancel(false)).isTrue();
        assertThat(closed.getCount()).isEqualTo(1);
        nativeResult.complete(Collections.singletonList(hit(0)));
        assertThat(closed.await(10, SECONDS)).isTrue();
        assertThat(searches.get()).isEqualTo(1);
    }
}
