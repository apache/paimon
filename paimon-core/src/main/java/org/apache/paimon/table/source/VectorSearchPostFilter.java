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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** One ANN request per query followed by exact filtering on its shard's data. */
final class VectorSearchPostFilter {
    private static final int MAX_BATCH_CANDIDATES = 1 << 20;

    private VectorSearchPostFilter() {}

    static int candidateLimit(int topK, int factor, int required, long shardSize) {
        if (topK <= 0 || factor <= 0 || required <= 0 || shardSize <= 0) {
            throw new IllegalArgumentException("Invalid post-filter candidate limits");
        }
        return (int)
                Math.min(
                        Integer.MAX_VALUE,
                        Math.min(shardSize, Math.max((long) topK * factor, required)));
    }

    static CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> search(
            float[][] vectors,
            int candidates,
            int required,
            Range range,
            @Nullable RoaringNavigableMap64 include,
            Function<float[][], CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>>
                    retrieve,
            Function<RoaringNavigableMap64, RoaringNavigableMap64> check,
            Runnable close) {
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>(vectors.length);
        AtomicBoolean cancelled = new AtomicBoolean();
        int chunkSize = Math.max(1, MAX_BATCH_CANDIDATES / candidates);
        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
        for (int start = 0; start < vectors.length; start += chunkSize) {
            float[][] batch =
                    Arrays.copyOfRange(vectors, start, Math.min(vectors.length, start + chunkSize));
            // Do not submit from the native completion thread: it may still hold
            // the only permit of a SemaphoredDelegatingExecutor.
            chain =
                    chain.thenComposeAsync(
                            ignored -> {
                                if (cancelled.get()) {
                                    throw new CancellationException();
                                }
                                return retrieve.apply(batch)
                                        .thenAcceptAsync(
                                                values -> {
                                                    if (cancelled.get()) {
                                                        throw new CancellationException();
                                                    }
                                                    if (values.size() != batch.length) {
                                                        throw new IllegalStateException(
                                                                "Vector index returned the wrong query count");
                                                    }
                                                    results.addAll(
                                                            filter(
                                                                    values, required, range,
                                                                    include, check));
                                                });
                            });
        }
        // Expose only a downstream stage. Cancelling it cannot skip cleanup or
        // close the reader while an in-flight native request still uses it.
        CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> output =
                chain.whenComplete((ignored, failure) -> close.run()).thenApply(ignored -> results);
        output.whenComplete(
                (ignored, failure) -> {
                    if (output.isCancelled()) {
                        cancelled.set(true);
                    }
                });
        return output;
    }

    static List<Optional<ScoredGlobalIndexResult>> filter(
            List<Optional<ScoredGlobalIndexResult>> values,
            int required,
            Range range,
            @Nullable RoaringNavigableMap64 include,
            Function<RoaringNavigableMap64, RoaringNavigableMap64> check) {
        RoaringNavigableMap64 candidates = new RoaringNavigableMap64();
        for (Optional<ScoredGlobalIndexResult> value : values) {
            if (value.isPresent()) {
                for (long id : value.get().results()) {
                    if (id < range.from
                            || id > range.to
                            || (include != null && !include.contains(id))) {
                        throw new IllegalStateException("Vector candidate lies outside its source");
                    }
                    candidates.add(id);
                }
            }
        }
        RoaringNavigableMap64 matched =
                candidates.isEmpty()
                        ? candidates
                        : RoaringNavigableMap64.and(check.apply(candidates), candidates);
        List<Optional<ScoredGlobalIndexResult>> filtered = new ArrayList<>(values.size());
        for (Optional<ScoredGlobalIndexResult> value : values) {
            if (!value.isPresent()) {
                filtered.add(Optional.empty());
                continue;
            }
            ScoredGlobalIndexResult candidate = value.get();
            RoaringNavigableMap64 ids = RoaringNavigableMap64.and(candidate.results(), matched);
            ScoredGlobalIndexResult selected =
                    ScoredGlobalIndexResult.create(ids, candidate.scoreGetter()).topK(required);
            Map<Long, Float> scores = new HashMap<>();
            for (long id : selected.results()) {
                scores.put(id, selected.scoreGetter().score(id));
            }
            filtered.add(
                    Optional.of(ScoredGlobalIndexResult.create(selected.results(), scores::get)));
        }
        return filtered;
    }
}
