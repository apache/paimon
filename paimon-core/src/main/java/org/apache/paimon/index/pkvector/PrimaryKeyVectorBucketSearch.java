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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.LongPredicate;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** ANN search plus search-mode-controlled exact fallback for one snapshot bucket. */
public class PrimaryKeyVectorBucketSearch {

    private static final Comparator<PkVectorSearchResult> BEST_FIRST =
            Comparator.comparingDouble(PkVectorSearchResult::distance)
                    .thenComparing(PkVectorSearchResult::dataFileName)
                    .thenComparingLong(PkVectorSearchResult::rowPosition);

    private final PkVectorDataFileReader.Factory vectorReaderFactory;
    @Nullable private final PkVectorAnnSegmentSearcher annSearcher;
    private final Map<String, String> searchOptions;
    private final String metric;
    private final GlobalIndexSearchMode searchMode;

    public PrimaryKeyVectorBucketSearch(
            PkVectorDataFileReader.Factory vectorReaderFactory,
            @Nullable PkVectorAnnSegmentSearcher annSearcher,
            Map<String, String> searchOptions,
            String metric,
            GlobalIndexSearchMode searchMode) {
        this.vectorReaderFactory = vectorReaderFactory;
        this.annSearcher = annSearcher;
        this.searchOptions = Collections.unmodifiableMap(new HashMap<>(searchOptions));
        this.metric = metric;
        this.searchMode = searchMode;
    }

    public List<PkVectorSearchResult> search(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            float[] query,
            int limit)
            throws IOException {
        Result result = search(state, activeFiles, deletionVectors, query, limit, limit);
        PriorityQueue<PkVectorSearchResult> nearest =
                new PriorityQueue<>(limit, BEST_FIRST.reversed());
        for (PkVectorSearchResult candidate : result.indexedCandidates) {
            add(nearest, candidate, limit);
        }
        for (PkVectorSearchResult candidate : result.exactCandidates) {
            add(nearest, candidate, limit);
        }
        return sorted(nearest);
    }

    public Result search(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            float[] query,
            int indexedLimit,
            int exactLimit)
            throws IOException {
        return search(
                state,
                activeFiles,
                deletionVectors,
                Collections.emptyMap(),
                query,
                indexedLimit,
                exactLimit);
    }

    public Result search(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            Map<String, List<Range>> rowRangesByFile,
            float[] query,
            int indexedLimit,
            int exactLimit)
            throws IOException {
        return join(
                searchAsync(
                        state,
                        activeFiles,
                        deletionVectors,
                        rowRangesByFile,
                        query,
                        indexedLimit,
                        exactLimit,
                        Runnable::run));
    }

    public CompletableFuture<Result> searchAsync(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            Map<String, List<Range>> rowRangesByFile,
            float[] query,
            int indexedLimit,
            int exactLimit,
            Executor executor) {
        return searchBatchAsync(
                        state,
                        activeFiles,
                        deletionVectors,
                        rowRangesByFile,
                        new float[][] {query},
                        indexedLimit,
                        exactLimit,
                        executor)
                .thenApply(results -> results.get(0));
    }

    public List<Result> searchBatch(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            float[][] queries,
            int indexedLimit,
            int exactLimit)
            throws IOException {
        return searchBatch(
                state,
                activeFiles,
                deletionVectors,
                Collections.emptyMap(),
                queries,
                indexedLimit,
                exactLimit);
    }

    public List<Result> searchBatch(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            Map<String, List<Range>> rowRangesByFile,
            float[][] queries,
            int indexedLimit,
            int exactLimit)
            throws IOException {
        return join(
                searchBatchAsync(
                        state,
                        activeFiles,
                        deletionVectors,
                        rowRangesByFile,
                        queries,
                        indexedLimit,
                        exactLimit,
                        Runnable::run));
    }

    public CompletableFuture<List<Result>> searchBatchAsync(
            PkVectorBucketIndexState state,
            List<DataFileMeta> activeFiles,
            Map<String, DeletionVector> deletionVectors,
            Map<String, List<Range>> rowRangesByFile,
            float[][] queries,
            int indexedLimit,
            int exactLimit,
            Executor executor) {
        checkArgument(queries != null && queries.length > 0, "Query vectors cannot be empty.");
        checkArgument(indexedLimit > 0, "Vector indexed search limit must be positive.");
        checkArgument(exactLimit > 0, "Vector exact search limit must be positive.");

        Map<String, DataFileMeta> filesByName = new HashMap<>();
        for (DataFileMeta file : activeFiles) {
            checkArgument(filesByName.put(file.fileName(), file) == null, "Duplicate data file.");
        }
        Set<String> activeSourceFiles = new HashSet<>(filesByName.keySet());
        Set<String> covered = new HashSet<>();
        List<CompletableFuture<List<List<PkVectorSearchResult>>>> indexedFutures =
                new ArrayList<>();
        for (IndexFileMeta ann : state.annSegments()) {
            PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(ann);
            for (PrimaryKeyIndexSourceFile source : sourceMeta.sourceFiles()) {
                DataFileMeta file = filesByName.get(source.fileName());
                if (file == null) {
                    continue;
                }
                checkArgument(
                        file.rowCount() == source.rowCount(),
                        "ANN source %s does not match the active data file.",
                        source.fileName());
                covered.add(source.fileName());
            }
            checkArgument(annSearcher != null, "ANN search is not configured.");
            if (queries.length == 1) {
                indexedFutures.add(
                        annSearcher
                                .searchAsync(
                                        ann,
                                        sourceMeta,
                                        queries[0],
                                        indexedLimit,
                                        deletionVectors,
                                        activeSourceFiles,
                                        rowRangesByFile,
                                        searchOptions)
                                .thenApply(Collections::singletonList));
            } else {
                indexedFutures.add(
                        annSearcher.searchBatchAsync(
                                ann,
                                sourceMeta,
                                queries,
                                indexedLimit,
                                deletionVectors,
                                activeSourceFiles,
                                rowRangesByFile,
                                searchOptions));
            }
        }

        List<CompletableFuture<List<List<PkVectorSearchResult>>>> exactFutures = new ArrayList<>();
        if (searchMode != GlobalIndexSearchMode.FAST) {
            for (DataFileMeta file : activeFiles) {
                if (covered.contains(file.fileName())) {
                    continue;
                }
                DeletionVector dv = deletionVectors.get(file.fileName());
                List<Range> rowRanges = rowRangesByFile.get(file.fileName());
                if (rowRanges != null && rowRanges.isEmpty()) {
                    continue;
                }
                LongPredicate excluded =
                        position ->
                                (dv != null && dv.isDeleted(position))
                                        || (rowRanges != null && !contains(rowRanges, position));
                exactFutures.add(
                        CompletableFuture.supplyAsync(
                                () -> exactSearch(file, queries, exactLimit, excluded), executor));
            }
        }
        List<CompletableFuture<?>> futures = new ArrayList<>();
        futures.addAll(indexedFutures);
        futures.addAll(exactFutures);
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            List<PriorityQueue<PkVectorSearchResult>> indexedNearest =
                                    queues(queries.length, indexedLimit);
                            for (CompletableFuture<List<List<PkVectorSearchResult>>> future :
                                    indexedFutures) {
                                List<List<PkVectorSearchResult>> batchResults = future.join();
                                checkArgument(
                                        batchResults.size() == queries.length,
                                        "ANN batch result count does not match query count.");
                                for (int i = 0; i < queries.length; i++) {
                                    for (PkVectorSearchResult result : batchResults.get(i)) {
                                        add(indexedNearest.get(i), result, indexedLimit);
                                    }
                                }
                            }
                            List<PriorityQueue<PkVectorSearchResult>> exactNearest =
                                    queues(queries.length, exactLimit);
                            for (CompletableFuture<List<List<PkVectorSearchResult>>> future :
                                    exactFutures) {
                                List<List<PkVectorSearchResult>> batchResults = future.join();
                                checkArgument(
                                        batchResults.size() == queries.length,
                                        "Exact batch result count does not match query count.");
                                for (int i = 0; i < queries.length; i++) {
                                    for (PkVectorSearchResult result : batchResults.get(i)) {
                                        add(exactNearest.get(i), result, exactLimit);
                                    }
                                }
                            }
                            List<Result> results = new ArrayList<>(queries.length);
                            for (int i = 0; i < queries.length; i++) {
                                results.add(
                                        new Result(
                                                sorted(indexedNearest.get(i)),
                                                sorted(exactNearest.get(i))));
                            }
                            return Collections.unmodifiableList(results);
                        });
    }

    private List<List<PkVectorSearchResult>> exactSearch(
            DataFileMeta file, float[][] queries, int limit, LongPredicate excluded) {
        try (PkVectorReader reader = vectorReaderFactory.create(file)) {
            return PkVectorExactSearcher.searchBatch(
                    file.fileName(), reader, queries, metric, limit, excluded);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    private static List<PriorityQueue<PkVectorSearchResult>> queues(int count, int limit) {
        List<PriorityQueue<PkVectorSearchResult>> queues = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            queues.add(new PriorityQueue<>(limit, BEST_FIRST.reversed()));
        }
        return queues;
    }

    private static <T> T join(CompletableFuture<T> future) throws IOException {
        try {
            return future.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }

    private static boolean contains(List<Range> ranges, long position) {
        int low = 0;
        int high = ranges.size() - 1;
        while (low <= high) {
            int middle = (low + high) >>> 1;
            Range range = ranges.get(middle);
            if (position < range.from) {
                high = middle - 1;
            } else if (position > range.to) {
                low = middle + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    private static List<PkVectorSearchResult> sorted(PriorityQueue<PkVectorSearchResult> nearest) {
        List<PkVectorSearchResult> result = new ArrayList<>(nearest);
        Collections.sort(result, BEST_FIRST);
        return Collections.unmodifiableList(result);
    }

    private static void add(
            PriorityQueue<PkVectorSearchResult> nearest,
            PkVectorSearchResult candidate,
            int limit) {
        if (nearest.size() < limit) {
            nearest.add(candidate);
        } else if (BEST_FIRST.compare(candidate, nearest.peek()) < 0) {
            nearest.poll();
            nearest.add(candidate);
        }
    }

    /** Separately bounded approximate-index and exact-fallback candidates. */
    public static class Result {

        private final List<PkVectorSearchResult> indexedCandidates;
        private final List<PkVectorSearchResult> exactCandidates;

        private Result(
                List<PkVectorSearchResult> indexedCandidates,
                List<PkVectorSearchResult> exactCandidates) {
            this.indexedCandidates = indexedCandidates;
            this.exactCandidates = exactCandidates;
        }

        public List<PkVectorSearchResult> indexedCandidates() {
            return indexedCandidates;
        }

        public List<PkVectorSearchResult> exactCandidates() {
            return exactCandidates;
        }
    }
}
