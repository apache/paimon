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

import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.MaxSimScorer;
import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorIndexUtils;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IntArrayList;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/** Implementation for {@link BatchVectorRead}. */
public class DataEvolutionBatchVectorRead extends AbstractDataEvolutionVectorRead
        implements BatchVectorRead {

    private static final long serialVersionUID = 1L;

    protected final float[][] vectors;

    public DataEvolutionBatchVectorRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, options);
        this.vectors = vectors;
        if (vectorColumn != null && VectorIndexUtils.isMultiVector(vectorColumn.type())) {
            VectorIndexUtils.validateQuery((ArrayType) vectorColumn.type(), vectors);
        }
    }

    @Override
    public List<GlobalIndexResult> readBatch(VectorScan.Plan plan) {
        this.planSnapshot = plan.snapshot();
        return readBatch(plan.splits());
    }

    private List<GlobalIndexResult> readBatch(List<VectorSearchSplit> splits) {
        int n = vectors.length;
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        prepareSplits(splits, indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            List<GlobalIndexResult> empty = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                empty.add(GlobalIndexResult.createEmpty());
            }
            return empty;
        }

        RowRangeIndex overlaps = overlappingRanges(indexSplits, rawRowRanges(rawSplits));
        RoaringNavigableMap64[] indexedRows = overlapBitmaps(n, overlaps);
        List<ScoredGlobalIndexResult[]> byType = new ArrayList<>();
        GlobalIndexer rawIndexer = null;
        for (List<IndexVectorSearchSplit> group : groupByIndexType(indexSplits)) {
            GlobalIndexer indexer = createGlobalIndexer(group);
            byType.add(readIndexedBatch(group, indexer, overlaps, indexedRows));
            if (rawIndexer == null) {
                rawIndexer = indexer;
            }
        }

        List<GlobalIndexResult> results = new ArrayList<>(n);
        RoaringNavigableMap64 rawPreFilter = rawPreFilter(rawSplits);
        for (int i = 0; i < n; i++) {
            List<ScoredGlobalIndexResult> partial = new ArrayList<>(byType.size());
            for (ScoredGlobalIndexResult[] group : byType) {
                partial.add(group[i]);
            }
            results.add(
                    withRawSearch(
                            ScoredGlobalIndexResult.merge(partial),
                            rawSplits,
                            rawIndexer,
                            rawPreFilter,
                            vectors[i],
                            indexedRows[i]));
        }
        return results;
    }

    private ScoredGlobalIndexResult[] readIndexedBatch(
            List<IndexVectorSearchSplit> splits,
            GlobalIndexer globalIndexer,
            RowRangeIndex overlaps,
            RoaringNavigableMap64[] indexedRows) {
        int n = vectors.length;
        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = vectorIndexType(splits);
        int searchLimit = indexedSearchLimit(indexType);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        boolean refine = requiresRefinement(indexType);
        // A refined group contributes only the rows that survive reranking. Until then its
        // duplicate tracking is independent of previous groups.
        RoaringNavigableMap64[] seen = refine ? overlapBitmaps(n, overlaps) : indexedRows;
        List<PriorityQueue<long[]>> heaps = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            heaps.add(new PriorityQueue<>(WEAKEST_SCORE_FIRST));
        }
        // Join in split order to preserve duplicate precedence. Limit the number of outstanding
        // reads so a slow split cannot pin every later split's result in completed futures.
        Deque<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> pending =
                new ArrayDeque<>();
        int submitted = 0;
        for (int consumed = 0; consumed < splits.size(); consumed++) {
            while (submitted < splits.size() && pending.size() < parallelism) {
                IndexVectorSearchSplit split = splits.get(submitted);
                pending.addLast(
                        evalBatch(
                                globalIndexer,
                                indexPathFactory,
                                split.rowRangeStart(),
                                split.rowRangeEnd(),
                                split.vectorIndexFiles(),
                                vectors,
                                searchLimit,
                                preFilters.isEmpty() ? null : preFilters.get(submitted),
                                executor));
                submitted++;
            }
            IndexVectorSearchSplit split = splits.get(consumed);
            boolean mayOverlap = overlaps.intersects(split.rowRangeStart(), split.rowRangeEnd());
            List<Optional<ScoredGlobalIndexResult>> batch = pending.removeFirst().join();
            for (int i = 0; i < n; i++) {
                if (batch.get(i).isPresent()) {
                    ScoredGlobalIndexResult result = batch.get(i).get();
                    ScoreGetter scores = result.scoreGetter();
                    for (long row : result.results()) {
                        if (mayOverlap && overlaps.intersects(row, row)) {
                            if (seen[i].contains(row)) {
                                continue;
                            }
                            seen[i].add(row);
                        }
                        offerScore(heaps.get(i), searchLimit, row, scores.score(row));
                    }
                }
            }
        }

        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            merged[i] = scoredResult(heaps.get(i));
        }
        if (refine) {
            merged = maybeRerankIndexedBatchResults(merged, indexType, globalIndexer);
            for (int i = 0; i < n && !overlaps.ranges().isEmpty(); i++) {
                RoaringNavigableMap64 rows = merged[i].results();
                rows.andNot(indexedRows[i]);
                for (long row : rows) {
                    if (overlaps.intersects(row, row)) {
                        indexedRows[i].add(row);
                    }
                }
            }
        }
        return merged;
    }

    private static RoaringNavigableMap64[] overlapBitmaps(int count, RowRangeIndex overlaps) {
        RoaringNavigableMap64[] seen = new RoaringNavigableMap64[count];
        if (!overlaps.ranges().isEmpty()) {
            for (int i = 0; i < count; i++) {
                seen[i] = new RoaringNavigableMap64();
            }
        }
        return seen;
    }

    private static RowRangeIndex overlappingRanges(
            List<IndexVectorSearchSplit> splits, List<Range> rawRanges) {
        List<Range> ranges = new ArrayList<>(rawRanges);
        for (IndexVectorSearchSplit split : splits) {
            ranges.add(new Range(split.rowRangeStart(), split.rowRangeEnd()));
        }
        ranges.sort(Comparator.comparingLong(range -> range.from));
        List<Range> overlaps = new ArrayList<>();
        if (!ranges.isEmpty()) {
            long end = ranges.get(0).to;
            for (int i = 1; i < ranges.size(); i++) {
                Range next = ranges.get(i);
                if (next.from <= end) {
                    overlaps.add(new Range(next.from, Math.min(end, next.to)));
                }
                end = Math.max(end, next.to);
            }
        }
        return RowRangeIndex.create(overlaps);
    }

    protected ScoredGlobalIndexResult[] maybeRerankIndexedBatchResults(
            ScoredGlobalIndexResult[] results, String indexType, GlobalIndexer globalIndexer) {
        if (!requiresRefinement(indexType)) {
            return results;
        }

        int n = results.length;
        int searchLimit = indexedSearchLimit(indexType);
        ScoredGlobalIndexResult[] candidates = new ScoredGlobalIndexResult[n];
        RoaringNavigableMap64 unionCandidates = new RoaringNavigableMap64();
        for (int i = 0; i < n; i++) {
            candidates[i] = results[i].topK(searchLimit);
            unionCandidates.or(candidates[i].results());
        }

        if (unionCandidates.isEmpty()) {
            return candidates;
        }

        if (VectorIndexUtils.isMultiVector(vectorColumn.type())) {
            return rerankMultiVectors(candidates, unionCandidates);
        }
        Map<Long, float[]> rawVectors = readRawVectors(unionCandidates, false);
        String metric = rawSearchMetric(globalIndexer);
        ScoredGlobalIndexResult[] reranked = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            reranked[i] =
                    scoreRawVectors(candidates[i].results(), rawVectors, vectors[i], metric, limit);
        }
        return reranked;
    }

    private boolean requiresRefinement(String indexType) {
        return VectorIndexUtils.isMultiVector(vectorColumn.type())
                || configuredRefineFactor(indexType) != 0;
    }

    private ScoredGlobalIndexResult[] rerankMultiVectors(
            ScoredGlobalIndexResult[] candidates, RoaringNavigableMap64 union) {
        Map<Long, IntArrayList> queriesByRow = new HashMap<>();
        List<PriorityQueue<long[]>> heaps = new ArrayList<>(vectors.length);
        for (int i = 0; i < vectors.length; i++) {
            heaps.add(new PriorityQueue<>(WEAKEST_SCORE_FIRST));
            for (long rowId : candidates[i].results()) {
                queriesByRow.computeIfAbsent(rowId, ignored -> new IntArrayList(1)).add(i);
            }
        }
        MaxSimScorer scorer = new MaxSimScorer((ArrayType) vectorColumn.type(), vectors);
        double[] scores = new double[vectors.length];
        readRawVectorRows(
                union.toRangeList(),
                union,
                false,
                (rowId, row, vectorIndex) -> {
                    IntArrayList queries = queriesByRow.get(rowId);
                    scorer.bestMatches(row.getArray(vectorIndex), queries, scores);
                    for (int i = 0; i < queries.size(); i++) {
                        if (scores[i] != Double.NEGATIVE_INFINITY) {
                            offerScore(heaps.get(queries.get(i)), limit, rowId, (float) scores[i]);
                        }
                    }
                });
        for (int i = 0; i < vectors.length; i++) {
            candidates[i] = scoredResult(heaps.get(i));
        }
        return candidates;
    }
}
