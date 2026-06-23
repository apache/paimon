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
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/** Implementation for {@link BatchVectorRead}. */
public class BatchVectorReadImpl extends AbstractVectorRead implements BatchVectorRead {

    private static final long serialVersionUID = 1L;

    protected final float[][] vectors;

    public BatchVectorReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, options);
        this.vectors = vectors;
    }

    @Override
    public List<GlobalIndexResult> readBatch(VectorScan.Plan plan) {
        return readBatch(plan.splits());
    }

    private List<GlobalIndexResult> readBatch(List<VectorSearchSplit> splits) {
        int n = vectors.length;
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        splitSearchSplits(splits, indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            List<GlobalIndexResult> empty = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                empty.add(GlobalIndexResult.createEmpty());
            }
            return empty;
        }

        GlobalIndexer globalIndexer =
                indexSplits.isEmpty() ? null : createGlobalIndexer(indexSplits);
        ScoredGlobalIndexResult[] indexedResults =
                indexSplits.isEmpty()
                        ? emptyScoredResults(n)
                        : readIndexedBatch(indexSplits, globalIndexer);

        List<GlobalIndexResult> results = new ArrayList<>(n);
        RoaringNavigableMap64 rawPreFilter = rawPreFilter(rawSplits);
        for (int i = 0; i < n; i++) {
            results.add(
                    withRawSearch(
                            indexedResults[i], rawSplits, globalIndexer, rawPreFilter, vectors[i]));
        }
        return results;
    }

    protected ScoredGlobalIndexResult[] readIndexedBatch(
            List<IndexVectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        long start = System.currentTimeMillis();
        int n = vectors.length;
        debug(
                "PAIMON_BATCH_VECTOR_READ_INDEXED_BEGIN splits=%d queries=%d limit=%d",
                splits.size(), n, limit);
        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        debug(
                "PAIMON_BATCH_VECTOR_READ_PREFILTER_DONE splits=%d queries=%d includes=%s elapsedMs=%d",
                splits.size(), n, preFilterSummary(preFilters), System.currentTimeMillis() - start);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);
        debug(
                "PAIMON_BATCH_VECTOR_READ_EXECUTOR parallelism=%d executor=%s elapsedMs=%d",
                parallelism, executor, System.currentTimeMillis() - start);

        List<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> futures =
                new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            RoaringNavigableMap64 include = preFilters.isEmpty() ? null : preFilters.get(i);
            debug(
                    "PAIMON_BATCH_VECTOR_READ_SUBMIT_BEGIN split=%d range=%d-%d vectorFiles=%d include=%d elapsedMs=%d",
                    i,
                    split.rowRangeStart(),
                    split.rowRangeEnd(),
                    split.vectorIndexFiles().size(),
                    include == null ? -1 : include.getLongCardinality(),
                    System.currentTimeMillis() - start);
            futures.add(
                    evalBatch(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vectors,
                            include,
                            executor));
            debug(
                    "PAIMON_BATCH_VECTOR_READ_SUBMIT_DONE split=%d elapsedMs=%d",
                    i, System.currentTimeMillis() - start);
        }

        debug(
                "PAIMON_BATCH_VECTOR_READ_WAIT_BEGIN futures=%d elapsedMs=%d",
                futures.size(), System.currentTimeMillis() - start);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        debug(
                "PAIMON_BATCH_VECTOR_READ_WAIT_DONE futures=%d elapsedMs=%d",
                futures.size(), System.currentTimeMillis() - start);

        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            merged[i] = ScoredGlobalIndexResult.createEmpty();
        }

        for (int splitIndex = 0; splitIndex < futures.size(); splitIndex++) {
            List<Optional<ScoredGlobalIndexResult>> splitResults = futures.get(splitIndex).join();
            int present = 0;
            for (int i = 0; i < n; i++) {
                if (splitResults.get(i).isPresent()) {
                    present++;
                    merged[i] = merged[i].or(splitResults.get(i).get());
                }
            }
            debug(
                    "PAIMON_BATCH_VECTOR_READ_MERGE split=%d present=%d elapsedMs=%d",
                    splitIndex, present, System.currentTimeMillis() - start);
        }
        debug(
                "PAIMON_BATCH_VECTOR_READ_DONE splits=%d queries=%d elapsedMs=%d",
                splits.size(), n, System.currentTimeMillis() - start);
        return merged;
    }

    private static String preFilterSummary(List<RoaringNavigableMap64> preFilters) {
        if (preFilters.isEmpty()) {
            return "[]";
        }
        List<Long> counts = new ArrayList<>(preFilters.size());
        for (RoaringNavigableMap64 preFilter : preFilters) {
            counts.add(preFilter.getLongCardinality());
        }
        return counts.toString();
    }
}
