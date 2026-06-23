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

/** Implementation for {@link VectorRead}. */
public class VectorReadImpl extends AbstractVectorRead implements VectorRead {

    private static final long serialVersionUID = 1L;

    protected final float[] vector;

    public VectorReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, options);
        this.vector = vector;
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        return readSplits(plan.splits());
    }

    protected GlobalIndexResult readSplits(List<? extends VectorSearchSplit> splits) {
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        splitSearchSplits(splits, indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        GlobalIndexer globalIndexer =
                indexSplits.isEmpty() ? null : createGlobalIndexer(indexSplits);
        ScoredGlobalIndexResult result =
                indexSplits.isEmpty()
                        ? ScoredGlobalIndexResult.createEmpty()
                        : readIndexed(indexSplits, globalIndexer);
        return withRawSearch(result, rawSplits, globalIndexer, rawPreFilter(rawSplits), vector);
    }

    protected ScoredGlobalIndexResult readIndexed(
            List<IndexVectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        long start = System.currentTimeMillis();
        debug("PAIMON_VECTOR_READ_INDEXED_BEGIN splits=%d", splits.size());
        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        debug(
                "PAIMON_VECTOR_READ_PREFILTER_DONE splits=%d preFilters=%d elapsedMs=%d",
                splits.size(), preFilters.size(), System.currentTimeMillis() - start);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);
        debug(
                "PAIMON_VECTOR_READ_EXECUTOR parallelism=%d executor=%s elapsedMs=%d",
                parallelism, executor, System.currentTimeMillis() - start);

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            RoaringNavigableMap64 include = preFilters.isEmpty() ? null : preFilters.get(i);
            debug(
                    "PAIMON_VECTOR_READ_SUBMIT_BEGIN index=%d range=%d-%d vectorFiles=%d include=%d elapsedMs=%d",
                    i,
                    split.rowRangeStart(),
                    split.rowRangeEnd(),
                    split.vectorIndexFiles().size(),
                    include == null ? -1 : include.getLongCardinality(),
                    System.currentTimeMillis() - start);
            CompletableFuture<Optional<ScoredGlobalIndexResult>> future =
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vector,
                            include,
                            executor);
            futures.add(future);
            debug(
                    "PAIMON_VECTOR_READ_SUBMIT_DONE index=%d futures=%d elapsedMs=%d",
                    i, futures.size(), System.currentTimeMillis() - start);
        }

        debug(
                "PAIMON_VECTOR_READ_WAIT_BEGIN futures=%d elapsedMs=%d",
                futures.size(), System.currentTimeMillis() - start);
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        debug(
                "PAIMON_VECTOR_READ_WAIT_DONE futures=%d elapsedMs=%d",
                futures.size(), System.currentTimeMillis() - start);

        ScoredGlobalIndexResult merged = ScoredGlobalIndexResult.createEmpty();
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<Optional<ScoredGlobalIndexResult>> future = futures.get(i);
            Optional<ScoredGlobalIndexResult> splitResult = future.join();
            debug(
                    "PAIMON_VECTOR_READ_MERGE index=%d present=%s elapsedMs=%d",
                    i, splitResult.isPresent(), System.currentTimeMillis() - start);
            if (splitResult.isPresent()) {
                merged = merged.or(splitResult.get());
            }
        }
        return merged;
    }
}
