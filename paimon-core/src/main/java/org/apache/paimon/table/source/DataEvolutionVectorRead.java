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

/** Data-evolution implementation for {@link VectorRead}. */
public class DataEvolutionVectorRead extends AbstractVectorRead implements VectorRead {

    private static final long serialVersionUID = 1L;

    protected final float[] vector;

    public DataEvolutionVectorRead(
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
        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = vectorIndexType(splits);
        int searchLimit = indexedSearchLimit(indexType);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            futures.add(
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vector,
                            searchLimit,
                            preFilters.isEmpty() ? null : preFilters.get(i),
                            executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ScoredGlobalIndexResult merged = ScoredGlobalIndexResult.createEmpty();
        for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future : futures) {
            Optional<ScoredGlobalIndexResult> splitResult = future.join();
            if (splitResult.isPresent()) {
                merged = merged.or(splitResult.get());
            }
        }
        return maybeRerankIndexedResult(merged, indexType, globalIndexer, vector);
    }
}
