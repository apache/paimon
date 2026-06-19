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
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            Map<String, String> options) {
        this(table, null, filter, limit, vectorColumn, vectors, options);
    }

    public BatchVectorReadImpl(
            FileStoreTable table,
            PartitionPredicate partitionFilter,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, options);
        this.vectors = vectors;
    }

    @Override
    public List<GlobalIndexResult> readBatch(List<VectorSearchSplit> splits) {
        return readBatch(splits, null);
    }

    @Override
    public List<GlobalIndexResult> readBatch(VectorScan.Plan plan, @Nullable Long nextRowId) {
        return readBatch(plan.splits(), nextRowId);
    }

    private List<GlobalIndexResult> readBatch(
            List<VectorSearchSplit> splits, @Nullable Long nextRowId) {
        int n = vectors.length;
        if (splits.isEmpty() && !rawSearchEnabled()) {
            List<GlobalIndexResult> empty = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                empty.add(GlobalIndexResult.createEmpty());
            }
            return empty;
        }

        GlobalIndexer globalIndexer = splits.isEmpty() ? null : createGlobalIndexer(splits);
        ScoredGlobalIndexResult[] indexedResults =
                splits.isEmpty() ? emptyScoredResults(n) : readIndexedBatch(splits, globalIndexer);

        List<GlobalIndexResult> results = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            results.add(
                    withRawSearch(indexedResults[i], splits, globalIndexer, nextRowId, vectors[i]));
        }
        return results;
    }

    protected ScoredGlobalIndexResult[] readIndexedBatch(
            List<VectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        int n = vectors.length;
        RoaringNavigableMap64 preFilter = preFilter(splits).orElse(null);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        List<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> futures =
                new ArrayList<>(splits.size());
        for (VectorSearchSplit split : splits) {
            futures.add(
                    evalBatch(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vectors,
                            preFilter,
                            executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[n];
        for (int i = 0; i < n; i++) {
            merged[i] = ScoredGlobalIndexResult.createEmpty();
        }

        for (CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> future : futures) {
            List<Optional<ScoredGlobalIndexResult>> splitResults = future.join();
            for (int i = 0; i < n; i++) {
                if (splitResults.get(i).isPresent()) {
                    merged[i] = merged[i].or(splitResults.get(i).get());
                }
            }
        }
        return merged;
    }
}
