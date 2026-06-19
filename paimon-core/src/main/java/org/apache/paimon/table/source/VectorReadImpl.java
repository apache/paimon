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
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            Map<String, String> options) {
        this(table, null, filter, limit, vectorColumn, vector, options);
    }

    public VectorReadImpl(
            FileStoreTable table,
            PartitionPredicate partitionFilter,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, options);
        this.vector = vector;
    }

    @Override
    public GlobalIndexResult read(List<VectorSearchSplit> splits) {
        return read(splits, null);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan, @Nullable Long nextRowId) {
        return read(plan.splits(), nextRowId);
    }

    private GlobalIndexResult read(List<VectorSearchSplit> splits, @Nullable Long nextRowId) {
        if (splits.isEmpty() && !rawSearchEnabled()) {
            return GlobalIndexResult.createEmpty();
        }

        GlobalIndexer globalIndexer = splits.isEmpty() ? null : createGlobalIndexer(splits);
        ScoredGlobalIndexResult result =
                splits.isEmpty()
                        ? ScoredGlobalIndexResult.createEmpty()
                        : readIndexed(splits, globalIndexer);
        return withRawSearch(result, splits, globalIndexer, nextRowId, vector);
    }

    protected ScoredGlobalIndexResult readIndexed(
            List<VectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        RoaringNavigableMap64 preFilter = preFilter(splits).orElse(null);

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        int parallelism = table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        ExecutorService executor = GlobalIndexReadThreadPool.getExecutorService(parallelism);

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(splits.size());
        for (VectorSearchSplit split : splits) {
            futures.add(
                    eval(
                            globalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vector,
                            preFilter,
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
        return merged;
    }
}
