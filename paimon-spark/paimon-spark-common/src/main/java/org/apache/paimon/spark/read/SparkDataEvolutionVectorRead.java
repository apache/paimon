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

package org.apache.paimon.spark.read;

import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.VectorGlobalIndexer;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataEvolutionVectorRead;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Spark-aware {@link DataEvolutionVectorRead} that distributes grouped vector index evaluation
 * across the Spark cluster instead of evaluating them with the local thread pool.
 */
public class SparkDataEvolutionVectorRead extends DataEvolutionVectorRead {

    private static final long serialVersionUID = 1L;

    /**
     * Metric learned from shard readers on executors. The driver indexer never opens those readers,
     * so its {@code metric()} stays on the table configuration.
     */
    private String distributedVectorMetric;

    public SparkDataEvolutionVectorRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, vector, options);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        this.planSnapshot = plan.snapshot();
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        prepareSplits(plan.splits(), indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        GlobalIndexer globalIndexer =
                !indexSplits.isEmpty() && !rawSplits.isEmpty()
                        ? createGlobalIndexer(indexSplits)
                        : null;
        ScoredGlobalIndexResult result =
                indexSplits.isEmpty()
                        ? ScoredGlobalIndexResult.createEmpty()
                        : readIndexSplitsInSpark(indexSplits, globalIndexer);
        return result.or(readRawSplitsInSpark(rawSplits, globalIndexer, rawPreFilter(rawSplits)))
                .topK(limit);
    }

    protected ScoredGlobalIndexResult readIndexSplitsInSpark(
            List<IndexVectorSearchSplit> splits, @Nullable GlobalIndexer globalIndexer) {
        if (splits.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        distributedVectorMetric = null;
        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
            return readIndexed(
                    splits, globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer);
        }

        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = vectorIndexType(splits);
        int searchLimit = indexedSearchLimit(indexType);
        List<SparkVectorSearchSupport.SerializedSplit> serializedSplits =
                new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            try {
                IndexVectorSearchSplit split = splits.get(i);
                RoaringNavigableMap64 preFilter = preFilters.isEmpty() ? null : preFilters.get(i);
                serializedSplits.add(
                        new SparkVectorSearchSupport.SerializedSplit(
                                InstantiationUtil.serializeObject(split),
                                preFilter == null
                                        ? null
                                        : InstantiationUtil.serializeObject(preFilter)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize VectorSearchSplit", e);
            }
        }
        List<List<SparkVectorSearchSupport.SerializedSplit>> splitGroups =
                SparkVectorSearchSupport.splitGroups(serializedSplits, parallelism);
        SerializableFunction<
                        List<SparkVectorSearchSupport.SerializedSplit>,
                        SparkVectorSearchSupport.SingleIndexGroupResult>
                task = group -> evaluateIndexGroup(group, searchLimit, parallelism);

        List<SparkVectorSearchSupport.SingleIndexGroupResult> groupResults =
                mapInSpark(splitGroups, task, splitGroups.size());
        List<byte[]> remoteResults = new ArrayList<>(groupResults.size());
        List<String> metrics = new ArrayList<>(groupResults.size());
        for (SparkVectorSearchSupport.SingleIndexGroupResult groupResult : groupResults) {
            remoteResults.add(groupResult.result);
            metrics.add(groupResult.metric);
        }
        distributedVectorMetric = SparkVectorSearchSupport.resolveVectorMetric(metrics);

        GlobalIndexer rerankGlobalIndexer =
                globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer;
        return maybeRerankIndexedResult(
                mergeRemoteResults(remoteResults, searchLimit),
                indexType,
                rerankGlobalIndexer,
                vector);
    }

    @Override
    protected String rawSearchMetric(@Nullable GlobalIndexer globalIndexer) {
        if (distributedVectorMetric != null) {
            return distributedVectorMetric;
        }
        return super.rawSearchMetric(globalIndexer);
    }

    private SparkVectorSearchSupport.SingleIndexGroupResult evaluateIndexGroup(
            List<SparkVectorSearchSupport.SerializedSplit> group,
            int searchLimit,
            int parallelism) {
        List<IndexVectorSearchSplit> splits = new ArrayList<>(group.size());
        List<RoaringNavigableMap64> groupPreFilters = new ArrayList<>(group.size());
        boolean openedReader = false;
        for (SparkVectorSearchSupport.SerializedSplit serializedSplit : group) {
            IndexVectorSearchSplit split =
                    SparkVectorSearchSupport.deserialize(
                            serializedSplit.split, "Failed to deserialize VectorSearchSplit");
            splits.add(split);
            groupPreFilters.add(
                    SparkVectorSearchSupport.deserializePreFilter(serializedSplit.preFilter));
            if (!split.vectorIndexFiles().isEmpty()) {
                openedReader = true;
            }
        }
        // Same field list as the local path, including scalar or text columns on a multi-field
        // index. A vector-only indexer selects the single-file reader and rejects those shards.
        GlobalIndexer taskGlobalIndexer = createGlobalIndexer(splits);
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        ExecutorService executor =
                GlobalIndexReadThreadPool.getExecutorService(Math.min(parallelism, group.size()));
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(group.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            futures.add(
                    eval(
                            taskGlobalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vector,
                            searchLimit,
                            groupPreFilters.get(i),
                            executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        List<ScoredGlobalIndexResult> results = new ArrayList<>(futures.size());
        for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future : futures) {
            Optional<ScoredGlobalIndexResult> next = future.join();
            if (next.isPresent()) {
                results.add(next.get());
            }
        }
        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.merge(results).topK(searchLimit);
        String metric = null;
        if (openedReader && taskGlobalIndexer instanceof VectorGlobalIndexer) {
            metric = ((VectorGlobalIndexer) taskGlobalIndexer).metric();
        }
        if (result.results().isEmpty()) {
            return new SparkVectorSearchSupport.SingleIndexGroupResult(null, metric);
        }
        try {
            return new SparkVectorSearchSupport.SingleIndexGroupResult(
                    new GlobalIndexResultSerializer().serialize(result), metric);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ScoredGlobalIndexResult", e);
        }
    }

    protected ScoredGlobalIndexResult readRawSplitsInSpark(
            List<RawVectorSearchSplit> splits,
            @Nullable GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 preFilter) {
        List<Range> rawRowRanges = rawRowRanges(splits);
        if (rawRowRanges.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        int parallelism = sparkParallelism();
        if (SparkVectorSearchSupport.rawRowCount(rawRowRanges) < parallelism * 2L) {
            return readRawSearch(
                    rawRowRanges, preFilter, rawSearchIndexer(splits, globalIndexer), vector);
        }

        String metric = rawSearchMetric(rawSearchIndexer(splits, globalIndexer));
        List<List<Range>> rangeGroups =
                SparkVectorSearchSupport.rangeGroups(rawRowRanges, parallelism);
        List<SparkVectorSearchSupport.SerializedSplit> serializedSplits =
                new ArrayList<>(rangeGroups.size());
        for (List<Range> rangeGroup : rangeGroups) {
            try {
                serializedSplits.add(
                        new SparkVectorSearchSupport.SerializedSplit(
                                InstantiationUtil.serializeObject(rangeGroup),
                                preFilter == null
                                        ? null
                                        : InstantiationUtil.serializeObject(preFilter)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize raw vector row ranges", e);
            }
        }
        List<List<SparkVectorSearchSupport.SerializedSplit>> splitGroups =
                SparkVectorSearchSupport.splitGroups(serializedSplits, parallelism);

        SerializableFunction<List<SparkVectorSearchSupport.SerializedSplit>, byte[]> task =
                group -> {
                    List<ScoredGlobalIndexResult> results = new ArrayList<>(group.size());
                    for (SparkVectorSearchSupport.SerializedSplit serializedSplit : group) {
                        List<Range> rowRanges =
                                SparkVectorSearchSupport.deserialize(
                                        serializedSplit.split,
                                        "Failed to deserialize raw vector row ranges");
                        ScoredGlobalIndexResult splitResult =
                                readRawSearch(
                                        rowRanges,
                                        SparkVectorSearchSupport.deserializePreFilter(
                                                serializedSplit.preFilter),
                                        metric,
                                        vector);
                        results.add(splitResult);
                    }
                    ScoredGlobalIndexResult result =
                            ScoredGlobalIndexResult.merge(results).topK(limit);
                    if (result.results().isEmpty()) {
                        return null;
                    }
                    try {
                        return new GlobalIndexResultSerializer().serialize(result);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to serialize ScoredGlobalIndexResult", e);
                    }
                };

        List<byte[]> remoteResults = mapInSpark(splitGroups, task, splitGroups.size());
        return mergeRemoteResults(remoteResults);
    }

    protected int sparkParallelism() {
        return SparkVectorSearchSupport.parallelism(table);
    }

    protected SparkEngineContext createEngineContext() {
        return SparkVectorSearchSupport.engineContext();
    }

    protected <I, O> List<O> mapInSpark(
            List<I> data, SerializableFunction<I, O> func, int parallelism) {
        return createEngineContext().map(data, func, parallelism);
    }

    private ScoredGlobalIndexResult mergeRemoteResults(List<byte[]> remoteResults) {
        return mergeRemoteResults(remoteResults, limit);
    }

    private ScoredGlobalIndexResult mergeRemoteResults(List<byte[]> remoteResults, int topK) {
        List<ScoredGlobalIndexResult> results = new ArrayList<>(remoteResults.size());
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[] bytes : remoteResults) {
            if (bytes != null) {
                try {
                    results.add(serializer.deserialize(bytes));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to deserialize ScoredGlobalIndexResult", e);
                }
            }
        }
        return ScoredGlobalIndexResult.merge(results).topK(topK);
    }
}
