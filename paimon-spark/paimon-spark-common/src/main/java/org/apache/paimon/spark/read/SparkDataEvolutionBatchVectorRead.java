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
import org.apache.paimon.table.source.DataEvolutionBatchVectorRead;
import org.apache.paimon.table.source.IndexVectorSearchSplit;
import org.apache.paimon.table.source.RawVectorSearchSplit;
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
 * Spark-aware {@link DataEvolutionBatchVectorRead}. Each Spark task evaluates every query vector
 * against its split group, and the driver merges one result per query in input order.
 */
public class SparkDataEvolutionBatchVectorRead extends DataEvolutionBatchVectorRead {

    private static final long serialVersionUID = 1L;

    /**
     * Metric learned from shard readers on executors. The driver indexer never opens those readers,
     * so its {@code metric()} stays on the table configuration.
     */
    private String distributedVectorMetric;

    public SparkDataEvolutionBatchVectorRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[][] vectors,
            @Nullable Map<String, String> options) {
        super(table, partitionFilter, filter, limit, vectorColumn, vectors, options);
    }

    @Override
    protected ScoredGlobalIndexResult[] readIndexedBatch(
            List<IndexVectorSearchSplit> splits, GlobalIndexer globalIndexer) {
        distributedVectorMetric = null;
        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
            return super.readIndexedBatch(splits, globalIndexer);
        }

        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = vectorIndexType(splits);
        int searchLimit = indexedSearchLimit(indexType);
        int queryCount = vectors.length;
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
                        SparkVectorSearchSupport.BatchIndexGroupResult>
                task = group -> evaluateIndexGroup(group, searchLimit, queryCount, parallelism);

        List<SparkVectorSearchSupport.BatchIndexGroupResult> groupResults =
                mapInSpark(splitGroups, task, splitGroups.size());
        List<byte[][]> remoteResults = new ArrayList<>(groupResults.size());
        List<String> metrics = new ArrayList<>(groupResults.size());
        for (SparkVectorSearchSupport.BatchIndexGroupResult groupResult : groupResults) {
            remoteResults.add(groupResult.results);
            metrics.add(groupResult.metric);
        }
        distributedVectorMetric = SparkVectorSearchSupport.resolveVectorMetric(metrics);
        ScoredGlobalIndexResult[] merged =
                mergeRemoteResults(remoteResults, queryCount, searchLimit);
        // Avoid loading the indexer when refine is off; maybeRerankIndexedBatchResults repeats this
        // check.
        if (configuredRefineFactor(indexType) == 0) {
            return merged;
        }
        GlobalIndexer rerankGlobalIndexer =
                globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer;
        return maybeRerankIndexedBatchResults(merged, indexType, rerankGlobalIndexer);
    }

    @Override
    protected List<GlobalIndexResult> combineWithRaw(
            ScoredGlobalIndexResult[] indexedResults,
            List<RawVectorSearchSplit> rawSplits,
            @Nullable GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 rawPreFilter) {
        List<Range> rawRowRanges = rawRowRanges(rawSplits);
        int parallelism = sparkParallelism();
        if (rawRowRanges.isEmpty()
                || SparkVectorSearchSupport.rawRowCount(rawRowRanges) < parallelism * 2L) {
            return super.combineWithRaw(indexedResults, rawSplits, globalIndexer, rawPreFilter);
        }

        String metric = distributedSearchMetric(rawSearchIndexer(rawSplits, globalIndexer));
        int queryCount = vectors.length;
        List<List<Range>> rangeGroups =
                SparkVectorSearchSupport.rangeGroups(rawRowRanges, parallelism);
        List<SparkVectorSearchSupport.SerializedSplit> serializedSplits =
                new ArrayList<>(rangeGroups.size());
        for (List<Range> rangeGroup : rangeGroups) {
            try {
                serializedSplits.add(
                        new SparkVectorSearchSupport.SerializedSplit(
                                InstantiationUtil.serializeObject(rangeGroup),
                                rawPreFilter == null
                                        ? null
                                        : InstantiationUtil.serializeObject(rawPreFilter)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize raw vector row ranges", e);
            }
        }
        List<List<SparkVectorSearchSupport.SerializedSplit>> splitGroups =
                SparkVectorSearchSupport.splitGroups(serializedSplits, parallelism);
        SerializableFunction<List<SparkVectorSearchSupport.SerializedSplit>, byte[][]> task =
                group -> evaluateRawGroup(group, metric, queryCount);

        List<byte[][]> remoteResults = mapInSpark(splitGroups, task, splitGroups.size());
        ScoredGlobalIndexResult[] rawResults = mergeRemoteResults(remoteResults, queryCount, limit);
        List<GlobalIndexResult> results = new ArrayList<>(queryCount);
        for (int i = 0; i < queryCount; i++) {
            results.add(indexedResults[i].or(rawResults[i]).topK(limit));
        }
        return results;
    }

    @Override
    protected String rawSearchMetric(@Nullable GlobalIndexer globalIndexer) {
        return distributedSearchMetric(globalIndexer);
    }

    private String distributedSearchMetric(@Nullable GlobalIndexer globalIndexer) {
        if (distributedVectorMetric != null) {
            return distributedVectorMetric;
        }
        return super.rawSearchMetric(globalIndexer);
    }

    private SparkVectorSearchSupport.BatchIndexGroupResult evaluateIndexGroup(
            List<SparkVectorSearchSupport.SerializedSplit> group,
            int searchLimit,
            int queryCount,
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
        List<CompletableFuture<List<Optional<ScoredGlobalIndexResult>>>> futures =
                new ArrayList<>(group.size());
        for (int i = 0; i < splits.size(); i++) {
            IndexVectorSearchSplit split = splits.get(i);
            futures.add(
                    evalBatch(
                            taskGlobalIndexer,
                            indexPathFactory,
                            split.rowRangeStart(),
                            split.rowRangeEnd(),
                            split.vectorIndexFiles(),
                            vectors,
                            searchLimit,
                            groupPreFilters.get(i),
                            executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<List<ScoredGlobalIndexResult>> resultsByQuery = emptyQueryResults(queryCount);
        for (CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> future : futures) {
            List<Optional<ScoredGlobalIndexResult>> splitResults = future.join();
            for (int i = 0; i < queryCount; i++) {
                Optional<ScoredGlobalIndexResult> splitResult = splitResults.get(i);
                if (splitResult.isPresent()) {
                    resultsByQuery.get(i).add(splitResult.get());
                }
            }
        }
        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[queryCount];
        for (int i = 0; i < queryCount; i++) {
            merged[i] = ScoredGlobalIndexResult.merge(resultsByQuery.get(i)).topK(searchLimit);
        }
        String metric = null;
        if (openedReader && taskGlobalIndexer instanceof VectorGlobalIndexer) {
            metric = ((VectorGlobalIndexer) taskGlobalIndexer).metric();
        }
        return new SparkVectorSearchSupport.BatchIndexGroupResult(serializeResults(merged), metric);
    }

    private byte[][] evaluateRawGroup(
            List<SparkVectorSearchSupport.SerializedSplit> group, String metric, int queryCount) {
        List<List<ScoredGlobalIndexResult>> resultsByQuery = emptyQueryResults(queryCount);
        for (SparkVectorSearchSupport.SerializedSplit serializedSplit : group) {
            List<Range> rowRanges =
                    SparkVectorSearchSupport.deserialize(
                            serializedSplit.split, "Failed to deserialize raw vector row ranges");
            RoaringNavigableMap64 preFilter =
                    SparkVectorSearchSupport.deserializePreFilter(serializedSplit.preFilter);
            for (int i = 0; i < queryCount; i++) {
                resultsByQuery.get(i).add(readRawSearch(rowRanges, preFilter, metric, vectors[i]));
            }
        }
        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[queryCount];
        for (int i = 0; i < queryCount; i++) {
            merged[i] = ScoredGlobalIndexResult.merge(resultsByQuery.get(i)).topK(limit);
        }
        return serializeResults(merged);
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

    private List<List<ScoredGlobalIndexResult>> emptyQueryResults(int queryCount) {
        List<List<ScoredGlobalIndexResult>> resultsByQuery = new ArrayList<>(queryCount);
        for (int i = 0; i < queryCount; i++) {
            resultsByQuery.add(new ArrayList<>());
        }
        return resultsByQuery;
    }

    @Nullable
    private byte[][] serializeResults(ScoredGlobalIndexResult[] results) {
        byte[][] serialized = new byte[results.length][];
        boolean any = false;
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        try {
            for (int i = 0; i < results.length; i++) {
                if (!results[i].results().isEmpty()) {
                    serialized[i] = serializer.serialize(results[i]);
                    any = true;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ScoredGlobalIndexResult", e);
        }
        return any ? serialized : null;
    }

    private ScoredGlobalIndexResult[] mergeRemoteResults(
            List<byte[][]> remoteResults, int queryCount, int topK) {
        List<List<ScoredGlobalIndexResult>> resultsByQuery = emptyQueryResults(queryCount);
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[][] group : remoteResults) {
            if (group == null) {
                continue;
            }
            for (int i = 0; i < queryCount; i++) {
                if (group[i] != null) {
                    try {
                        resultsByQuery.get(i).add(serializer.deserialize(group[i]));
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to deserialize ScoredGlobalIndexResult", e);
                    }
                }
            }
        }
        ScoredGlobalIndexResult[] merged = new ScoredGlobalIndexResult[queryCount];
        for (int i = 0; i < queryCount; i++) {
            merged[i] = ScoredGlobalIndexResult.merge(resultsByQuery.get(i)).topK(topK);
        }
        return merged;
    }
}
