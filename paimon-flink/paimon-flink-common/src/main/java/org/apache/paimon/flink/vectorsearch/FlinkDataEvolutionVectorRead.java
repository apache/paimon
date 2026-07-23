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

package org.apache.paimon.flink.vectorsearch;

import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.GlobalIndexReadThreadPool;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Flink-aware {@link DataEvolutionVectorRead}. */
public class FlinkDataEvolutionVectorRead extends DataEvolutionVectorRead {

    private static final long serialVersionUID = 1L;
    private static final byte INDEX_RESULT = 0;
    private static final byte RAW_RESULT = 1;

    private final transient StreamExecutionEnvironment env;

    public FlinkDataEvolutionVectorRead(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector,
            @Nullable Map<String, String> options,
            StreamExecutionEnvironment env) {
        super(table, partitionFilter, filter, limit, vectorColumn, vector, options);
        this.env = checkNotNull(env);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        List<IndexVectorSearchSplit> indexSplits = new ArrayList<>();
        List<RawVectorSearchSplit> rawSplits = new ArrayList<>();
        splitSearchSplits(plan.splits(), indexSplits, rawSplits);
        if (indexSplits.isEmpty() && rawSplits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        GlobalIndexer globalIndexer =
                !indexSplits.isEmpty() && !rawSplits.isEmpty()
                        ? createGlobalIndexer(indexSplits)
                        : null;
        if (!indexSplits.isEmpty() && !rawSplits.isEmpty()) {
            int parallelism = flinkParallelism();
            List<Range> rawRowRanges = rawRowRanges(rawSplits);
            if (indexSplits.size() >= parallelism * 2L
                    && rawRowCount(rawRowRanges) >= parallelism * 2L) {
                return readIndexAndRawSplitsInFlink(
                        indexSplits,
                        rawSplits,
                        rawRowRanges,
                        globalIndexer,
                        rawPreFilter(rawSplits),
                        parallelism);
            }
        }

        ScoredGlobalIndexResult indexed =
                indexSplits.isEmpty()
                        ? ScoredGlobalIndexResult.createEmpty()
                        : readIndexSplitsInFlink(indexSplits, globalIndexer);
        ScoredGlobalIndexResult raw =
                readRawSplitsInFlink(rawSplits, globalIndexer, rawPreFilter(rawSplits));
        return indexed.or(raw).topK(limit);
    }

    protected ScoredGlobalIndexResult readIndexSplitsInFlink(
            List<IndexVectorSearchSplit> splits, @Nullable GlobalIndexer globalIndexer) {
        if (splits.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        int parallelism = flinkParallelism();
        if (splits.size() < parallelism * 2L) {
            return readIndexed(
                    splits, globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer);
        }

        List<RoaringNavigableMap64> preFilters = preFilters(splits);
        String indexType = vectorIndexType(splits);
        int searchLimit = indexedSearchLimit(indexType);
        List<List<SerializedSplit>> splitGroups = indexSplitGroups(splits, preFilters, parallelism);
        List<byte[]> remoteResults =
                executeIndexSearchGroups(splitGroups, searchLimit, parallelism);
        GlobalIndexer rerankGlobalIndexer =
                globalIndexer == null ? createGlobalIndexer(splits) : globalIndexer;
        return maybeRerankIndexedResult(
                mergeRemoteResults(remoteResults, searchLimit),
                indexType,
                rerankGlobalIndexer,
                vector);
    }

    private List<List<SerializedSplit>> indexSplitGroups(
            List<IndexVectorSearchSplit> splits,
            List<RoaringNavigableMap64> preFilters,
            int parallelism) {
        List<SerializedSplit> serializedSplits = new ArrayList<>(splits.size());
        for (int i = 0; i < splits.size(); i++) {
            try {
                IndexVectorSearchSplit split = splits.get(i);
                RoaringNavigableMap64 preFilter = preFilters.isEmpty() ? null : preFilters.get(i);
                serializedSplits.add(
                        new SerializedSplit(
                                InstantiationUtil.serializeObject(split),
                                preFilter == null
                                        ? null
                                        : InstantiationUtil.serializeObject(preFilter)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize vector-search split.", e);
            }
        }
        return splitGroups(serializedSplits, parallelism);
    }

    protected ScoredGlobalIndexResult readRawSplitsInFlink(
            List<RawVectorSearchSplit> splits,
            @Nullable GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 preFilter) {
        List<Range> rawRowRanges = rawRowRanges(splits);
        if (rawRowRanges.isEmpty()) {
            return ScoredGlobalIndexResult.createEmpty();
        }

        int parallelism = flinkParallelism();
        if (rawRowCount(rawRowRanges) < parallelism * 2L) {
            return readRawSearch(
                    rawRowRanges, preFilter, rawSearchIndexer(splits, globalIndexer), vector);
        }

        String metric = rawSearchMetric(rawSearchIndexer(splits, globalIndexer));
        List<List<SerializedSplit>> splitGroups =
                rawSplitGroups(rawRowRanges, preFilter, parallelism);
        List<byte[]> remoteResults = executeRawSearchGroups(splitGroups, metric, parallelism);
        return mergeRemoteResults(remoteResults, limit);
    }

    private List<List<SerializedSplit>> rawSplitGroups(
            List<Range> rawRowRanges, @Nullable RoaringNavigableMap64 preFilter, int parallelism) {
        List<List<Range>> rangeGroups = rangeGroups(rawRowRanges, parallelism);
        List<SerializedSplit> serializedSplits = new ArrayList<>(rangeGroups.size());
        for (List<Range> rangeGroup : rangeGroups) {
            try {
                RoaringNavigableMap64 groupPreFilter = groupPreFilter(rangeGroup, preFilter);
                serializedSplits.add(
                        new SerializedSplit(
                                InstantiationUtil.serializeObject(rangeGroup),
                                groupPreFilter == null
                                        ? null
                                        : InstantiationUtil.serializeObject(groupPreFilter)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize raw vector row ranges.", e);
            }
        }
        return splitGroups(serializedSplits, parallelism);
    }

    @Nullable
    private RoaringNavigableMap64 groupPreFilter(
            List<Range> rangeGroup, @Nullable RoaringNavigableMap64 preFilter) {
        if (preFilter == null) {
            return null;
        }

        RoaringNavigableMap64 groupRows = new RoaringNavigableMap64();
        for (Range range : rangeGroup) {
            groupRows.addRange(range);
        }
        groupRows.and(preFilter);
        if (groupRows.getLongCardinality() == rawRowCount(rangeGroup)) {
            return null;
        }
        groupRows.runOptimize();
        return groupRows;
    }

    private ScoredGlobalIndexResult readIndexAndRawSplitsInFlink(
            List<IndexVectorSearchSplit> indexSplits,
            List<RawVectorSearchSplit> rawSplits,
            List<Range> rawRowRanges,
            GlobalIndexer globalIndexer,
            @Nullable RoaringNavigableMap64 rawPreFilter,
            int parallelism) {
        String indexType = vectorIndexType(indexSplits);
        int searchLimit = indexedSearchLimit(indexType);
        List<List<SerializedSplit>> indexGroups =
                indexSplitGroups(indexSplits, preFilters(indexSplits), parallelism);

        String rawMetric = rawSearchMetric(rawSearchIndexer(rawSplits, globalIndexer));
        List<List<SerializedSplit>> rawGroups =
                rawSplitGroups(rawRowRanges, rawPreFilter, parallelism);

        List<byte[]> taggedResults =
                executeIndexAndRawSearchGroups(
                        indexGroups, searchLimit, rawGroups, rawMetric, parallelism);
        List<byte[]> indexedResults = new ArrayList<>();
        List<byte[]> rawResults = new ArrayList<>();
        for (byte[] taggedResult : taggedResults) {
            if (taggedResult.length == 0) {
                throw new IllegalArgumentException("Missing vector-search result type.");
            }
            byte[] result = untagResult(taggedResult);
            if (taggedResult[0] == INDEX_RESULT) {
                indexedResults.add(result);
            } else if (taggedResult[0] == RAW_RESULT) {
                rawResults.add(result);
            } else {
                throw new IllegalArgumentException(
                        "Unknown vector-search result type: " + taggedResult[0]);
            }
        }

        ScoredGlobalIndexResult indexed =
                maybeRerankIndexedResult(
                        mergeRemoteResults(indexedResults, searchLimit),
                        indexType,
                        globalIndexer,
                        vector);
        ScoredGlobalIndexResult raw = mergeRemoteResults(rawResults, limit);
        return indexed.or(raw).topK(limit);
    }

    protected int flinkParallelism() {
        int maxParallelism =
                Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
        int envParallelism = checkNotNull(env).getParallelism();
        return envParallelism > 0
                ? Math.max(1, Math.min(envParallelism, maxParallelism))
                : maxParallelism;
    }

    protected List<byte[]> executeIndexSearchGroups(
            List<List<SerializedSplit>> groups, int searchLimit, int parallelism) {
        String operatorName = "Vector Index Search";
        return collectResults(
                indexSearchStream(groups, searchLimit, parallelism, operatorName), operatorName);
    }

    protected List<byte[]> executeRawSearchGroups(
            List<List<SerializedSplit>> groups, String metric, int parallelism) {
        String operatorName = "Raw Vector Search";
        return collectResults(
                rawSearchStream(groups, metric, parallelism, operatorName), operatorName);
    }

    protected List<byte[]> executeIndexAndRawSearchGroups(
            List<List<SerializedSplit>> indexGroups,
            int searchLimit,
            List<List<SerializedSplit>> rawGroups,
            String rawMetric,
            int parallelism) {
        DataStream<byte[]> indexResults =
                tagResults(
                        indexSearchStream(
                                indexGroups, searchLimit, parallelism, "Vector Index Search"),
                        INDEX_RESULT,
                        "Tag Vector Index Results");
        DataStream<byte[]> rawResults =
                tagResults(
                        rawSearchStream(rawGroups, rawMetric, parallelism, "Raw Vector Search"),
                        RAW_RESULT,
                        "Tag Raw Vector Results");
        return collectResults(indexResults.union(rawResults), "Vector Index and Raw Search");
    }

    private DataStream<byte[]> indexSearchStream(
            List<List<SerializedSplit>> groups,
            int searchLimit,
            int parallelism,
            String operatorName) {
        DataStream<byte[]> results =
                taskSource(groups, operatorName)
                        .map(
                                (MapFunction<byte[], byte[]>)
                                        bytes ->
                                                executeIndexSearchGroup(
                                                        deserializeSplitGroup(bytes),
                                                        searchLimit,
                                                        parallelism))
                        .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name(operatorName)
                        .setParallelism(Math.min(parallelism, groups.size()));
        return results;
    }

    private DataStream<byte[]> rawSearchStream(
            List<List<SerializedSplit>> groups,
            String metric,
            int parallelism,
            String operatorName) {
        return taskSource(groups, operatorName)
                .map(
                        (MapFunction<byte[], byte[]>)
                                bytes ->
                                        executeRawSearchGroup(deserializeSplitGroup(bytes), metric))
                .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .name(operatorName)
                .setParallelism(Math.min(parallelism, groups.size()));
    }

    static DataStream<byte[]> tagResults(
            DataStream<byte[]> results, byte resultType, String operatorName) {
        return results.map((MapFunction<byte[], byte[]>) bytes -> tagResult(resultType, bytes))
                .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .name(operatorName)
                .setParallelism(results.getParallelism());
    }

    private DataStream<byte[]> taskSource(List<List<SerializedSplit>> groups, String operatorName) {
        List<byte[]> serializedGroups = new ArrayList<>(groups.size());
        try {
            for (List<SerializedSplit> group : groups) {
                serializedGroups.add(InstantiationUtil.serializeObject(group));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize vector-search task groups.", e);
        }

        return StreamExecutionEnvironmentUtils.fromData(
                        checkNotNull(env),
                        serializedGroups,
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .name(operatorName + " Source")
                .setParallelism(1)
                .rebalance();
    }

    private byte[] executeIndexSearchGroup(
            List<SerializedSplit> group, int searchLimit, int parallelism) {
        List<IndexVectorSearchSplit> splits = new ArrayList<>(group.size());
        for (SerializedSplit serializedSplit : group) {
            splits.add(deserializeSplit(serializedSplit.split));
        }

        GlobalIndexer taskGlobalIndexer = createGlobalIndexer(splits);
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        ExecutorService executor =
                GlobalIndexReadThreadPool.getExecutorService(Math.min(parallelism, group.size()));

        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                new ArrayList<>(group.size());
        for (int i = 0; i < group.size(); i++) {
            SerializedSplit serializedSplit = group.get(i);
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
                            deserializePreFilter(serializedSplit.preFilter),
                            executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future : futures) {
            Optional<ScoredGlobalIndexResult> next = future.join();
            if (next.isPresent()) {
                result = result.or(next.get());
            }
        }
        return serializeResult(result.topK(searchLimit));
    }

    protected byte[] executeRawSearchGroup(List<SerializedSplit> group, String metric) {
        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        for (SerializedSplit serializedSplit : group) {
            ScoredGlobalIndexResult splitResult =
                    readRawSearch(
                            deserializeRanges(serializedSplit.split),
                            deserializePreFilter(serializedSplit.preFilter),
                            metric,
                            vector);
            result = result.or(splitResult);
        }
        return serializeResult(result.topK(limit));
    }

    private List<SerializedSplit> deserializeSplitGroup(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize vector-search task group.", e);
        }
    }

    private List<byte[]> collectResults(DataStream<byte[]> results, String operatorName) {
        List<byte[]> output = new ArrayList<>();
        try (CloseableIterator<byte[]> iterator =
                results.executeAndCollect(flinkJobName(operatorName))) {
            iterator.forEachRemaining(output::add);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute " + operatorName + ".", e);
        }
        return output;
    }

    protected String flinkJobName(String operatorName) {
        return "Vector Search - " + operatorName + " : " + table.fullName();
    }

    static byte[] tagResult(byte resultType, byte[] result) {
        byte[] tagged = new byte[result.length + 1];
        tagged[0] = resultType;
        System.arraycopy(result, 0, tagged, 1, result.length);
        return tagged;
    }

    private static byte[] untagResult(byte[] taggedResult) {
        byte[] result = new byte[taggedResult.length - 1];
        System.arraycopy(taggedResult, 1, result, 0, result.length);
        return result;
    }

    private byte[] serializeResult(ScoredGlobalIndexResult result) {
        if (result.results().isEmpty()) {
            return new byte[0];
        }
        try {
            return new GlobalIndexResultSerializer().serialize(result);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize scored vector-search result.", e);
        }
    }

    private IndexVectorSearchSplit deserializeSplit(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize vector-search split.", e);
        }
    }

    private List<Range> deserializeRanges(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize raw vector row ranges.", e);
        }
    }

    @Nullable
    private RoaringNavigableMap64 deserializePreFilter(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize vector pre-filter.", e);
        }
    }

    private List<List<SerializedSplit>> splitGroups(
            List<SerializedSplit> serializedSplits, int parallelism) {
        List<List<SerializedSplit>> groups = new ArrayList<>(parallelism);
        int groupSize = (serializedSplits.size() + parallelism - 1) / parallelism;
        for (int start = 0; start < serializedSplits.size(); start += groupSize) {
            groups.add(
                    new ArrayList<>(
                            serializedSplits.subList(
                                    start, Math.min(start + groupSize, serializedSplits.size()))));
        }
        return groups;
    }

    private ScoredGlobalIndexResult mergeRemoteResults(List<byte[]> remoteResults, int topK) {
        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[] bytes : remoteResults) {
            if (bytes != null && bytes.length > 0) {
                try {
                    result = result.or(serializer.deserialize(bytes));
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to deserialize scored vector-search result.", e);
                }
            }
        }
        return result.topK(topK);
    }

    private List<List<Range>> rangeGroups(List<Range> ranges, int parallelism) {
        long rowCount = rawRowCount(ranges);
        int groupCount = (int) Math.min(parallelism, rowCount);
        long targetRowsPerGroup = (rowCount - 1) / groupCount + 1;

        List<List<Range>> groups = new ArrayList<>(groupCount);
        List<Range> currentGroup = new ArrayList<>();
        long currentRows = 0;
        for (Range range : ranges) {
            long from = range.from;
            while (from <= range.to) {
                if (currentRows == targetRowsPerGroup) {
                    groups.add(currentGroup);
                    currentGroup = new ArrayList<>();
                    currentRows = 0;
                }
                long remainingGroupRows = targetRowsPerGroup - currentRows;
                long to = Math.min(range.to, from + remainingGroupRows - 1);
                Range next = new Range(from, to);
                currentGroup.add(next);
                currentRows += next.count();
                from = to + 1;
            }
        }
        if (!currentGroup.isEmpty()) {
            groups.add(currentGroup);
        }
        return groups;
    }

    private long rawRowCount(List<Range> ranges) {
        long rowCount = 0;
        for (Range range : ranges) {
            long count = range.count();
            if (Long.MAX_VALUE - rowCount < count) {
                return Long.MAX_VALUE;
            }
            rowCount += count;
        }
        return rowCount;
    }

    static class SerializedSplit implements java.io.Serializable {

        private static final long serialVersionUID = 1L;

        private final byte[] split;
        @Nullable private final byte[] preFilter;

        private SerializedSplit(@Nullable byte[] split, @Nullable byte[] preFilter) {
            this.split = split;
            this.preFilter = preFilter;
        }
    }
}
