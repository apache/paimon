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
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.VectorReadImpl;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.spark.broadcast.Broadcast;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/**
 * Spark-aware {@link VectorReadImpl} that distributes grouped vector index evaluation across the
 * Spark cluster instead of evaluating them with the local thread pool.
 */
public class SparkVectorReadImpl extends VectorReadImpl {

    private static final long serialVersionUID = 1L;

    public SparkVectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector) {
        super(table, filter, limit, vectorColumn, vector);
    }

    @Override
    public GlobalIndexResult read(List<VectorSearchSplit> splits) {
        if (splits.isEmpty()) {
            return GlobalIndexResult.createEmpty();
        }

        int parallelism =
                Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
        if (splits.size() < parallelism * 2) {
            return super.read(splits);
        }

        RoaringNavigableMap64 preFilter = preFilter(splits).orElse(null);
        String indexType = splits.get(0).vectorIndexFiles().get(0).indexType();
        List<byte[]> splitBytes = new ArrayList<>(splits.size());
        for (VectorSearchSplit split : splits) {
            try {
                splitBytes.add(InstantiationUtil.serializeObject(split));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize VectorSearchSplit", e);
            }
        }
        List<List<byte[]>> splitGroups = splitGroups(splitBytes, parallelism);
        SparkEngineContext engineContext = new SparkEngineContext();
        Broadcast<RoaringNavigableMap64> preFilterBroadcast =
                preFilter == null ? null : engineContext.broadcast(preFilter);

        SerializableFunction<List<byte[]>, byte[]> task =
                group -> {
                    GlobalIndexer globalIndexer =
                            GlobalIndexerFactoryUtils.load(indexType)
                                    .create(vectorColumn, table.coreOptions().toConfiguration());
                    IndexPathFactory indexPathFactory =
                            table.store().pathFactory().globalIndexFileFactory();

                    RoaringNavigableMap64 includeRowIds =
                            preFilterBroadcast == null ? null : preFilterBroadcast.value();
                    ExecutorService executor =
                            GlobalIndexReadThreadPool.getExecutorService(
                                    Math.min(parallelism, group.size()));
                    List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures =
                            new ArrayList<>(group.size());
                    for (byte[] bytes : group) {
                        VectorSearchSplit split = deserializeSplit(bytes);
                        futures.add(
                                eval(
                                        globalIndexer,
                                        indexPathFactory,
                                        split.rowRangeStart(),
                                        split.rowRangeEnd(),
                                        split.vectorIndexFiles(),
                                        includeRowIds,
                                        executor));
                    }
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                    ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
                    for (CompletableFuture<Optional<ScoredGlobalIndexResult>> f : futures) {
                        Optional<ScoredGlobalIndexResult> next = f.join();
                        if (next.isPresent()) {
                            result = result.or(next.get());
                        }
                    }
                    result = result.topK(limit);
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

        List<byte[]> remoteResults;
        try {
            remoteResults = engineContext.map(splitGroups, task, splitGroups.size());
        } finally {
            if (preFilterBroadcast != null) {
                preFilterBroadcast.unpersist(false);
            }
        }

        ScoredGlobalIndexResult result = ScoredGlobalIndexResult.createEmpty();
        GlobalIndexResultSerializer serializer = new GlobalIndexResultSerializer();
        for (byte[] bytes : remoteResults) {
            if (bytes != null) {
                try {
                    result = result.or(serializer.deserialize(bytes));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to deserialize ScoredGlobalIndexResult", e);
                }
            }
        }
        return result.topK(limit);
    }

    private VectorSearchSplit deserializeSplit(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize VectorSearchSplit", e);
        }
    }

    private List<List<byte[]>> splitGroups(List<byte[]> splitBytes, int parallelism) {
        List<List<byte[]>> groups = new ArrayList<>(parallelism);
        int groupSize = (splitBytes.size() + parallelism - 1) / parallelism;
        for (int start = 0; start < splitBytes.size(); start += groupSize) {
            groups.add(
                    new ArrayList<>(
                            splitBytes.subList(
                                    start, Math.min(start + groupSize, splitBytes.size()))));
        }
        return groups;
    }
}
