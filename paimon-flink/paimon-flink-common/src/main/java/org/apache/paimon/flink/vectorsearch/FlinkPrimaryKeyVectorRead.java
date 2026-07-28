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
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorRead;
import org.apache.paimon.table.source.PrimaryKeyVectorScan;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;

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

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Flink-aware {@link PrimaryKeyVectorRead}. */
public class FlinkPrimaryKeyVectorRead extends PrimaryKeyVectorRead {

    private static final long serialVersionUID = 1L;

    private final transient StreamExecutionEnvironment env;

    public FlinkPrimaryKeyVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[] query,
            int limit,
            Map<String, String> searchOptions,
            @Nullable Predicate filter,
            StreamExecutionEnvironment env) {
        super(table, vectorField, query, limit, searchOptions, filter);
        this.env = checkNotNull(env);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        PrimaryKeyVectorScan.Plan primaryKeyPlan = primaryKeyPlan(plan);
        List<BucketVectorSearchSplit> splits = bucketSplits(primaryKeyPlan);
        int parallelism = flinkParallelism();
        if (splits.size() < parallelism * 2L) {
            return super.read(plan);
        }

        List<byte[]> serializedSplits = new ArrayList<>(splits.size());
        for (BucketVectorSearchSplit split : splits) {
            try {
                serializedSplits.add(InstantiationUtil.serializeObject(split));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize primary-key vector split.", e);
            }
        }

        List<List<byte[]>> groups = splitGroups(serializedSplits, parallelism);
        List<byte[]> groupResults = executeBucketSearchGroups(groups, parallelism);
        List<SearchResult> searchResults = new ArrayList<>(groupResults.size());
        for (byte[] groupResult : groupResults) {
            searchResults.add(deserializeSearchResult(groupResult));
        }
        return createResult(primaryKeyPlan, mergeSearchResults(searchResults));
    }

    protected int flinkParallelism() {
        int maxParallelism =
                Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
        int envParallelism = checkNotNull(env).getParallelism();
        return envParallelism > 0
                ? Math.max(1, Math.min(envParallelism, maxParallelism))
                : maxParallelism;
    }

    protected List<byte[]> executeBucketSearchGroups(List<List<byte[]>> groups, int parallelism) {
        String operatorName = "Primary-Key Vector Search";
        List<byte[]> serializedGroups = new ArrayList<>(groups.size());
        try {
            for (List<byte[]> group : groups) {
                serializedGroups.add(InstantiationUtil.serializeObject(group));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize primary-key vector task groups.", e);
        }

        DataStream<byte[]> results =
                StreamExecutionEnvironmentUtils.fromData(
                                checkNotNull(env),
                                serializedGroups,
                                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name(operatorName + " Source")
                        .setParallelism(1)
                        .rebalance()
                        .map(
                                (MapFunction<byte[], byte[]>)
                                        bytes -> executeBucketSearchGroup(deserializeGroup(bytes)))
                        .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name(operatorName)
                        .setParallelism(Math.min(parallelism, groups.size()));

        List<byte[]> output = new ArrayList<>(groups.size());
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

    protected byte[] executeBucketSearchGroup(List<byte[]> group) {
        List<BucketVectorSearchSplit> taskSplits = new ArrayList<>(group.size());
        for (byte[] bytes : group) {
            taskSplits.add(deserializeSplit(bytes));
        }
        try {
            return InstantiationUtil.serializeObject(searchBuckets(taskSplits));
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize primary-key vector candidates.", e);
        }
    }

    private List<byte[]> deserializeGroup(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize primary-key vector task group.", e);
        }
    }

    private List<List<byte[]>> splitGroups(List<byte[]> splits, int parallelism) {
        List<List<byte[]>> groups = new ArrayList<>(parallelism);
        int groupSize = (splits.size() + parallelism - 1) / parallelism;
        for (int start = 0; start < splits.size(); start += groupSize) {
            groups.add(
                    new ArrayList<>(
                            splits.subList(start, Math.min(start + groupSize, splits.size()))));
        }
        return groups;
    }

    private BucketVectorSearchSplit deserializeSplit(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize primary-key vector split.", e);
        }
    }

    private SearchResult deserializeSearchResult(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(
                    "Failed to deserialize primary-key vector search result.", e);
        }
    }
}
