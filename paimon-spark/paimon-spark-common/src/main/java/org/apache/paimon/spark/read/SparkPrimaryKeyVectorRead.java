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

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorRead;
import org.apache.paimon.table.source.PrimaryKeyVectorScan;
import org.apache.paimon.table.source.VectorScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SerializableFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/** Spark-aware {@link PrimaryKeyVectorRead}. */
public class SparkPrimaryKeyVectorRead extends PrimaryKeyVectorRead {

    private static final long serialVersionUID = 1L;

    public SparkPrimaryKeyVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[] query,
            int limit,
            Map<String, String> searchOptions) {
        super(table, vectorField, query, limit, searchOptions);
    }

    @Override
    public GlobalIndexResult read(VectorScan.Plan plan) {
        PrimaryKeyVectorScan.Plan primaryKeyPlan = primaryKeyPlan(plan);
        List<BucketVectorSearchSplit> splits = bucketSplits(primaryKeyPlan);
        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
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
        SerializableFunction<List<byte[]>, byte[]> task =
                group -> {
                    List<BucketVectorSearchSplit> taskSplits = new ArrayList<>(group.size());
                    for (byte[] bytes : group) {
                        taskSplits.add(deserializeSplit(bytes));
                    }
                    try {
                        return InstantiationUtil.serializeObject(searchBuckets(taskSplits));
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to serialize primary-key vector candidates.", e);
                    }
                };
        List<byte[]> groupResults = mapInSpark(groups, task, groups.size());
        List<Candidate> candidates = new ArrayList<>();
        for (byte[] groupResult : groupResults) {
            candidates.addAll(deserializeCandidates(groupResult));
        }
        return createResult(primaryKeyPlan, candidates);
    }

    protected int sparkParallelism() {
        return Math.max(1, table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM));
    }

    protected SparkEngineContext createEngineContext() {
        return new SparkEngineContext();
    }

    protected <I, O> List<O> mapInSpark(
            List<I> data, SerializableFunction<I, O> function, int parallelism) {
        return createEngineContext().map(data, function, parallelism);
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

    @SuppressWarnings("unchecked")
    private List<Candidate> deserializeCandidates(byte[] bytes) {
        try {
            return InstantiationUtil.deserializeObject(
                    bytes, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize primary-key vector candidates.", e);
        }
    }
}
