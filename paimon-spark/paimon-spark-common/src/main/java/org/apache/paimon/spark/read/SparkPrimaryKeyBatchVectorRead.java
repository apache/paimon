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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.BucketVectorSearchSplit;
import org.apache.paimon.table.source.PrimaryKeyBatchVectorRead;
import org.apache.paimon.table.source.PrimaryKeyVectorRead;
import org.apache.paimon.table.source.PrimaryKeyVectorRead.SearchResult;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SerializableFunction;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Spark-aware {@link PrimaryKeyBatchVectorRead}. */
public class SparkPrimaryKeyBatchVectorRead extends PrimaryKeyBatchVectorRead {

    private static final long serialVersionUID = 1L;

    public SparkPrimaryKeyBatchVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[][] queries,
            int limit,
            Map<String, String> searchOptions,
            @Nullable Predicate filter) {
        super(table, vectorField, queries, limit, searchOptions, filter);
    }

    @Override
    protected List<SearchResult> searchAllBuckets(
            PrimaryKeyVectorRead firstRead, List<BucketVectorSearchSplit> splits) {
        int parallelism = sparkParallelism();
        if (splits.size() < parallelism * 2) {
            return super.searchAllBuckets(firstRead, splits);
        }

        List<byte[]> serializedSplits = new ArrayList<>(splits.size());
        for (BucketVectorSearchSplit split : splits) {
            try {
                serializedSplits.add(InstantiationUtil.serializeObject(split));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize primary-key vector split.", e);
            }
        }
        List<List<byte[]>> groups =
                SparkVectorSearchSupport.splitGroups(serializedSplits, parallelism);
        SerializableFunction<List<byte[]>, byte[]> task =
                group -> {
                    List<BucketVectorSearchSplit> taskSplits = new ArrayList<>(group.size());
                    for (byte[] bytes : group) {
                        taskSplits.add(
                                SparkVectorSearchSupport.deserialize(
                                        bytes, "Failed to deserialize primary-key vector split."));
                    }
                    try {
                        return InstantiationUtil.serializeObject(
                                new ArrayList<>(searchBucketGroup(firstRead, taskSplits)));
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to serialize primary-key vector batch candidates.", e);
                    }
                };
        List<byte[]> groupResults = mapInSpark(groups, task, groups.size());

        int queryCount = queries.length;
        List<List<SearchResult>> resultsByQuery = new ArrayList<>(queryCount);
        for (int i = 0; i < queryCount; i++) {
            resultsByQuery.add(new ArrayList<>());
        }
        for (byte[] groupResult : groupResults) {
            List<SearchResult> groupSearchResults =
                    SparkVectorSearchSupport.deserialize(
                            groupResult,
                            "Failed to deserialize primary-key vector batch candidates.");
            checkArgument(
                    groupSearchResults.size() == queryCount,
                    "Primary-key vector batch result count does not match query count.");
            for (int i = 0; i < queryCount; i++) {
                resultsByQuery.get(i).add(groupSearchResults.get(i));
            }
        }
        return firstRead.mergeBatchGroupResults(resultsByQuery);
    }

    protected List<SearchResult> searchBucketGroup(
            PrimaryKeyVectorRead firstRead, List<BucketVectorSearchSplit> splits) {
        return firstRead.searchBuckets(splits, queries);
    }

    protected int sparkParallelism() {
        return SparkVectorSearchSupport.parallelism(table);
    }

    protected SparkEngineContext createEngineContext() {
        return SparkVectorSearchSupport.engineContext();
    }

    protected <I, O> List<O> mapInSpark(
            List<I> data, SerializableFunction<I, O> function, int parallelism) {
        return createEngineContext().map(data, function, parallelism);
    }
}
