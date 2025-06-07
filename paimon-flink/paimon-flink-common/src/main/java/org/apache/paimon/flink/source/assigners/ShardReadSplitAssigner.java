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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.flink.source.shardread.FileStoreSourceSplitWithDpp;
import org.apache.paimon.table.source.DataSplit;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;

/**
 * The SplitAssigner for shard read. Each reader will only be assigned an empty split, the real
 * splits will generate on TaskManager.
 */
public class ShardReadSplitAssigner extends PreAssignSplitAssigner {

    private final FileStoreSourceSplitGenerator splitGenerator =
            new FileStoreSourceSplitGenerator();

    public ShardReadSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits) {
        super(splitBatchSize, context, splits);
    }

    private ShardReadSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        super(partitionRowProjection, dynamicFilteringData, splitBatchSize, parallelism, splits);
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        /**
         * if a reader fail, we still add a mock split to pendingSplitAssignment, after the reader
         * restart, it will call StaticFileStoreSplitEnumerator#handleSplitRequest ->
         * ShardSourceReader#addSplits, then do withShard plan on TaskManagers.
         */
        LinkedList<FileStoreSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());

        FileStoreSourceSplit fileStoreSourceSplit =
                splitGenerator.createSplit(
                        DataSplit.builder()
                                .withPartition(EMPTY_ROW)
                                .withBucket(0)
                                .withDataFiles(Collections.emptyList())
                                .withBucketPath("")
                                .build());

        if (partitionRowProjection != null && dynamicFilteringData != null) {
            remainingSplits.add(
                    FileStoreSourceSplitWithDpp.fromFileStoreSourceSplit(
                            fileStoreSourceSplit, partitionRowProjection, dynamicFilteringData));
        } else {
            remainingSplits.add(fileStoreSourceSplit);
        }

        numberOfPendingSplits.getAndAdd(1);
    }

    @Override
    public Map<Integer, LinkedList<FileStoreSourceSplit>> createBatchFairSplitAssignment(
            Collection<FileStoreSourceSplit> splits, int numReaders) {
        if (splits.size() != numReaders) {
            throw new IllegalArgumentException(
                    "Error, splits.size() must be equal with numReaders."
                            + " splits.size() is : "
                            + splits.size()
                            + ", numReaders is : "
                            + numReaders);
        }

        Map<Integer, LinkedList<FileStoreSourceSplit>> assignment = new HashMap<>(numReaders);

        int readerIndex = 0;
        for (FileStoreSourceSplit fileStoreSourceSplit : splits) {
            LinkedList<FileStoreSourceSplit> splitList = new LinkedList<>();
            if (partitionRowProjection != null && dynamicFilteringData != null) {
                splitList.add(
                        FileStoreSourceSplitWithDpp.fromFileStoreSourceSplit(
                                fileStoreSourceSplit,
                                partitionRowProjection,
                                dynamicFilteringData));
            } else {
                splitList.add(fileStoreSourceSplit);
            }

            assignment.put(readerIndex++, splitList);
        }

        return assignment;
    }

    public SplitAssigner ofDynamicPartitionPruning(
            Projection partitionRowProjection, DynamicFilteringData dynamicFilteringData) {
        return new ShardReadSplitAssigner(
                splitBatchSize, parallelism, splits, partitionRowProjection, dynamicFilteringData);
    }
}
