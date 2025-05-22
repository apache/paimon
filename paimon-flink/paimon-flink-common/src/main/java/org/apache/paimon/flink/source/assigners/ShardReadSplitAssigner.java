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

import org.apache.paimon.flink.source.FileStoreSourceSplit;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * The SplitAssigner for shard read. Each reader will only be assigned an empty split, the real
 * splits will generate on TaskManager.
 */
public class ShardReadSplitAssigner extends PreAssignSplitAssigner {

    public ShardReadSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits) {
        super(splitBatchSize, context, splits);
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
            splitList.add(fileStoreSourceSplit);
            assignment.put(readerIndex++, splitList);
        }

        return assignment;
    }
}
