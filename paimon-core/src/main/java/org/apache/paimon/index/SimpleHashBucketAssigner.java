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

package org.apache.paimon.index;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Int2ShortHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** When we need to overwrite the table, we should use this to avoid loading index. */
public class SimpleHashBucketAssigner implements BucketAssigner {

    private final int numAssigners;
    private final int assignId;
    private final long targetBucketRowNumber;

    private final Map<BinaryRow, SimplePartitionIndex> partitionIndex;

    public SimpleHashBucketAssigner(int numAssigners, int assignId, long targetBucketRowNumber) {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.partitionIndex = new HashMap<>();
    }

    @Override
    public int assign(BinaryRow partition, int hash) {
        SimplePartitionIndex index = partitionIndex.get(partition);
        if (index == null) {
            partition = partition.copy();
            index = new SimplePartitionIndex();
            this.partitionIndex.put(partition, index);
        }
        return index.assign(hash);
    }

    @Override
    public void prepareCommit(long commitIdentifier) {
        // do nothing
    }

    @VisibleForTesting
    Set<BinaryRow> currentPartitions() {
        return partitionIndex.keySet();
    }

    /** Simple partition bucket hash assigner. */
    private class SimplePartitionIndex {

        public final Int2ShortHashMap hash2Bucket = new Int2ShortHashMap();
        private final Map<Integer, Long> bucketInformation;
        private int currentBucket;

        private SimplePartitionIndex() {
            bucketInformation = new HashMap<>();
            loadNewBucket();
        }

        public int assign(int hash) {
            // the same hash should go into the same bucket
            if (hash2Bucket.containsKey(hash)) {
                return hash2Bucket.get(hash);
            }

            Long num = bucketInformation.computeIfAbsent(currentBucket, i -> 0L);
            if (num >= targetBucketRowNumber) {
                loadNewBucket();
            }
            bucketInformation.compute(currentBucket, (i, l) -> l == null ? 1L : l + 1);
            hash2Bucket.put(hash, (short) currentBucket);
            return currentBucket;
        }

        private void loadNewBucket() {
            for (int i = 0; i < Short.MAX_VALUE; i++) {
                if (i % numAssigners == assignId && !bucketInformation.containsKey(i)) {
                    currentBucket = i;
                    return;
                }
            }
            throw new RuntimeException(
                    "Can't find a suitable bucket to assign, all the bucket are assigned?");
        }
    }
}
