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

package org.apache.paimon.flink.lookup.partitioner;

import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * {@link BucketShuffleStrategy} defines a strategy for determining the target subtask id for a
 * given bucket id and join key hash. It also provides a method to retrieve the set of bucket ids
 * that are required to be cached by a specific subtask.
 */
public class BucketShuffleStrategy implements ShuffleStrategy {

    private final int numBuckets;

    public BucketShuffleStrategy(int numBuckets) {
        checkState(numBuckets > 0, "Number of buckets should be positive.");
        this.numBuckets = numBuckets;
    }

    /**
     * Determines the target subtask ID for a given bucket ID and join key hash.
     *
     * <p>The method uses two different strategies based on the comparison between the number of
     * buckets and the number of subtasks:
     *
     * <p>1. If the number of buckets is greater than or equal to the number of subtasks (numBuckets
     * >= numSubtasks), then the target subtask ID is determined by assigning buckets to subtasks in
     * a round-robin manner:
     *
     * {@code
     * e.g., numBuckets = 5, numSubtasks = 3
     * Bucket 0 -> Subtask 0
     * Bucket 1 -> Subtask 1
     * Bucket 2 -> Subtask 2
     * Bucket 3 -> Subtask 0
     * Bucket 4 -> Subtask 1
     *
     * }
     *
     * <p>2. If the number of buckets is less than the number of subtasks (numBuckets <
     * numSubtasks), then the target subtask ID is determined by distributing subtasks to buckets in
     * a round-robin manner:
     *
     * {@code
     * e.g., numBuckets = 2, numSubtasks = 5
     * Bucket 0 -> Subtask 0,2,4
     * Bucket 1 -> Subtask 1,3
     *
     * }
     *
     * @param bucketId The ID of the bucket.
     * @param joinKeyHash The hash of the join key.
     * @param numSubtasks The total number of target subtasks.
     * @return The target subtask ID for the given bucket ID and join key hash.
     */
    public int getTargetSubtaskId(int bucketId, int joinKeyHash, int numSubtasks) {
        if (numBuckets >= numSubtasks) {
            return bucketId % numSubtasks;
        } else {
            // Calculate the number of ranges to group the subtasks
            int bucketNumMultipleRange = numSubtasks / numBuckets;
            int adjustedBucketNumMultipleRange;
            // If the start index of the current bucket plus the total range exceeds
            // the number of subtasks, use the calculated range without adjustment.
            // Example: bucketId = 1, numSubtasks = 4, numBuckets = 2
            if ((bucketId + bucketNumMultipleRange * numBuckets) >= numSubtasks) {
                adjustedBucketNumMultipleRange = bucketNumMultipleRange;
            } else {
                // Otherwise, increment the range by one to ensure all subtasks are included.
                // Example: bucketId = 0, numSubtasks = 5, numBuckets = 2
                adjustedBucketNumMultipleRange = bucketNumMultipleRange + 1;
            }
            return (joinKeyHash % adjustedBucketNumMultipleRange) * numBuckets + bucketId;
        }
    }

    /**
     * Retrieves the set of bucket IDs that are required to be cached by a specific subtask. This
     * method uses the same strategy as {@link #getTargetSubtaskId(int, int, int)} to ensure
     * consistency in bucket to subtask mapping.
     *
     * <p>The method uses two different strategies based on the comparison between the number of
     * buckets and the number of subtasks:
     *
     * <p>1. If the number of buckets is greater than or equal to the number of subtasks (numBuckets
     * >= numSubtasks), then each subtask caches buckets in a round-robin manner.
     *
     * {@code
     * e.g., numBuckets = 5, numSubtasks = 3
     * Subtask 0 -> Bucket 0,3
     * Subtask 1 -> Bucket 1,4
     * Subtask 2 -> Bucket 2
     *
     * }
     *
     * <p>2. If the number of buckets is less than the number of subtasks (numBuckets <
     * numSubtasks), then each subtask caches buckets by polling from buckets in a round-robin
     * manner:
     *
     * {@code
     * e.g., numBuckets = 2, numSubtasks = 5
     * Subtask 0 -> Bucket 0
     * Subtask 1 -> Bucket 1
     * Subtask 2 -> Bucket 0
     * Subtask 3 -> Bucket 1
     * Subtask 4 -> Bucket 0
     *
     * }
     *
     * @param subtaskId The ID of the subtask.
     * @param numSubtasks The total number of subtasks.
     * @return The set of bucket IDs that are required to be cached by the specified subtask.
     */
    public Set<Integer> getRequiredCacheBucketIds(int subtaskId, int numSubtasks) {
        Set<Integer> requiredCacheBucketIds = new HashSet<>();
        if (numBuckets >= numSubtasks) {
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                if (bucketId % numSubtasks == subtaskId) {
                    requiredCacheBucketIds.add(bucketId);
                }
            }
        } else {
            for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
                if (bucketId == subtaskId % numBuckets) {
                    requiredCacheBucketIds.add(bucketId);
                }
            }
        }
        return requiredCacheBucketIds;
    }
}
