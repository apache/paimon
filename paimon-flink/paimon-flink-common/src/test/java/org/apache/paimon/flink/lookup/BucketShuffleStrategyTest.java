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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.flink.lookup.partitioner.BucketShuffleStrategy;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** The test for {@link BucketShuffleStrategy}. */
public class BucketShuffleStrategyTest {

    @Test
    public void testConstructorWithInvalidBuckets() {
        Throwable thrown = catchThrowable(() -> new BucketShuffleStrategy(0));
        assertThat(thrown)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Number of buckets should be positive.");
    }

    @Test
    public void testGetTargetSubtaskIdWithMoreBucketsThanSubtasks() {
        BucketShuffleStrategy bucketShuffleStrategy = new BucketShuffleStrategy(10);
        int targetSubtaskId = bucketShuffleStrategy.getTargetSubtaskId(5, 12345, 4);
        assertThat(targetSubtaskId).isEqualTo(1);
    }

    @Test
    public void testGetTargetSubtaskIdWithLessBucketsThanSubtasks() {
        BucketShuffleStrategy bucketShuffleStrategy1 = new BucketShuffleStrategy(3);
        int targetSubtaskId1 = bucketShuffleStrategy1.getTargetSubtaskId(2, 12345, 10);
        int expectedTargetSubtaskId1 = ((12345 % 3) * 3 + 2);
        assertThat(targetSubtaskId1).isEqualTo(expectedTargetSubtaskId1);
        BucketShuffleStrategy bucketShuffleStrategy2 = new BucketShuffleStrategy(2);
        int targetSubtaskId2 = bucketShuffleStrategy2.getTargetSubtaskId(0, 54321, 7);
        int expectedTargetSubtaskId2 = (54321 % 4) * 2;
        assertThat(targetSubtaskId2).isEqualTo(expectedTargetSubtaskId2);
    }

    @Test
    public void testGetRequiredCacheBucketIdsWithMoreBucketsThanSubtasks() {
        BucketShuffleStrategy bucketShuffleStrategy = new BucketShuffleStrategy(10);
        Set<Integer> bucketIds = bucketShuffleStrategy.getRequiredCacheBucketIds(1, 4);
        // Subtask ID 1 should cache all buckets where bucketId % 4 == 1
        assertThat(bucketIds).containsExactlyInAnyOrder(1, 5, 9);
    }

    @Test
    public void testGetRequiredCacheBucketIdsWithLessBucketsThanSubtasks() {
        BucketShuffleStrategy bucketShuffleStrategy = new BucketShuffleStrategy(3);
        Set<Integer> bucketIds = bucketShuffleStrategy.getRequiredCacheBucketIds(1, 10);
        // Subtask ID 1 should cache the bucket where bucketId == 1 % 3
        assertThat(bucketIds).containsExactlyInAnyOrder(1);
    }

    @Test
    public void testSymmetryOfGetTargetSubtaskIdAndGetRequiredCacheBucketIds() {
        for (int numBuckets = 1; numBuckets < 100; ++numBuckets) {
            for (int numSubtasks = 1; numSubtasks < 100; ++numSubtasks) {
                testCorrectness(numBuckets, numSubtasks);
            }
        }
    }

    private void testCorrectness(int numBuckets, int numSubtasks) {
        BucketShuffleStrategy bucketShuffleStrategy = new BucketShuffleStrategy(numBuckets);
        Set<Integer> allTargetSubtaskIds = new HashSet<>();
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            for (int joinKeyHash = 0; joinKeyHash < numSubtasks; ++joinKeyHash) {
                int subtaskId =
                        bucketShuffleStrategy.getTargetSubtaskId(
                                bucketId, joinKeyHash, numSubtasks);
                assertThat(subtaskId).isNotNegative().isLessThan(numSubtasks);
                allTargetSubtaskIds.add(subtaskId);
                Set<Integer> requiredCacheBucketIds =
                        bucketShuffleStrategy.getRequiredCacheBucketIds(subtaskId, numSubtasks);
                assertThat(requiredCacheBucketIds).contains(bucketId);
            }
        }
        assertThat(allTargetSubtaskIds).hasSize(numSubtasks);
    }
}
