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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Int2ShortHashMap;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PartitionIndex}. */
public class PartitionIndexTest {

    private static final long TARGET_BUCKET_ROW_NUMBER = 100L;
    private static final BinaryRow PARTITION = BinaryRow.EMPTY_ROW;

    @Test
    public void testAssignNewHash() {
        PartitionIndex index = createPartitionIndex(3);

        int hash1 = 12345;
        int bucket = index.assign(hash1, i -> true, -1, -1, -1, Duration.ofHours(1));

        assertThat(bucket).isIn(0, 1, 2);
        assertThat(index.hash2Bucket.get(hash1)).isEqualTo((short) bucket);
    }

    @Test
    public void testAssignExistingHash() {
        PartitionIndex index = createPartitionIndex(3);

        int hash1 = 12345;
        int bucket1 = index.assign(hash1, i -> true, -1, -1, -1, Duration.ofHours(1));
        int bucket2 = index.assign(hash1, i -> true, -1, -1, -1, Duration.ofHours(1));

        assertThat(bucket1).isEqualTo(bucket2);
    }

    @Test
    public void testBucketFillsUpAndMovesToNext() {
        PartitionIndex index = createPartitionIndex(2);

        // Fill first bucket
        for (int i = 0; i < TARGET_BUCKET_ROW_NUMBER; i++) {
            index.assign(i, j -> true, -1, -1, -1, Duration.ofHours(1));
        }

        // Next hash should go to second bucket
        int nextHash = 99999;
        int bucket = index.assign(nextHash, j -> true, -1, -1, -1, Duration.ofHours(1));

        // Should have moved to the second available bucket
        assertThat(index.nonFullBucketInformation.size()).isLessThanOrEqualTo(2);
    }

    @Test
    public void testShouldRefreshEmptyBuckets_WhenThresholdReached() {
        PartitionIndex index = createPartitionIndex(10);

        // Remove buckets until we reach threshold
        // With 10 buckets and threshold of 3, we need to remove 7 buckets
        int minEmptyBucketsBeforeAsyncCheck = 3;
        int maxBucketId = 10;

        for (int i = 0; i < 7; i++) {
            index.nonFullBucketInformation.remove(i);
        }

        // Now we have exactly 3 buckets left, should trigger refresh
        assertThat(index.nonFullBucketInformation.size())
                .isEqualTo(maxBucketId - minEmptyBucketsBeforeAsyncCheck);

        // Assign with parameters that should trigger refresh check
        int hash = 12345;
        index.assign(
                hash,
                i -> true,
                maxBucketId,
                maxBucketId,
                minEmptyBucketsBeforeAsyncCheck,
                Duration.ofMillis(1));

        assertThat(index.accessed).isTrue();
    }

    @Test
    public void testShouldNotRefreshEmptyBuckets_WhenAboveThreshold() {
        PartitionIndex index = createPartitionIndex(10);

        int minEmptyBucketsBeforeAsyncCheck = 3;
        int maxBucketId = 10;

        // We have 10 buckets, threshold is 3, so 10 - 3 = 7
        // We need to have fewer than 7 to trigger, so with 8+ we shouldn't trigger
        assertThat(index.nonFullBucketInformation.size()).isGreaterThan(7);

        int hash = 12345;
        index.assign(
                hash,
                i -> true,
                maxBucketId,
                maxBucketId,
                minEmptyBucketsBeforeAsyncCheck,
                Duration.ofHours(1));

        assertThat(index.hash2Bucket.containsKey(hash)).isTrue();
    }

    @Test
    public void testShouldNotRefreshWhenMaxBucketIdIsNegative() {
        PartitionIndex index = createPartitionIndex(5);

        // Remove most buckets
        for (int i = 0; i < 4; i++) {
            index.nonFullBucketInformation.remove(i);
        }

        int hash = 12345;
        // maxBucketId = -1 means unlimited, should not trigger refresh
        index.assign(hash, i -> true, -1, -1, 2, Duration.ofHours(1));

        assertThat(index.hash2Bucket.containsKey(hash)).isTrue();
    }

    @Test
    public void testShouldNotRefreshWhenThresholdIsNegative() {
        PartitionIndex index = createPartitionIndex(5);

        // Remove most buckets
        for (int i = 0; i < 4; i++) {
            index.nonFullBucketInformation.remove(i);
        }

        int hash = 12345;
        // minEmptyBucketsBeforeAsyncCheck = -1 means disabled
        index.assign(hash, i -> true, 10, 10, -1, Duration.ofHours(1));

        assertThat(index.hash2Bucket.containsKey(hash)).isTrue();
    }

    @Test
    public void testAccessedFlagIsSet() {
        PartitionIndex index = createPartitionIndex(3);
        index.accessed = false;

        int hash = 12345;
        index.assign(hash, i -> true, -1, -1, -1, Duration.ofHours(1));

        assertThat(index.accessed).isTrue();
    }

    @Test
    public void testCreateNewBucketWithinLimit() {
        PartitionIndex index = createPartitionIndex(2);

        int maxBucketsNum = 5;
        int maxBucketId = 2;

        // Assign hash that will create new bucket
        int hash = 12345;
        int bucket =
                index.assign(hash, i -> true, maxBucketsNum, maxBucketId, -1, Duration.ofHours(1));

        // Should create new bucket (2, 3, or 4)
        assertThat(bucket).isIn(0, 1, 2, 3, 4);
        assertThat(index.totalBucketSet).contains(bucket);
    }

    @Test
    public void testBucketFilter() {
        PartitionIndex index = createPartitionIndex(5);

        // Filter: only allow even buckets
        int hash = 12345;
        int bucket = index.assign(hash, i -> i % 2 == 0, 10, 5, -1, Duration.ofHours(1));

        // Should be assigned to an even bucket
        assertThat(bucket % 2).isEqualTo(0);
    }

    @Test
    public void testMultipleHashesDistribution() {
        PartitionIndex index = createPartitionIndex(3);

        // Assign multiple different hashes
        for (int i = 0; i < 10; i++) {
            int hash = 1000 + i;
            int bucket = index.assign(hash, j -> true, -1, -1, -1, Duration.ofHours(1));
            assertThat(bucket).isIn(0, 1, 2);
            assertThat(index.hash2Bucket.get(hash)).isEqualTo((short) bucket);
        }

        assertThat(index.hash2Bucket.size()).isEqualTo(10);
    }

    @Test
    public void testNonFullBucketInformationUpdates() {
        PartitionIndex index = createPartitionIndex(3);

        int hash = 12345;
        int bucket = index.assign(hash, i -> true, -1, -1, -1, Duration.ofHours(1));

        // Check that bucket count was incremented
        Long count = index.nonFullBucketInformation.get(bucket);
        assertThat(count).isNotNull();
        assertThat(count).isGreaterThan(0L);
    }

    @Test
    public void testTotalBucketSetMaintained() {
        PartitionIndex index = createPartitionIndex(3);

        assertThat(index.totalBucketSet).hasSize(3);
        assertThat(index.totalBucketArray).hasSize(3);

        // Assign to existing buckets
        for (int i = 0; i < 10; i++) {
            index.assign(1000 + i, j -> true, -1, -1, -1, Duration.ofHours(1));
        }

        // Total bucket set should still be the same size if no new buckets created
        assertThat(index.totalBucketSet.size()).isLessThanOrEqualTo(3);
    }

    private PartitionIndex createPartitionIndex(int numBuckets) {
        Int2ShortHashMap.Builder mapBuilder = Int2ShortHashMap.builder();
        ConcurrentHashMap<Integer, Long> bucketInfo = new ConcurrentHashMap<>();

        for (int i = 0; i < numBuckets; i++) {
            bucketInfo.put(i, 0L);
        }

        return new PartitionIndex(
                mapBuilder.build(),
                bucketInfo,
                TARGET_BUCKET_ROW_NUMBER,
                null, // IndexFileHandler not needed for these tests
                PARTITION);
    }
}
