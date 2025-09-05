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

package org.apache.paimon.manifest;

import org.apache.paimon.utils.BiFilter;
import org.apache.paimon.utils.Filter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BucketFilter}. */
public class BucketFilterTest {

    @Test
    public void testCreateWithAllNullParameters() {
        // Test that create method returns null when all parameters are null/false
        assertThat(BucketFilter.create(false, null, null, null)).isNull();
    }

    @Test
    public void testCreateWithOnlyReadRealBuckets() {
        // Test that create method returns a BucketFilter when onlyReadRealBuckets is true
        BucketFilter filter = BucketFilter.create(true, null, null, null);
        assertThat(filter).isNotNull();
        assertThat(filter.specifiedBucket()).isNull();
    }

    @Test
    public void testCreateWithSpecifiedBucket() {
        // Test that create method returns a BucketFilter when specifiedBucket is not null
        BucketFilter filter = BucketFilter.create(false, 1, null, null);
        assertThat(filter).isNotNull();
        assertThat(filter.specifiedBucket()).isEqualTo(1);
    }

    @Test
    public void testCreateWithBucketFilter() {
        // Test that create method returns a BucketFilter when bucketFilter is not null
        Filter<Integer> bucketFilter = value -> value > 0;
        BucketFilter filter = BucketFilter.create(false, null, bucketFilter, null);
        assertThat(filter).isNotNull();
        assertThat(filter.specifiedBucket()).isNull();
    }

    @Test
    public void testCreateWithTotalAwareBucketFilter() {
        // Test that create method returns a BucketFilter when totalAwareBucketFilter is not null
        BiFilter<Integer, Integer> totalAwareBucketFilter =
                (bucket, totalBucket) -> bucket < totalBucket;
        BucketFilter filter = BucketFilter.create(false, null, null, totalAwareBucketFilter);
        assertThat(filter).isNotNull();
        assertThat(filter.specifiedBucket()).isNull();
    }

    @Test
    public void testTestWithOnlyReadRealBuckets() {
        // Test the test method with onlyReadRealBuckets parameter
        BucketFilter filter = BucketFilter.create(true, null, null, null);

        // Real buckets (non-negative) should pass
        assertThat(filter.test(0, 1)).isTrue();
        assertThat(filter.test(1, 2)).isTrue();

        // Virtual buckets (negative) should not pass
        assertThat(filter.test(-1, 1)).isFalse();
        assertThat(filter.test(-2, 2)).isFalse();
    }

    @Test
    public void testTestWithSpecifiedBucket() {
        // Test the test method with specifiedBucket parameter
        BucketFilter filter = BucketFilter.create(false, 1, null, null);

        // Only the specified bucket should pass
        assertThat(filter.test(1, 2)).isTrue();

        // Other buckets should not pass
        assertThat(filter.test(0, 2)).isFalse();
        assertThat(filter.test(2, 3)).isFalse();
    }

    @Test
    public void testTestWithBucketFilter() {
        // Test the test method with bucketFilter parameter
        Filter<Integer> bucketFilter = value -> value % 2 == 0; // Even buckets only
        BucketFilter filter = BucketFilter.create(false, null, bucketFilter, null);

        // Even buckets should pass
        assertThat(filter.test(0, 1)).isTrue();
        assertThat(filter.test(2, 3)).isTrue();
        assertThat(filter.test(4, 5)).isTrue();

        // Odd buckets should not pass
        assertThat(filter.test(1, 2)).isFalse();
        assertThat(filter.test(3, 4)).isFalse();
        assertThat(filter.test(5, 6)).isFalse();
    }

    @Test
    public void testTestWithTotalAwareBucketFilter() {
        // Test the test method with totalAwareBucketFilter parameter
        BiFilter<Integer, Integer> totalAwareBucketFilter =
                (bucket, totalBucket) -> bucket < totalBucket / 2;
        BucketFilter filter = BucketFilter.create(false, null, null, totalAwareBucketFilter);

        // Buckets less than half of totalBucket should pass
        assertThat(filter.test(0, 4)).isTrue();
        assertThat(filter.test(1, 4)).isTrue();

        // Buckets greater than or equal to half of totalBucket should not pass
        assertThat(filter.test(2, 4)).isFalse();
        assertThat(filter.test(3, 4)).isFalse();
    }

    @Test
    public void testTestWithMultipleFilters() {
        // Test the test method with multiple filters combined
        Filter<Integer> bucketFilter = value -> value > 0; // Positive buckets only
        BiFilter<Integer, Integer> totalAwareBucketFilter =
                (bucket, totalBucket) -> bucket < totalBucket - 1;
        BucketFilter filter = BucketFilter.create(true, 1, bucketFilter, totalAwareBucketFilter);

        // Bucket 1 is positive, is the specified bucket, and is less than totalBucket-1 for
        // totalBucket=3
        assertThat(filter.test(1, 3)).isTrue();

        // Bucket 0 is not positive, so it should not pass
        assertThat(filter.test(0, 3)).isFalse();

        // Bucket 2 is not the specified bucket, so it should not pass
        assertThat(filter.test(2, 3)).isFalse();

        // Bucket 1 with totalBucket=2 should not pass because 1 >= 2-1
        assertThat(filter.test(1, 2)).isFalse();

        // Negative bucket should not pass because onlyReadRealBuckets is true
        assertThat(filter.test(-1, 3)).isFalse();
    }

    @Test
    public void testSpecifiedBucket() {
        // Test the specifiedBucket method
        BucketFilter filterWithSpecifiedBucket = BucketFilter.create(false, 2, null, null);
        assertThat(filterWithSpecifiedBucket.specifiedBucket()).isEqualTo(2);
    }
}
