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

import javax.annotation.Nullable;

/** Filter for bucket. */
public class BucketFilter {

    private final boolean onlyReadRealBuckets;
    private final @Nullable Integer specifiedBucket;
    private final @Nullable Filter<Integer> bucketFilter;
    private final @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter;

    public BucketFilter(
            boolean onlyReadRealBuckets,
            @Nullable Integer specifiedBucket,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter) {
        this.onlyReadRealBuckets = onlyReadRealBuckets;
        this.specifiedBucket = specifiedBucket;
        this.bucketFilter = bucketFilter;
        this.totalAwareBucketFilter = totalAwareBucketFilter;
    }

    public static @Nullable BucketFilter create(
            boolean onlyReadRealBuckets,
            @Nullable Integer specifiedBucket,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable BiFilter<Integer, Integer> totalAwareBucketFilter) {
        if (!onlyReadRealBuckets
                && specifiedBucket == null
                && bucketFilter == null
                && totalAwareBucketFilter == null) {
            return null;
        }

        return new BucketFilter(
                onlyReadRealBuckets, specifiedBucket, bucketFilter, totalAwareBucketFilter);
    }

    @Nullable
    public Integer specifiedBucket() {
        return specifiedBucket;
    }

    public boolean test(int bucket, int totalBucket) {
        if (onlyReadRealBuckets && bucket < 0) {
            return false;
        }
        if (specifiedBucket != null && bucket != specifiedBucket) {
            return false;
        }
        if (bucketFilter != null && !bucketFilter.test(bucket)) {
            return false;
        }
        return totalAwareBucketFilter == null || totalAwareBucketFilter.test(bucket, totalBucket);
    }
}
