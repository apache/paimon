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

package org.apache.paimon.table;

import java.util.List;

/**
 * Bucket spec holds all bucket information, we can do plan optimization during table scan.
 *
 * <p>If the `bucketMode` is {@link BucketMode#HASH_DYNAMIC}, then `numBucket` is -1;
 *
 * @since 0.9
 */
public class BucketSpec {

    private final BucketMode bucketMode;
    private final List<String> bucketKeys;
    private final int numBuckets;

    public BucketSpec(BucketMode bucketMode, List<String> bucketKeys, int numBuckets) {
        this.bucketMode = bucketMode;
        this.bucketKeys = bucketKeys;
        this.numBuckets = numBuckets;
    }

    public BucketMode getBucketMode() {
        return bucketMode;
    }

    public List<String> getBucketKeys() {
        return bucketKeys;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    @Override
    public String toString() {
        return "BucketSpec{"
                + "bucketMode="
                + bucketMode
                + ", bucketKeys="
                + bucketKeys
                + ", numBuckets="
                + numBuckets
                + '}';
    }
}
