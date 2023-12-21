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

package org.apache.paimon.crosspartition;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Filter;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/** Bucket Assigner to assign bucket in a partition. */
public class BucketAssigner {

    private final Map<BinaryRow, TreeMap<Integer, Integer>> stats = new HashMap<>();

    public void bootstrapBucket(BinaryRow part, int bucket) {
        TreeMap<Integer, Integer> bucketMap = bucketMap(part);
        Integer count = bucketMap.get(bucket);
        if (count == null) {
            count = 0;
        }
        bucketMap.put(bucket, count + 1);
    }

    public int assignBucket(BinaryRow part, Filter<Integer> filter, int maxCount) {
        TreeMap<Integer, Integer> bucketMap = bucketMap(part);
        for (Map.Entry<Integer, Integer> entry : bucketMap.entrySet()) {
            int bucket = entry.getKey();
            int count = entry.getValue();
            if (filter.test(bucket) && count < maxCount) {
                bucketMap.put(bucket, count + 1);
                return bucket;
            }
        }

        for (int i = 0; ; i++) {
            if (filter.test(i) && !bucketMap.containsKey(i)) {
                bucketMap.put(i, 1);
                return i;
            }
        }
    }

    public void decrement(BinaryRow part, int bucket) {
        bucketMap(part).compute(bucket, (k, v) -> v == null ? 0 : v - 1);
    }

    private TreeMap<Integer, Integer> bucketMap(BinaryRow part) {
        TreeMap<Integer, Integer> map = stats.get(part);
        if (map == null) {
            map = new TreeMap<>();
            stats.put(part.copy(), map);
        }
        return map;
    }
}
