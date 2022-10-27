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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;

import java.util.ArrayList;
import java.util.List;

/** A {@link BucketComputer} to compute bucket by round-robin from task buckets. */
public class RoundRobinBucketComputer implements BucketComputer {

    private final List<Integer> buckets;

    private int nextIndex = -1;

    public RoundRobinBucketComputer(int taskId, int numTasks, int numBucket) {
        if (numTasks > numBucket) {
            throw new RuntimeException(
                    String.format(
                            "The number of tasks (%s) is more than that of buckets (%s),"
                                    + " which is not allowed. You can decrease the parallelism of sink.",
                            numTasks, numBucket));
        }

        this.buckets = new ArrayList<>();
        for (int i = 0; i < numBucket; i++) {
            if (i % numTasks == taskId) {
                buckets.add(i);
            }
        }
    }

    @Override
    public int bucket(RowData row) {
        nextIndex = (nextIndex + 1) % buckets.size();
        return buckets.get(nextIndex);
    }

    @Override
    public int bucket(RowData row, BinaryRowData pk) {
        return bucket(row);
    }
}
