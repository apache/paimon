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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.PostponeUtils.PostponeBucketRouter;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Plan for an execution-engine postpone merge read. */
public final class PostponeMergePlan implements TableScan.Plan {

    private final List<DataSplit> realSplits;
    private final List<DataSplit> postponeSplits;
    private final PostponeBucketRouter bucketRouter;
    private final RowType keyType;
    private final RowType resultReadType;
    private final RowType mergeReadType;
    private final long numPotentialBuckets;

    PostponeMergePlan(
            List<DataSplit> realSplits,
            List<DataSplit> postponeSplits,
            PostponeBucketRouter bucketRouter,
            RowType keyType,
            RowType resultReadType,
            RowType mergeReadType) {
        this.realSplits = realSplits;
        this.postponeSplits = postponeSplits;
        this.bucketRouter = bucketRouter;
        this.keyType = keyType;
        this.resultReadType = resultReadType;
        this.mergeReadType = mergeReadType;
        this.numPotentialBuckets = numPotentialBuckets(realSplits, postponeSplits, bucketRouter);
    }

    public List<DataSplit> realSplits() {
        return realSplits;
    }

    public List<DataSplit> postponeSplits() {
        return postponeSplits;
    }

    @Override
    public List<Split> splits() {
        List<Split> splits = new ArrayList<>(realSplits);
        splits.addAll(postponeSplits);
        return splits;
    }

    public PostponeBucketRouter bucketRouter() {
        return bucketRouter;
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType resultReadType() {
        return resultReadType;
    }

    public RowType mergeReadType() {
        return mergeReadType;
    }

    /** Number of (partition, bucket) groups which this plan may merge. */
    public long numPotentialBuckets() {
        return numPotentialBuckets;
    }

    private static long numPotentialBuckets(
            List<DataSplit> realSplits,
            List<DataSplit> postponeSplits,
            PostponeBucketRouter bucketRouter) {
        Map<BinaryRow, Set<Integer>> realBuckets = new HashMap<>();
        for (DataSplit split : realSplits) {
            realBuckets
                    .computeIfAbsent(split.partition(), ignored -> new HashSet<>())
                    .add(split.bucket());
        }

        Set<BinaryRow> postponePartitions = new HashSet<>();
        for (DataSplit split : postponeSplits) {
            postponePartitions.add(split.partition());
        }

        long count = 0;
        for (Map.Entry<BinaryRow, Set<Integer>> entry : realBuckets.entrySet()) {
            BinaryRow partition = entry.getKey();
            count +=
                    postponePartitions.remove(partition)
                            ? bucketRouter.numBuckets(partition)
                            : entry.getValue().size();
        }
        for (BinaryRow partition : postponePartitions) {
            count += bucketRouter.numBuckets(partition);
        }
        return count;
    }
}
