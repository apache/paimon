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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.transform.BucketStrategy;
import org.apache.paimon.transform.Truncate;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A bucket id extractor to extract bucket id according to given {@link BucketStrategy}. */
public class StrategyBasedBucketIdExtractor {
    private final BucketStrategy<Integer> bucketStrategy;

    public StrategyBasedBucketIdExtractor(BucketStrategy<?> bucketStrategy, RowType bucketKeyType) {
        if (bucketStrategy instanceof Truncate) {
            checkArgument(
                    bucketKeyType.getFieldCount() == 1,
                    "bucketKeys must contain exactly one key when use truncate strategy.");
            checkArgument(
                    bucketKeyType.getTypeAt(0).getTypeRoot() == DataTypeRoot.INTEGER,
                    "when use truncate strategy, only integer bucket key type are supported currently. ");
            // must be BucketStrategy<Integer>, cast directly
            //noinspection unchecked
            this.bucketStrategy = (BucketStrategy<Integer>) bucketStrategy;
        } else {
            throw new IllegalArgumentException("Unsupported bucket strategy: " + bucketStrategy);
        }
    }

    public int extractBucket(InternalRow bucketRow) {
        // must be truncate integer
        return bucketStrategy.apply(bucketRow.getInt(0));
    }
}
