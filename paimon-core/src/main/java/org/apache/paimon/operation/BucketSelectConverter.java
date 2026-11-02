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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.BucketFunctionType;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TriFilter;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;

/** Bucket filter push down in scan to skip files. */
public class BucketSelectConverter {

    private final BucketMode bucketMode;
    private final CoreOptions coreOptions;
    private final RowType rowType;
    private final RowType partitionType;
    private final RowType bucketKeyType;

    /**
     * @deprecated Use {@link #BucketSelectConverter(BucketMode, CoreOptions, RowType, RowType,
     *     RowType)} so custom bucket function providers are preserved.
     */
    @Deprecated
    public BucketSelectConverter(
            BucketMode bucketMode,
            BucketFunctionType bucketFunctionType,
            RowType rowType,
            RowType partitionType,
            RowType bucketKeyType) {
        this(
                bucketMode,
                new CoreOptions(
                        Collections.singletonMap(
                                CoreOptions.BUCKET_FUNCTION_TYPE.key(),
                                bucketFunctionType.toString())),
                rowType,
                partitionType,
                bucketKeyType);
    }

    public BucketSelectConverter(
            BucketMode bucketMode,
            CoreOptions coreOptions,
            RowType rowType,
            RowType partitionType,
            RowType bucketKeyType) {
        this.bucketMode = bucketMode;
        this.coreOptions = coreOptions;
        this.rowType = rowType;
        this.partitionType = partitionType;
        this.bucketKeyType = bucketKeyType;
    }

    public Optional<TriFilter<BinaryRow, Integer, Integer>> convert(Predicate predicate) {
        if (bucketMode != BucketMode.HASH_FIXED && bucketMode != BucketMode.POSTPONE_MODE) {
            return Optional.empty();
        }

        if (bucketKeyType.getFieldCount() == 0) {
            return Optional.empty();
        }

        Set<String> predicateFields = collectFieldNames(predicate);
        if (!predicateFields.containsAll(bucketKeyType.getFieldNames())) {
            return Optional.empty();
        }

        return Optional.of(
                new BucketSelector(predicate, coreOptions, rowType, partitionType, bucketKeyType));
    }
}
