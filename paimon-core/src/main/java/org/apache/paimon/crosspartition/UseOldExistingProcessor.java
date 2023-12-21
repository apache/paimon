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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/** A {@link ExistingProcessor} to use old partition and bucket. */
public class UseOldExistingProcessor implements ExistingProcessor {

    private final ProjectToRowFunction setPartition;
    private final BiConsumer<InternalRow, Integer> collector;

    public UseOldExistingProcessor(
            ProjectToRowFunction setPartition, BiConsumer<InternalRow, Integer> collector) {
        this.setPartition = setPartition;
        this.collector = collector;
    }

    @Override
    public boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket) {
        InternalRow newValue = setPartition.apply(newRow, previousPart);
        collector.accept(newValue, previousBucket);
        return false;
    }

    @Override
    public void bulkLoadNewRecords(
            Function<SortOrder, RowIterator> iteratorFunction,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket) {
        RowIterator iterator = iteratorFunction.apply(SortOrder.ASCENDING);
        InternalRow row;
        BinaryRow currentKey = null;
        BinaryRow currentPartition = null;
        int currentBucket = -1;
        while ((row = iterator.next()) != null) {
            BinaryRow primaryKey = extractPrimary.apply(row);
            BinaryRow partition = extractPartition.apply(row);
            if (currentKey == null || !currentKey.equals(primaryKey)) {
                int bucket = assignBucket.apply(partition);
                collector.accept(row, bucket);
                currentKey = primaryKey.copy();
                currentPartition = partition.copy();
                currentBucket = bucket;
            } else {
                InternalRow newRow = setPartition.apply(row, currentPartition);
                collector.accept(newRow, currentBucket);
            }
        }
    }
}
