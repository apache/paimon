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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;

import java.util.function.BiConsumer;
import java.util.function.Function;

/** Processor to process existing key. */
public interface ExistingProcessor {

    /** @return should process new record. */
    boolean processExists(InternalRow newRow, BinaryRow previousPart, int previousBucket);

    void bulkLoadNewRecords(
            Function<SortOrder, RowIterator> iteratorFunction,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket);

    static void bulkLoadCollectFirst(
            BiConsumer<InternalRow, Integer> collector,
            RowIterator iterator,
            Function<InternalRow, BinaryRow> extractPrimary,
            Function<InternalRow, BinaryRow> extractPartition,
            Function<BinaryRow, Integer> assignBucket) {
        InternalRow row;
        BinaryRow currentKey = null;
        while ((row = iterator.next()) != null) {
            BinaryRow primaryKey = extractPrimary.apply(row);
            if (currentKey == null || !currentKey.equals(primaryKey)) {
                collector.accept(row, assignBucket.apply(extractPartition.apply(row)));
                currentKey = primaryKey.copy();
            }
        }
    }

    static ExistingProcessor create(
            MergeEngine mergeEngine,
            ProjectToRowFunction setPartition,
            BucketAssigner bucketAssigner,
            BiConsumer<InternalRow, Integer> collector) {
        switch (mergeEngine) {
            case DEDUPLICATE:
                return new DeleteExistingProcessor(setPartition, bucketAssigner, collector);
            case PARTIAL_UPDATE:
            case AGGREGATE:
                return new UseOldExistingProcessor(setPartition, collector);
            case FIRST_ROW:
                return new SkipNewExistingProcessor(collector);
            default:
                throw new UnsupportedOperationException("Unsupported engine: " + mergeEngine);
        }
    }

    /** Input Order for sorting. */
    enum SortOrder {
        ASCENDING,
        DESCENDING,
    }
}
