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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexEvaluator;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.Contains;
import org.apache.paimon.predicate.EndsWith;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Like;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldIds;

/**
 * Evaluates a row predicate on the data for a given set of row ids and returns the ids that satisfy
 * it. Only the filter columns and the row id are read, so this is the exact counterpart of a scalar
 * global index lookup: a search can hand the result to its index as an include bitmap without
 * touching the index corpus.
 */
class FilteredRowIdReader {

    private final FileStoreTable table;
    @Nullable private final Snapshot planSnapshot;
    @Nullable private final PartitionPredicate partitionFilter;
    private final Predicate filter;

    FilteredRowIdReader(
            FileStoreTable table,
            @Nullable Snapshot planSnapshot,
            @Nullable PartitionPredicate partitionFilter,
            Predicate filter) {
        this.table = table;
        this.planSnapshot = planSnapshot;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
    }

    /**
     * Whether a scalar global index evaluation of {@code filter} is an exact match set. The global
     * index contract only promises candidates: {@link GlobalIndexEvaluator} drops a conjunct no
     * index can evaluate, and BTree answers {@code contains} / {@code endsWith} / {@code like} with
     * every non-null row. A search that ranks rows before the engine filters them must refine a
     * non-exact answer through {@link #matchingRowIds} first.
     */
    static boolean isExact(
            RowType rowType, Predicate filter, GlobalIndexEvaluator.Evaluation evaluation) {
        Set<Integer> filterFieldIds = collectFieldIds(rowType, filter);
        if (!evaluation.contributingFieldIds().containsAll(filterFieldIds)) {
            return false;
        }
        return !hasCandidateOnlyLeaf(filter);
    }

    private static boolean hasCandidateOnlyLeaf(Predicate predicate) {
        if (predicate instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                if (hasCandidateOnlyLeaf(child)) {
                    return true;
                }
            }
            return false;
        }
        if (predicate instanceof LeafPredicate) {
            LeafFunction function = ((LeafPredicate) predicate).function();
            return function instanceof Contains
                    || function instanceof EndsWith
                    || function instanceof Like;
        }
        return false;
    }

    /** The subset of {@code rows} whose data satisfies the filter. */
    RoaringNavigableMap64 matchingRowIds(RoaringNavigableMap64 rows) {
        RoaringNavigableMap64 matching = new RoaringNavigableMap64();
        if (rows.isEmpty()) {
            return matching;
        }
        List<Range> rowRanges = Range.sortAndMergeOverlap(rows.toRangeList(), true);
        RowType readType = readType();
        int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());

        TableScan.Plan plan =
                readBuilder(readType, false).withRowRanges(rowRanges).newScan().plan();
        try (RecordReader<InternalRow> reader =
                readBuilder(readType, true).newRead().executeFilter().createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        long rowId = row.getLong(rowIdIndex);
                        if (rows.contains(rowId)) {
                            matching.add(rowId);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("Failed to evaluate the row filter on table data.", e);
        }
        return matching;
    }

    private RowType readType() {
        RowType tableRowType = table.rowType();
        Set<String> filterFields = PredicateVisitor.collectFieldNames(filter);
        List<String> readFields = new ArrayList<>();
        for (String field : tableRowType.getFieldNames()) {
            if (filterFields.contains(field)) {
                readFields.add(field);
            }
        }
        return SpecialFields.rowTypeWithRowId(tableRowType.project(readFields));
    }

    private ReadBuilder readBuilder(RowType readType, boolean withFilter) {
        ReadBuilder readBuilder = readTable().newReadBuilder().withReadType(readType);
        if (partitionFilter != null) {
            readBuilder.withPartitionFilter(partitionFilter);
        }
        if (withFilter) {
            readBuilder.withFilter(filter);
        }
        return readBuilder;
    }

    private FileStoreTable readTable() {
        if (planSnapshot == null) {
            return table;
        }
        Map<String, String> pinOptions = new HashMap<>();
        pinOptions.put(
                CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.FROM_SNAPSHOT.toString());
        pinOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(planSnapshot.id()));
        pinOptions.put(CoreOptions.SCAN_VERSION.key(), null);
        pinOptions.put(CoreOptions.SCAN_TAG_NAME.key(), null);
        pinOptions.put(CoreOptions.SCAN_WATERMARK.key(), null);
        pinOptions.put(CoreOptions.SCAN_TIMESTAMP.key(), null);
        pinOptions.put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
        return table.copyWithoutTimeTravel(pinOptions);
    }
}
