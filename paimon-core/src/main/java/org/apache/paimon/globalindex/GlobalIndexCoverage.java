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

package org.apache.paimon.globalindex;

import org.apache.paimon.CoreOptions.GlobalIndexSearchMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.predicate.PredicateVisitor.collectFieldIds;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Row ranges covered and not covered by global index files. */
public class GlobalIndexCoverage {

    private final FileStoreTable table;
    @Nullable private final Snapshot snapshot;
    @Nullable private final PartitionPredicate partitionFilter;
    private final Map<Integer, List<Range>> coverageByField;
    @Nullable private List<Range> dataRangesByDataFiles;
    private boolean dataRangesComplete;

    public GlobalIndexCoverage(
            FileStoreTable table,
            @Nullable Snapshot snapshot,
            @Nullable PartitionPredicate partitionFilter,
            Collection<IndexFileMeta> indexFiles) {
        this.table = table;
        this.snapshot = snapshot;
        this.partitionFilter = partitionFilter;
        this.coverageByField = new HashMap<>();
        this.dataRangesComplete = true;
        for (IndexFileMeta indexFile : indexFiles) {
            GlobalIndexMeta meta = checkNotNull(indexFile.globalIndexMeta());
            if (meta.sourceMeta() != null) {
                continue;
            }
            Range range = new Range(meta.rowRangeStart(), meta.rowRangeEnd());
            addCoverage(meta.indexFieldId(), range);
            if (meta.extraFieldIds() != null) {
                for (int extra : meta.extraFieldIds()) {
                    addCoverage(extra, range);
                }
            }
        }
    }

    public List<Range> unindexedRanges(RowType rowType, @Nullable Predicate predicate) {
        return unindexedRanges(collectFieldIds(rowType, predicate));
    }

    public List<Range> unindexedRanges(int fieldId) {
        return unindexedRanges(Collections.singleton(fieldId));
    }

    public List<Range> unindexedRanges(Collection<Integer> fieldIds) {
        GlobalIndexSearchMode searchMode = table.coreOptions().globalIndexSearchMode();
        if (searchMode == GlobalIndexSearchMode.FAST) {
            return Collections.emptyList();
        }
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Collections.emptyList();
        }

        List<Range> dataRanges;
        if (searchMode == GlobalIndexSearchMode.DETAIL) {
            dataRanges = dataRangesByDataFiles();
        } else {
            dataRanges = Collections.singletonList(new Range(0, snapshot.nextRowId() - 1));
        }

        List<Range> predicateIndexedRanges =
                Range.sortAndMergeOverlap(indexedRanges(fieldIds), true);
        List<Range> unindexedRanges = new ArrayList<>();
        for (Range dataRange : Range.sortAndMergeOverlap(dataRanges, true)) {
            unindexedRanges.addAll(dataRange.exclude(predicateIndexedRanges));
        }
        return Range.sortAndMergeOverlap(unindexedRanges, true);
    }

    public List<Range> unindexedRangesForCorrectness(Collection<Integer> fieldIds) {
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Collections.emptyList();
        }

        List<Range> indexedRanges = Range.sortAndMergeOverlap(indexedRanges(fieldIds), true);
        List<Range> dataRanges = dataRangesByDataFiles();
        if (!dataRangesComplete) {
            return new Range(0, snapshot.nextRowId() - 1).exclude(indexedRanges);
        }

        List<Range> unindexedRanges = new ArrayList<>();
        for (Range dataRange : Range.sortAndMergeOverlap(dataRanges, true)) {
            unindexedRanges.addAll(dataRange.exclude(indexedRanges));
        }
        return Range.sortAndMergeOverlap(unindexedRanges, true);
    }

    private void addCoverage(int fieldId, Range range) {
        coverageByField.computeIfAbsent(fieldId, k -> new ArrayList<>()).add(range);
    }

    private List<Range> indexedRanges(Collection<Integer> fieldIds) {
        List<Range> ranges = null;
        for (Integer fieldId : fieldIds) {
            List<Range> fieldRanges = coverageByField.get(fieldId);
            if (fieldRanges == null || fieldRanges.isEmpty()) {
                return Collections.emptyList();
            }
            fieldRanges = Range.sortAndMergeOverlap(fieldRanges, true);
            ranges = ranges == null ? fieldRanges : Range.and(ranges, fieldRanges);
        }
        return ranges == null ? Collections.emptyList() : Range.sortAndMergeOverlap(ranges, true);
    }

    private List<Range> dataRangesByDataFiles() {
        if (dataRangesByDataFiles != null) {
            return dataRangesByDataFiles;
        }
        SnapshotReader snapshotReader =
                table.newSnapshotReader()
                        .withPartitionFilter(partitionFilter)
                        .withMode(ScanMode.ALL)
                        .withSnapshot(snapshot);
        List<Range> dataRanges = new ArrayList<>();
        for (Split split : snapshotReader.read().splits()) {
            if (!(split instanceof DataSplit)) {
                continue;
            }
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                if (file.firstRowId() != null) {
                    dataRanges.add(file.nonNullRowIdRange());
                } else {
                    dataRangesComplete = false;
                }
            }
        }
        dataRangesByDataFiles = dataRanges;
        return dataRangesByDataFiles;
    }
}
