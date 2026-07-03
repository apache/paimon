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

import org.apache.paimon.Snapshot;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Filters global row ids with deletion vectors from a snapshot. */
public class DeletionVectorRowIdFilter {

    private final FileStoreTable table;
    @Nullable private final Snapshot snapshot;
    @Nullable private final PartitionPredicate partitionFilter;

    public DeletionVectorRowIdFilter(
            FileStoreTable table,
            @Nullable Snapshot snapshot,
            @Nullable PartitionPredicate partitionFilter) {
        this.table = table;
        this.snapshot = snapshot;
        this.partitionFilter = partitionFilter;
    }

    public GlobalIndexResult filter(GlobalIndexResult result) {
        if (!shouldFilter(result.results())) {
            return result;
        }
        return GlobalIndexResult.create(filter(result.results()));
    }

    public ScoredGlobalIndexResult filter(ScoredGlobalIndexResult result) {
        if (!shouldFilter(result.results())) {
            return result;
        }
        return ScoredGlobalIndexResult.create(filter(result.results()), result.scoreGetter());
    }

    public RoaringNavigableMap64 deletedRows(List<Range> ranges) {
        RoaringNavigableMap64 deletedRows = new RoaringNavigableMap64();
        if (!table.coreOptions().deletionVectorsEnabled() || snapshot == null || ranges.isEmpty()) {
            return deletedRows;
        }

        List<Range> normalizedRanges = Range.sortAndMergeOverlap(ranges, true);
        if (normalizedRanges.isEmpty()) {
            return deletedRows;
        }

        RoaringNavigableMap64 candidateRows = bitmapOf(normalizedRanges);
        SnapshotReader reader = table.newSnapshotReader().withSnapshot(snapshot);
        if (partitionFilter != null) {
            reader = reader.withPartitionFilter(partitionFilter);
        }
        reader = reader.withRowRanges(normalizedRanges);

        try {
            for (DataSplit split : reader.read().dataSplits()) {
                addDeletedRows(deletedRows, candidateRows, split);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read deletion vectors for global row ids.", e);
        }
        return deletedRows;
    }

    private boolean shouldFilter(RoaringNavigableMap64 rowIds) {
        return table.coreOptions().deletionVectorsEnabled()
                && snapshot != null
                && !rowIds.isEmpty();
    }

    private RoaringNavigableMap64 filter(RoaringNavigableMap64 rowIds) {
        RoaringNavigableMap64 filtered =
                RoaringNavigableMap64.or(new RoaringNavigableMap64(), rowIds);
        filtered.andNot(deletedRows(rowIds.toRangeList()));
        return filtered;
    }

    private void addDeletedRows(
            RoaringNavigableMap64 deletedRows, RoaringNavigableMap64 candidateRows, DataSplit split)
            throws IOException {
        List<DataFileMeta> files = split.dataFiles();
        Optional<List<DeletionFile>> optionalDeletionFiles = split.deletionFiles();
        if (!optionalDeletionFiles.isPresent()) {
            return;
        }

        DeletionVector.Factory factory =
                DeletionVector.factory(table.fileIO(), files, optionalDeletionFiles.get());
        for (DataFileMeta file : files) {
            if (file.firstRowId() == null) {
                continue;
            }
            Optional<DeletionVector> optionalDeletionVector = factory.create(file.fileName());
            if (!optionalDeletionVector.isPresent()) {
                continue;
            }

            RoaringNavigableMap64 fileDeletedRows =
                    Bitmap64DeletionVector.fromDeletionVector(optionalDeletionVector.get())
                            .toRoaringNavigableMap64(file.nonNullFirstRowId());
            fileDeletedRows.and(bitmapOf(file.nonNullRowIdRange()));
            fileDeletedRows.and(candidateRows);
            deletedRows.or(fileDeletedRows);
        }
    }

    private static RoaringNavigableMap64 bitmapOf(Range range) {
        return bitmapOf(Collections.singletonList(range));
    }

    private static RoaringNavigableMap64 bitmapOf(List<Range> ranges) {
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        for (Range range : ranges) {
            result.addRange(range);
        }
        return result;
    }
}
