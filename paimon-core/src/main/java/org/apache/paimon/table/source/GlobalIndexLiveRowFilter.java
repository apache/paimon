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

import org.apache.paimon.Snapshot;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.TimeTravelUtil;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Live-row filtering shared by global-index based search readers. */
class GlobalIndexLiveRowFilter {

    @Nullable
    static RoaringNavigableMap64 liveRows(
            @Nullable FileStoreTable table, @Nullable PartitionPredicate partitionFilter) {
        return liveRows(table, null, partitionFilter, null);
    }

    @Nullable
    static RoaringNavigableMap64 liveRows(
            @Nullable FileStoreTable table,
            @Nullable Snapshot pinnedSnapshot,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable List<Range> rowRanges) {
        if (table == null || !table.coreOptions().deletionVectorsEnabled()) {
            return null;
        }

        // Pin to the index scan's snapshot so row-ids and live rows agree.
        @Nullable
        Snapshot snapshot =
                pinnedSnapshot != null ? pinnedSnapshot : TimeTravelUtil.tryTravelOrLatest(table);
        if (snapshot == null) {
            return null;
        }

        SnapshotReader snapshotReader =
                table.newSnapshotReader()
                        .withPartitionFilter(partitionFilter)
                        .withMode(ScanMode.ALL)
                        .withSnapshot(snapshot);
        if (rowRanges != null) {
            snapshotReader.withRowRanges(rowRanges);
        }

        List<Split> splits = snapshotReader.read().splits();
        RoaringNavigableMap64 liveRows = new RoaringNavigableMap64();
        // Phase 1: union every file's row-id range.
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                addRowRanges(liveRows, (DataSplit) split);
            }
        }
        // Phase 2: subtract each DV. Ranges are all unioned first (no re-add),
        // and peak memory stays at one DV at a time.
        for (Split split : splits) {
            if (split instanceof DataSplit) {
                subtractDeletedRows(table, liveRows, (DataSplit) split);
            }
        }
        return liveRows;
    }

    @Nullable
    static RoaringNavigableMap64 forRange(
            @Nullable RoaringNavigableMap64 liveRows, long from, long to) {
        if (liveRows == null) {
            return null;
        }

        Range range = new Range(from, to);
        RoaringNavigableMap64 includeRows = new RoaringNavigableMap64();
        includeRows.addRange(range);
        includeRows.and(liveRows);
        return includeRows.getLongCardinality() == range.count() ? null : includeRows;
    }

    private static void addRowRanges(RoaringNavigableMap64 liveRows, DataSplit split) {
        for (DataFileMeta file : split.dataFiles()) {
            if (file.firstRowId() != null) {
                liveRows.addRange(file.nonNullRowIdRange());
            }
        }
    }

    private static void subtractDeletedRows(
            FileStoreTable table, RoaringNavigableMap64 liveRows, DataSplit split) {
        List<DataFileMeta> files = split.dataFiles();
        List<DeletionFile> deletionFiles = split.deletionFiles().orElse(null);
        DeletionVector.Factory deletionVectorFactory =
                DeletionVector.factory(table.fileIO(), files, deletionFiles);
        for (DataFileMeta file : files) {
            if (file.firstRowId() == null) {
                continue;
            }
            long firstRowId = file.nonNullFirstRowId();

            Optional<DeletionVector> deletionVector;
            try {
                deletionVector = deletionVectorFactory.create(file.fileName());
            } catch (IOException e) {
                throw new RuntimeException(
                        "Failed to read deletion vector for file " + file.fileName(), e);
            }
            if (!deletionVector.isPresent() || deletionVector.get().isEmpty()) {
                continue;
            }

            RoaringNavigableMap64 deletedRows = new RoaringNavigableMap64();
            deletionVector
                    .get()
                    .forEachDeletedPosition(position -> deletedRows.add(firstRowId + position));
            liveRows.andNot(deletedRows);
        }
    }

    private GlobalIndexLiveRowFilter() {}
}
