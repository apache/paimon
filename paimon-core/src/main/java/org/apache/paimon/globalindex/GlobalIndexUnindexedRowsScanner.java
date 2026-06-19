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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.RowRangeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.SCAN_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_VERSION;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT;
import static org.apache.paimon.table.SpecialFields.ROW_ID;
import static org.apache.paimon.table.SpecialFields.rowTypeWithRowId;

/** Scans raw rows whose row ids are not covered by a global index query. */
public class GlobalIndexUnindexedRowsScanner {

    private final FileStoreTable table;
    private final Snapshot snapshot;
    private final PartitionPredicate partitionFilter;
    private final Predicate filter;

    public GlobalIndexUnindexedRowsScanner(
            FileStoreTable table,
            Snapshot snapshot,
            PartitionPredicate partitionFilter,
            Predicate filter) {
        this.table = table;
        this.snapshot = snapshot;
        this.partitionFilter = partitionFilter;
        this.filter = filter;
    }

    public RoaringNavigableMap64 withUnindexedRows(RoaringNavigableMap64 indexedResultRows) {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        rows.or(indexedResultRows);
        rows.or(matchingRows(unindexedRanges()));
        return rows;
    }

    private List<Range> unindexedRanges() {
        if (snapshot == null || snapshot.nextRowId() == null || snapshot.nextRowId() <= 0) {
            return Collections.emptyList();
        }

        List<Range> dataRanges;
        if (table.coreOptions().globalIndexSearchMode() == GlobalIndexSearchMode.DETAIL) {
            dataRanges = dataRangesByDataFiles();
        } else {
            dataRanges = Collections.singletonList(new Range(0, snapshot.nextRowId() - 1));
        }

        List<Range> predicateIndexedRanges =
                GlobalIndexScanner.indexedRanges(table, partitionFilter, filter, snapshot);
        predicateIndexedRanges = Range.sortAndMergeOverlap(predicateIndexedRanges, true);

        List<Range> unindexedRanges = new ArrayList<>();
        for (Range dataRange : Range.sortAndMergeOverlap(dataRanges, true)) {
            unindexedRanges.addAll(dataRange.exclude(predicateIndexedRanges));
        }
        return Range.sortAndMergeOverlap(unindexedRanges, true);
    }

    private List<Range> dataRangesByDataFiles() {
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
                }
            }
        }
        return dataRanges;
    }

    private RoaringNavigableMap64 matchingRows(List<Range> ranges) {
        RoaringNavigableMap64 rows = new RoaringNavigableMap64();
        if (ranges.isEmpty()) {
            return rows;
        }

        RowType readType = rowTypeWithRowId(table.rowType());
        RowRangeIndex rowRangeIndex = RowRangeIndex.create(ranges);
        ReadBuilder readBuilder =
                table.copyWithoutTimeTravel(snapshotReadOptions())
                        .newReadBuilder()
                        .withReadType(readType)
                        .withFilter(filter)
                        .withPartitionFilter(partitionFilter);
        List<Split> splits = readBuilder.withRowRangeIndex(rowRangeIndex).newScan().plan().splits();
        int rowIdIndex = readType.getFieldIndex(ROW_ID.name());
        try {
            TableRead read = readBuilder.newRead();
            try (org.apache.paimon.reader.RecordReader<InternalRow> reader =
                    read.executeFilter().createReader(splits)) {
                reader.forEachRemaining(row -> rows.add(row.getLong(rowIdIndex)));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to scan unindexed data for global index raw search.", e);
        }
        return rows;
    }

    private Map<String, String> snapshotReadOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(SCAN_MODE.key(), FROM_SNAPSHOT.toString());
        options.put(SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
        options.put(SCAN_TAG_NAME.key(), null);
        options.put(SCAN_WATERMARK.key(), null);
        options.put(SCAN_TIMESTAMP.key(), null);
        options.put(SCAN_TIMESTAMP_MILLIS.key(), null);
        options.put(SCAN_FILE_CREATION_TIME_MILLIS.key(), null);
        options.put(SCAN_CREATION_TIME_MILLIS.key(), null);
        options.put(SCAN_VERSION.key(), null);
        return options;
    }
}
