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

import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/** A {@link FileRecordReader} which emits blob placeholder rows for a row-id range. */
class AllPlaceholdersRecordReader implements FileRecordReader<InternalRow> {

    private static final Path PLACEHOLDER_PATH = new Path("placeholder");

    private final long firstRowId;
    private final int fieldCount;
    private final int blobIndex;
    private final int rowIdIndex;
    private final int seqNumIndex;
    private final long sequenceNumber;
    private final List<Range> selectedRanges;
    private boolean returned;

    AllPlaceholdersRecordReader(
            long firstRowId,
            long rowCount,
            @Nullable List<Range> rowRanges,
            RowType readRowType,
            int blobIndex,
            long sequenceNumber) {
        this.firstRowId = firstRowId;
        this.fieldCount = readRowType.getFieldCount();
        this.blobIndex = blobIndex;
        this.rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        this.seqNumIndex = readRowType.getFieldIndex(SpecialFields.SEQUENCE_NUMBER.name());
        this.sequenceNumber = sequenceNumber;
        this.selectedRanges = selectedRanges(firstRowId, rowCount, rowRanges);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() {
        if (returned || selectedRanges.isEmpty()) {
            return null;
        }
        returned = true;
        return new PlaceholderIterator();
    }

    @Override
    public void close() {
        // nothing to close
    }

    private List<Range> selectedRanges(
            long firstRowId, long rowCount, @Nullable List<Range> rowRanges) {
        if (rowCount <= 0) {
            return Collections.emptyList();
        }

        List<Range> fullRange =
                Collections.singletonList(new Range(firstRowId, firstRowId + rowCount - 1));
        if (rowRanges == null) {
            return fullRange;
        }

        return Range.and(fullRange, Range.sortAndMergeOverlap(rowRanges));
    }

    private InternalRow placeholderRow(long rowId) {
        GenericRow row = new GenericRow(fieldCount);
        row.setField(blobIndex, BlobPlaceholder.INSTANCE);
        if (rowIdIndex >= 0) {
            row.setField(rowIdIndex, rowId);
        }
        if (seqNumIndex >= 0) {
            row.setField(seqNumIndex, sequenceNumber);
        }
        return row;
    }

    /** Iterator to emit placeholders with row ranges pushed. */
    private class PlaceholderIterator implements FileRecordIterator<InternalRow> {

        private int rangeIndex = 0;
        private long nextRowId = selectedRanges.get(0).from;
        private long returnedRowId = firstRowId - 1;

        @Override
        public long returnedPosition() {
            return returnedRowId - firstRowId;
        }

        @Override
        public Path filePath() {
            return PLACEHOLDER_PATH;
        }

        @Nullable
        @Override
        public InternalRow next() {
            while (rangeIndex < selectedRanges.size()) {
                Range range = selectedRanges.get(rangeIndex);
                if (nextRowId <= range.to) {
                    returnedRowId = nextRowId;
                    nextRowId++;
                    return placeholderRow(returnedRowId);
                }

                rangeIndex++;
                if (rangeIndex < selectedRanges.size()) {
                    nextRowId = selectedRanges.get(rangeIndex).from;
                }
            }
            return null;
        }

        @Override
        public void releaseBatch() {
            // nothing to release
        }
    }
}
