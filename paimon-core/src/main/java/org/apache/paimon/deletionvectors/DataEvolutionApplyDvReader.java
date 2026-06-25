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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * The RecordReader to apply deletion vectors for data evolution tables. At first, readType will be
 * enriched by `_ROW_ID`, then the returned id will be filtered by DVs.
 *
 * <p>This reader assumes that the underlying reader will return monotonically incrementing
 * _ROW_IDs, which is guaranteed by the current implementation.
 */
public class DataEvolutionApplyDvReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;
    private final List<RowRangeDeletionVector> deletionVectors;
    @Nullable private final ProjectedRow projectedRow;
    private final int rowIdIndex;

    private long lastRowId = -1;
    private int nextDvIndex;
    private RowRangeDeletionVector currentDv;

    public DataEvolutionApplyDvReader(RecordReader<InternalRow> reader, Info info) {
        this.reader = reader;
        this.deletionVectors = new ArrayList<>(info.deletionVectors);
        this.deletionVectors.sort(Comparator.comparingLong(dv -> dv.range.from));
        this.rowIdIndex = info.rowIdIndex;
        this.projectedRow = info.projectedRow;

        this.nextDvIndex = 1;
        this.currentDv = deletionVectors.get(0);
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        return new RecordIterator<InternalRow>() {

            @Nullable
            @Override
            public InternalRow next() throws IOException {
                while (true) {
                    InternalRow row = iterator.next();
                    if (row == null) {
                        return null;
                    }

                    if (!isDeleted(row)) {
                        if (projectedRow != null) {
                            return projectedRow.replaceRow(row);
                        }
                        return row;
                    }
                }
            }

            @Override
            public void releaseBatch() {
                iterator.releaseBatch();
            }
        };
    }

    private boolean isDeleted(InternalRow row) {
        long rowId = row.getLong(rowIdIndex);
        checkRowIdMonotonicity(rowId);

        moveToPossibleDv(rowId);

        if (currentDv == null || !currentDv.mayContains(rowId)) {
            return false;
        }

        return currentDv.isDeleted(rowId);
    }

    private void checkRowIdMonotonicity(long rowId) {
        if (lastRowId >= 0) {
            Preconditions.checkState(
                    rowId > lastRowId,
                    "This reader works only if underlying reader produces incremental _ROW_IDs.");
        }

        lastRowId = rowId;
    }

    private void moveToPossibleDv(long rowId) {
        if (currentDv == null) {
            return;
        }

        while (rowId > currentDv.range.to) {
            if (nextDvIndex >= deletionVectors.size()) {
                currentDv = null;
                return;
            }
            currentDv = deletionVectors.get(nextDvIndex);
            nextDvIndex++;
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static Info readInfo(
            FileIO fileIO, RowType readRowType, Map<Range, DeletionFile> deletionFiles)
            throws IOException {
        if (deletionFiles == null || deletionFiles.isEmpty()) {
            return Info.noDeletionVectors(readRowType);
        }

        List<RowRangeDeletionVector> deletionVectors = new ArrayList<>(deletionFiles.size());
        for (Map.Entry<Range, DeletionFile> entry : deletionFiles.entrySet()) {
            DeletionVector deletionVector = DeletionVector.read(fileIO, entry.getValue());
            if (!deletionVector.isEmpty()) {
                deletionVectors.add(new RowRangeDeletionVector(entry.getKey(), deletionVector));
            }
        }
        if (deletionVectors.isEmpty()) {
            return Info.noDeletionVectors(readRowType);
        }

        int rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        RowType actualReadType = readRowType;
        ProjectedRow projectedRow = null;
        if (rowIdIndex == -1) {
            actualReadType = SpecialFields.rowTypeWithRowId(readRowType);
            rowIdIndex = actualReadType.getFieldCount() - 1;
            int[] mappings = new int[readRowType.getFieldCount()];
            for (int i = 0; i < readRowType.getFieldCount(); i++) {
                mappings[i] = i;
            }
            projectedRow = ProjectedRow.from(mappings);
        }

        return new Info(deletionVectors, rowIdIndex, actualReadType, projectedRow);
    }

    /** Information for data evolution deletion vector applying. */
    public static class Info {

        private final List<RowRangeDeletionVector> deletionVectors;
        private final int rowIdIndex;
        public final RowType actualReadType;
        @Nullable private final ProjectedRow projectedRow;

        private Info(
                List<RowRangeDeletionVector> deletionVectors,
                int rowIdIndex,
                RowType actualReadType,
                @Nullable ProjectedRow projectedRow) {
            this.deletionVectors = deletionVectors;
            this.rowIdIndex = rowIdIndex;
            this.actualReadType = actualReadType;
            this.projectedRow = projectedRow;
        }

        private static Info noDeletionVectors(RowType readRowType) {
            return new Info(Collections.emptyList(), -1, readRowType, null);
        }

        public boolean hasDeletionVectors() {
            return !deletionVectors.isEmpty();
        }
    }

    /** Deletion Vector and range pair. */
    private static class RowRangeDeletionVector {

        private final Range range;
        private final DeletionVector deletionVector;

        private RowRangeDeletionVector(Range range, DeletionVector deletionVector) {
            this.range = range;
            this.deletionVector = deletionVector;
        }

        boolean mayContains(long rowId) {
            return rowId <= range.to && rowId >= range.from;
        }

        boolean isDeleted(long rowId) {
            return deletionVector.isDeleted(rowId - range.from);
        }
    }
}
