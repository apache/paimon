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

import org.apache.paimon.append.ForceSingleBatchReader;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.reverseOrder;
import static java.util.Comparator.comparingLong;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Resolves blob placeholder rows by falling back through older sequence groups. The read logic is
 * as below:
 *
 * <ol>
 *   <li>Group files by max-seq, higher-seq files will have newer records.
 *   <li>Sort files by range within each group, and create sequential readers for them. Note that
 *       absent ranges will be read as all-placeholder rows.
 *   <li>Sort by max-seq and merge read all readers, records at each index will be the first
 *       non-placeholder blob.
 * </ol>
 */
public class BlobFallbackRecordReader implements RecordReader<InternalRow> {

    private final List<RecordReader<InternalRow>> groupReaders = new ArrayList<>();
    private final int blobIndex;
    private boolean returned;

    BlobFallbackRecordReader(
            List<DataFileMeta> files,
            BlobFileReaderFactory readerFactory,
            List<Range> rowRanges,
            RowType readRowType,
            int blobIndex) {
        this.blobIndex = blobIndex;

        checkArgument(!files.isEmpty(), "Blob bunch should not be empty.");
        long firstRowId = Long.MAX_VALUE;
        long lastRowId = Long.MIN_VALUE;

        // sort group readers in descending order
        Map<Long, List<DataFileMeta>> sequenceGroups = new TreeMap<>(reverseOrder());
        for (DataFileMeta file : files) {
            Range fileRange = file.nonNullRowIdRange();
            firstRowId = Math.min(firstRowId, fileRange.from);
            lastRowId = Math.max(lastRowId, fileRange.to);

            sequenceGroups
                    .computeIfAbsent(file.maxSequenceNumber(), ignored -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<Long, List<DataFileMeta>> entry : sequenceGroups.entrySet()) {
            // within each group, sort by first row id
            List<DataFileMeta> groupFiles = entry.getValue();
            groupFiles.sort(comparingLong(DataFileMeta::nonNullFirstRowId));

            DataFileMeta current, next;
            for (int i = 0; i < groupFiles.size() - 1; i++) {
                current = groupFiles.get(i);
                next = groupFiles.get(i + 1);

                Preconditions.checkState(
                        !current.nonNullRowIdRange().hasIntersection(next.nonNullRowIdRange()),
                        "Blob files within a same max_seq_num should not overlap. Find: %s, %s",
                        current,
                        next);
            }

            groupReaders.add(
                    new ForceSingleBatchReader(
                            new BlobSequenceGroupRecordReader(
                                    groupFiles,
                                    readerFactory,
                                    rowRanges,
                                    readRowType,
                                    blobIndex,
                                    firstRowId,
                                    lastRowId)));
        }
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (returned) {
            return null;
        }
        returned = true;

        // all readers are forced returning single batch
        RecordIterator<InternalRow>[] iterators = new RecordIterator[groupReaders.size()];
        for (int i = 0; i < groupReaders.size(); i++) {
            RecordIterator<InternalRow> iterator = groupReaders.get(i).readBatch();
            if (iterator == null) {
                if (i != 0) {
                    throw new IllegalStateException("All readers should be a single batch reader.");
                }
                for (int j = i + 1; j < groupReaders.size(); j++) {
                    if (groupReaders.get(j).readBatch() != null) {
                        throw new IllegalStateException(
                                "All readers should be a single batch reader.");
                    }
                }
                return null;
            }
            iterators[i] = iterator;
        }

        return new RecordIterator<InternalRow>() {
            @Nullable
            @Override
            public InternalRow next() throws IOException {
                InternalRow result = null;
                // We should always move each iterator forward
                // This may significantly increase memory usage and decrease read efficiency
                // if `blob-as-descriptor` is disabled and many non-null blobs are updated
                // TODO: Do not read stale records if there's a newer non-placeholder
                //   record. e.g. introduce a discard method to directly discard the
                //   next record?
                for (int i = 0; i < iterators.length; i++) {
                    RecordIterator<InternalRow> iterator = iterators[i];
                    InternalRow row = iterator.next();
                    if (row == null) {
                        if (i != 0) {
                            throw new IllegalStateException(
                                    "All readers of each max_seq group should have the same number of records.");
                        }
                        for (int j = i + 1; j < iterators.length; j++) {
                            if (iterators[j].next() != null) {
                                throw new IllegalStateException(
                                        "All readers of each max_seq group should have the same number of records.");
                            }
                        }
                        return null;
                    }
                    // result is the first non-placeholder record
                    if (result == null && !isPlaceHolder(row)) {
                        result = row;
                    }
                }
                if (result == null) {
                    throw new IllegalStateException(
                            "Invalid state: all blob files at the same row id store a placeholder, it's a bug.");
                }
                return result;
            }

            @Override
            public void releaseBatch() {
                for (RecordIterator<InternalRow> iterator : iterators) {
                    iterator.releaseBatch();
                }
            }
        };
    }

    private boolean isPlaceHolder(InternalRow row) {
        return !row.isNullAt(blobIndex) && row.getBlob(blobIndex) == BlobPlaceholder.INSTANCE;
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        for (RecordReader<InternalRow> reader : groupReaders) {
            try {
                reader.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Reads one blob sequence group (all blob files with the same max_seq_num) and emits
     * placeholder rows for row id gaps. For example, if the full row range is [0, 100], but there's
     * only one blob file with row range [20, 80], then the rows with row id [0, 19] and [81, 100]
     * will be emitted as placeholder rows.
     *
     * <p>This reader should always be fully consumed, or the internal states may be broken.
     *
     * <p>Note that we can not simply concat all data files and read them, even though we guarantee
     * writing the full-range data during data-evolution. The complexity is introduced by row-level
     * compaction. For example, if we execute following operations:
     *
     * <ol>
     *   <li>Write [0, 100], generate files [0, 50], [51, 100]
     *   <li>Update [0, 100] files, generate files [0, 25], [26, 75], [76, 100]
     *   <li>Insert new blobs for range [101, 200], generate files [101, 200]
     *   <li>Update new blobs for range [101, 200], generate files [101, 150], [151, 200]
     *   <li>Compact, merge [0, 100], [101, 200] to a single range
     *   <li>Update the compacted files, generate files [0, 200]
     * </ol>
     *
     * <p>The data files layout would be:
     *
     * <pre>
     *         |<-----------------------  merged range: row 0 ~ 200  --------------------------------->|
     *         |                                                                                       |
     *         ┌─────────────────────────┐┌──────────────────────┐
     *   seq1: │      file1 (0~50)       ││     file2 (51~100)   │           (empty on 101~200)
     *         └─────────────────────────┘└──────────────────────┘
     *         ┌─────────────┐┌──────────────────────┐┌──────────┐
     *   seq2: │ file3(0~25) ││    file4 (26~75)     ││f5(76~100)│           (empty on 101~200)
     *         └─────────────┘└──────────────────────┘└──────────┘
     *                                                            ┌────────────────────────────────────┐
     *   seq3:                (empty on 0~100)                    │         file6 (101~200)            │
     *                                                            └────────────────────────────────────┘
     *                                                            ┌─────────────────┐┌─────────────────┐
     *   seq4:                (empty on 0~100)                    │ file7 (101~150) ││ file8 (151~200) │
     *                                                            └─────────────────┘└─────────────────┘
     *         ┌───────────────────────────────────────────────────────────────────────────────────────┐
     *   seq6: │                              file9 (0~200)                                            │
     *         └───────────────────────────────────────────────────────────────────────────────────────┘
     * </pre>
     *
     * <p>We treat all gaps as full-placeholders, and correctly resolve pushed-ranges.
     */
    public static class BlobSequenceGroupRecordReader implements RecordReader<InternalRow> {

        private final List<DataFileMeta> files;
        private final BlobFileReaderFactory readerFactory;
        // pushed row ranges
        private final List<Range> rowRanges;
        private final RowType readRowType;
        private final int blobIndex;
        private final long lastRowId;

        private RecordReader<InternalRow> currentReader;
        private DataFileMeta currentFile;
        private int nextFileIndex;
        private int nextRowRangeIndex;
        // expected next row id
        private long nextRowId;

        private InternalRow placeholderRow;

        BlobSequenceGroupRecordReader(
                List<DataFileMeta> files,
                BlobFileReaderFactory readerFactory,
                List<Range> rowRanges,
                RowType readRowType,
                int blobIndex,
                long firstRowId,
                long lastRowId) {
            this.files = files;
            this.readerFactory = readerFactory;
            this.rowRanges = rowRanges == null ? null : Range.sortAndMergeOverlap(rowRanges);
            this.readRowType = readRowType;
            this.blobIndex = blobIndex;
            this.lastRowId = lastRowId;

            this.nextFileIndex = 0;
            this.nextRowRangeIndex = 0;
            setNextRowId(firstRowId);

            this.placeholderRow = null;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            while (true) {
                if (currentReader != null) {
                    RecordIterator<InternalRow> batch = currentReader.readBatch();
                    if (batch != null) {
                        return batch;
                    }
                    // row ranges have been pushed to readers
                    // directly set nextRowId as the lastRowId + 1
                    setNextRowId(lastRowId(currentFile) + 1);
                    closeCurrentFileReader();
                    continue;
                }

                if (nextRowId > lastRowId) {
                    return null;
                }

                // skip files whose ranges are before nextRowId
                while (nextFileIndex < files.size()
                        && lastRowId(files.get(nextFileIndex)) < nextRowId) {
                    nextFileIndex++;
                }
                if (nextFileIndex >= files.size()) {
                    return placeHolderBatch(lastRowId);
                }

                DataFileMeta nextFile = files.get(nextFileIndex);
                if (nextFile.nonNullFirstRowId() > nextRowId) {
                    return placeHolderBatch(nextFile.nonNullFirstRowId() - 1);
                }

                createReader(nextFile);
            }
        }

        /**
         * Set nextRowId and try to move to the next selected row id. So the final nextRowId may be
         * greater than the input value.
         */
        private void setNextRowId(long nextRowId) {
            this.nextRowId = nextRowId;
            tryMoveToSelectedRow();
        }

        private void tryMoveToSelectedRow() {
            if (nextRowId > lastRowId || rowRanges == null) {
                return;
            }

            while (nextRowRangeIndex < rowRanges.size()) {
                Range range = rowRanges.get(nextRowRangeIndex);
                if (nextRowId >= range.from && nextRowId <= range.to) {
                    // if nextRowId is within the range, do not need to move
                    return;
                } else if (nextRowId < range.from) {
                    // else if nextRowId < next range, move to next range's `from`
                    nextRowId = range.from;
                    return;
                }
                // else nextRowId > range.to, try next range
                nextRowRangeIndex++;
            }

            // all ranges consumed, no need to read
            nextRowId = lastRowId + 1;
        }

        private RecordIterator<InternalRow> placeHolderBatch(long endRowId) {
            return new RecordIterator<InternalRow>() {
                long rowId;

                @Nullable
                @Override
                public InternalRow next() {
                    rowId = nextRowId;
                    if (rowId > endRowId) {
                        return null;
                    }
                    setNextRowId(rowId + 1);
                    return placeHolderRow();
                }

                @Override
                public void releaseBatch() {
                    // nothing to release
                }
            };
        }

        private InternalRow placeHolderRow() {
            if (placeholderRow == null) {
                GenericRow row = new GenericRow(readRowType.getFieldCount());
                row.setField(blobIndex, BlobPlaceholder.INSTANCE);
                placeholderRow = row;
            }
            return placeholderRow;
        }

        private long lastRowId(DataFileMeta file) {
            return file.nonNullFirstRowId() + file.rowCount() - 1;
        }

        private void closeCurrentFileReader() throws IOException {
            if (currentReader != null) {
                currentReader.close();
                currentReader = null;
            }
            currentFile = null;
        }

        private void createReader(DataFileMeta nextFile) throws IOException {
            currentFile = nextFile;
            currentReader = readerFactory.create(nextFile);
            nextFileIndex++;
        }

        @Override
        public void close() throws IOException {
            closeCurrentFileReader();
        }
    }

    /** Factory to create readers. */
    interface BlobFileReaderFactory {
        RecordReader<InternalRow> create(DataFileMeta file) throws IOException;
    }
}
