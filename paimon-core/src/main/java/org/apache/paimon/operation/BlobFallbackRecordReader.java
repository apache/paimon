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
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
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
            long rowCount,
            BlobFileReaderFactory readerFactory,
            List<Range> rowRanges,
            RowType readRowType,
            int blobIndex) {
        this.blobIndex = blobIndex;

        checkArgument(!files.isEmpty(), "Blob bunch should not be empty.");
        long firstRowId =
                files.stream().mapToLong(DataFileMeta::nonNullFirstRowId).min().getAsLong();
        long lastRowId = firstRowId + rowCount - 1;

        // sort descendent group readers in descending order
        Map<Long, List<DataFileMeta>> sequenceGroups = new TreeMap<>(reverseOrder());
        for (DataFileMeta file : files) {
            sequenceGroups
                    .computeIfAbsent(file.maxSequenceNumber(), ignored -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<Long, List<DataFileMeta>> entry : sequenceGroups.entrySet()) {
            // within each group, sort by first row id
            List<DataFileMeta> groupFiles = entry.getValue();
            groupFiles.sort(comparingLong(DataFileMeta::nonNullFirstRowId));
            groupReaders.add(
                    new ForceSingleBatchReader(
                            new BlobSequenceGroupRecordReader(
                                    groupFiles,
                                    readerFactory,
                                    rowRanges,
                                    readRowType,
                                    blobIndex,
                                    firstRowId,
                                    lastRowId,
                                    entry.getKey())));
        }
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (returned) {
            return null;
        }
        returned = true;

        RecordIterator<InternalRow>[] iterators = new RecordIterator[groupReaders.size()];
        for (int i = 0; i < groupReaders.size(); i++) {
            RecordIterator<InternalRow> iterator = groupReaders.get(i).readBatch();
            if (iterator == null) {
                return null;
            }
            iterators[i] = iterator;
        }

        return new RecordIterator<InternalRow>() {
            @Nullable
            @Override
            public InternalRow next() throws IOException {
                InternalRow result = null;
                // we should always move each iterator forward
                for (RecordIterator<InternalRow> iterator : iterators) {
                    InternalRow row = iterator.next();
                    if (row == null) {
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
        return !row.isNullAt(blobIndex) && row.getBlob(blobIndex) == Blob.PLACE_HOLDER;
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
     * will be emitted as placeholder rows (and also maybe enriched with special fields).
     */
    public static class BlobSequenceGroupRecordReader implements RecordReader<InternalRow> {

        private final List<DataFileMeta> files;
        private final BlobFileReaderFactory readerFactory;
        // pushed row ranges
        private final List<Range> rowRanges;
        private final RowType readRowType;
        private final int blobIndex;
        private final long lastRowId;
        private final long sequenceNumber;

        private RecordReader<InternalRow> currentReader;
        private DataFileMeta currentFile;
        private int fileIndex;
        private long nextRowId;

        BlobSequenceGroupRecordReader(
                List<DataFileMeta> files,
                BlobFileReaderFactory readerFactory,
                List<Range> rowRanges,
                RowType readRowType,
                int blobIndex,
                long firstRowId,
                long lastRowId,
                long sequenceNumber) {
            this.files = files;
            this.readerFactory = readerFactory;
            this.rowRanges = rowRanges == null ? null : Range.sortAndMergeOverlap(rowRanges);
            this.readRowType = readRowType;
            this.blobIndex = blobIndex;
            this.lastRowId = lastRowId;
            this.sequenceNumber = sequenceNumber;
            this.nextRowId = firstRowId;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            while (true) {
                nextRowId = nextSelectedRowId(nextRowId);
                if (nextRowId > lastRowId) {
                    return null;
                }

                if (currentReader != null) {
                    RecordIterator<InternalRow> batch = currentReader.readBatch();
                    if (batch != null) {
                        return batch;
                    }
                    long afterFile = lastRowId(currentFile) + 1;
                    closeCurrentFileReader();
                    nextRowId = afterFile;
                    continue;
                }

                while (fileIndex < files.size() && lastRowId(files.get(fileIndex)) < nextRowId) {
                    fileIndex++;
                }
                if (fileIndex >= files.size()) {
                    return placeHolderBatch(lastRowId);
                }

                DataFileMeta nextFile = files.get(fileIndex);
                if (nextFile.nonNullFirstRowId() > nextRowId) {
                    return placeHolderBatch(nextFile.nonNullFirstRowId() - 1);
                }

                currentFile = nextFile;
                currentReader = readerFactory.create(nextFile);
                fileIndex++;
            }
        }

        private long nextSelectedRowId(long rowId) {
            if (rowRanges == null) {
                return rowId;
            }
            for (Range range : rowRanges) {
                if (rowId < range.from) {
                    return range.from;
                }
                if (rowId <= range.to) {
                    return rowId;
                }
            }
            return lastRowId + 1;
        }

        private RecordIterator<InternalRow> placeHolderBatch(long endRowId) {
            long startRowId = nextRowId;
            nextRowId = endRowId + 1;
            return new RecordIterator<InternalRow>() {

                long rowId = startRowId;

                @Nullable
                @Override
                public InternalRow next() {
                    rowId = nextSelectedRowId(rowId);
                    if (rowId > endRowId) {
                        return null;
                    }
                    return placeHolderRow(rowId++);
                }

                @Override
                public void releaseBatch() {}
            };
        }

        private GenericRow placeHolderRow(long rowId) {
            GenericRow row = new GenericRow(readRowType.getFieldCount());
            row.setField(blobIndex, Blob.PLACE_HOLDER);
            for (int i = 0; i < readRowType.getFieldCount(); i++) {
                String fieldName = readRowType.getFieldNames().get(i);
                if (SpecialFields.ROW_ID.name().equals(fieldName)) {
                    row.setField(i, rowId);
                } else if (SpecialFields.SEQUENCE_NUMBER.name().equals(fieldName)) {
                    row.setField(i, sequenceNumber);
                }
            }
            return row;
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

        @Override
        public void close() throws IOException {
            closeCurrentFileReader();
        }
    }

    interface BlobFileReaderFactory {

        RecordReader<InternalRow> create(DataFileMeta file) throws IOException;
    }
}
