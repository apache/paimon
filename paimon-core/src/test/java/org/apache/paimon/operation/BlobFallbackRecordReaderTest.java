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

import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobPlaceholder;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.BlobFallbackRecordReader.BlobSequenceGroupRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobFallbackRecordReader}. */
public class BlobFallbackRecordReaderTest {

    private static final int BLOB_INDEX = 0;
    private static final String BLOB_FIELD = "blob_col";
    private static final RowType READ_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(BLOB_INDEX, BLOB_FIELD, DataTypes.BLOB()),
                            new DataField(1, SpecialFields.ROW_ID.name(), DataTypes.BIGINT()),
                            new DataField(
                                    2, SpecialFields.SEQUENCE_NUMBER.name(), DataTypes.BIGINT())));

    @Test
    public void testBlobSequenceGroupReaderWithRowRanges() throws Exception {
        List<DataFileMeta> files =
                Arrays.asList(blobFile("blob1", 0, 3, 10), blobFile("blob2", 5, 2, 10));
        List<Range> rowRanges = ranges(1, 1, 3, 5);

        ReadResult rows = readSequenceGroup(files, rowRanges, 0, 6, 10);

        assertThat(rows.rowIds).containsExactly(1L, 5L);
        assertThat(rows.placeholderRowCount).isEqualTo(2);
        assertThat(rows.batchSizes).containsExactly(1, 2, 1);
    }

    @Test
    public void testBlobSequenceGroupReaderWithMultipleRangesInFileAndGap() throws Exception {
        DataFileMeta wideFile = blobFile("wide-file", 20, 31, 10);
        List<Range> wideFileRanges = ranges(9, 10, 19, 20, 25, 30, 35, 40, 43, 45, 48, 52, 55, 56);

        ReadResult wideFileRows =
                readSequenceGroup(Collections.singletonList(wideFile), wideFileRanges, 10, 60, 10);

        List<Long> wideFileActualRowIds = rowIdsInRanges(20, 50, wideFileRanges);
        assertThat(wideFileRows.rowIds).containsExactlyElementsOf(wideFileActualRowIds);
        assertThat(wideFileRows.placeholderRowCount)
                .isEqualTo(
                        rowIdsInRanges(10, 19, wideFileRanges).size()
                                + rowIdsInRanges(51, 60, wideFileRanges).size());
        assertThat(wideFileRows.batchSizes)
                .containsExactlyElementsOf(batchSizes(2, wideFileActualRowIds.size(), 1, 4));

        DataFileMeta firstFile = blobFile("first-file", 0, 11, 10);
        DataFileMeta secondFile = blobFile("second-file", 50, 11, 10);
        List<Range> gapRanges = ranges(0, 0, 9, 12, 25, 30, 35, 40, 43, 45, 48, 52, 58, 62);

        ReadResult gapRows =
                readSequenceGroup(Arrays.asList(firstFile, secondFile), gapRanges, 0, 62, 10);

        assertThat(gapRows.rowIds)
                .containsExactlyElementsOf(
                        concat(
                                rowIdsInRanges(0, 10, gapRanges),
                                rowIdsInRanges(50, 60, gapRanges)));
        assertThat(gapRows.placeholderRowCount)
                .isEqualTo(
                        rowIdsInRanges(11, 49, gapRanges).size()
                                + rowIdsInRanges(61, 62, gapRanges).size());
        assertThat(gapRows.batchSizes).containsExactly(1, 1, 1, 19, 1, 1, 1, 1, 1, 1, 2);
    }

    @Test
    public void testBlobFallbackRecordReader() throws Exception {
        DataFileMeta newFile = blobFile("new-file", 0, 3, 2);
        DataFileMeta oldFile = blobFile("old-file", 0, 5, 1);

        ReadResult rows =
                readFallback(Arrays.asList(newFile, oldFile), null, placeholderRows(newFile, 1));

        assertThat(rows.rowIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertThat(rows.sequenceNumbers).containsExactly(2L, 1L, 2L, 1L, 1L);
    }

    @Test
    public void testBlobFallbackRecordReaderDerivesRowIdBoundsFromFiles() throws Exception {
        DataFileMeta newFile = blobFile("new-file", 10, 2, 2);
        DataFileMeta oldFile = blobFile("old-file", 8, 6, 1);

        ReadResult rows =
                readFallback(Arrays.asList(newFile, oldFile), null, placeholderRows(newFile, 10));

        assertThat(rows.rowIds).containsExactly(8L, 9L, 10L, 11L, 12L, 13L);
        assertThat(rows.sequenceNumbers).containsExactly(1L, 1L, 1L, 2L, 1L, 1L);
    }

    @Test
    public void testBlobFallbackRecordReaderThrowsIfAllRowsArePlaceholders() {
        DataFileMeta newFile = blobFile("new-placeholder-file", 0, 1, 2);
        DataFileMeta oldFile = blobFile("old-placeholder-file", 0, 1, 1);

        assertThatThrownBy(
                        () ->
                                readFallback(
                                        Arrays.asList(newFile, oldFile),
                                        null,
                                        placeholderRows(newFile, 0, oldFile, 0)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("all blob files at the same row id store a placeholder");
    }

    @Test
    public void testBlobFallbackRecordReaderWithRowRanges() throws Exception {
        DataFileMeta oldFile = blobFile("old-file", 20, 26, 1);
        DataFileMeta newFile1 = blobFile("new-file-1", 20, 11, 2);
        DataFileMeta newFile2 = blobFile("new-file-2", 40, 6, 2);
        List<Range> rowRanges = ranges(15, 20, 25, 26, 29, 33, 35, 41);

        ReadResult rows =
                readFallback(
                        Arrays.asList(newFile2, oldFile, newFile1),
                        rowRanges,
                        Collections.emptySet());

        assertThat(rows.rowIds)
                .containsExactly(
                        20L, 25L, 26L, 29L, 30L, 31L, 32L, 33L, 35L, 36L, 37L, 38L, 39L, 40L, 41L);
        assertThat(rows.sequenceNumbers)
                .containsExactly(2L, 2L, 2L, 2L, 2L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 2L, 2L);
    }

    private static ReadResult readFallback(
            List<DataFileMeta> files, List<Range> rowRanges, Set<String> placeholderRows)
            throws Exception {
        return ReadResult.read(
                new BlobFallbackRecordReader(
                        files,
                        file -> oneRowPerBatchReader(fileRows(file, rowRanges, placeholderRows)),
                        rowRanges,
                        READ_ROW_TYPE,
                        BLOB_INDEX));
    }

    private static ReadResult readSequenceGroup(
            List<DataFileMeta> files,
            List<Range> rowRanges,
            long firstRowId,
            long lastRowId,
            long sequenceNumber)
            throws Exception {
        return ReadResult.read(
                new BlobSequenceGroupRecordReader(
                        files,
                        file -> oneRowPerBatchReader(fileRows(file, rowRanges)),
                        rowRanges,
                        READ_ROW_TYPE,
                        BLOB_INDEX,
                        firstRowId,
                        lastRowId));
    }

    private static DataFileMeta blobFile(
            String fileName, long firstRowId, long rowCount, long maxSequenceNumber) {
        return DataFileMeta.create(
                fileName + ".blob",
                rowCount,
                rowCount,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                SimpleStats.EMPTY_STATS,
                SimpleStats.EMPTY_STATS,
                0,
                maxSequenceNumber,
                0L,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(System.currentTimeMillis()),
                rowCount,
                null,
                FileSource.APPEND,
                null,
                null,
                firstRowId,
                Arrays.asList(BLOB_FIELD));
    }

    private static List<Range> ranges(long... bounds) {
        if (bounds.length % 2 != 0) {
            throw new IllegalArgumentException("Range bounds should be paired.");
        }

        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < bounds.length; i += 2) {
            ranges.add(new Range(bounds[i], bounds[i + 1]));
        }
        return ranges;
    }

    private static List<InternalRow> fileRows(DataFileMeta file, List<Range> rowRanges) {
        return fileRows(file, rowRanges, Collections.emptySet());
    }

    private static List<InternalRow> fileRows(
            DataFileMeta file, List<Range> rowRanges, Set<String> placeholderRows) {
        List<InternalRow> rows = new ArrayList<>();
        long lastRowId = file.nonNullFirstRowId() + file.rowCount() - 1;
        for (long rowId = file.nonNullFirstRowId(); rowId <= lastRowId; rowId++) {
            if (selected(rowId, rowRanges)) {
                rows.add(
                        blobRow(
                                rowId,
                                file.maxSequenceNumber(),
                                placeholderRows.contains(rowKey(file, rowId))));
            }
        }
        return rows;
    }

    private static boolean selected(long rowId, List<Range> rowRanges) {
        if (rowRanges == null) {
            return true;
        }
        for (Range range : rowRanges) {
            if (rowId < range.from) {
                return false;
            }
            if (rowId <= range.to) {
                return true;
            }
        }
        return false;
    }

    private static List<Long> rowIdsInRanges(long firstRowId, long lastRowId, List<Range> ranges) {
        List<Long> rowIds = new ArrayList<>();
        for (long rowId = firstRowId; rowId <= lastRowId; rowId++) {
            if (selected(rowId, ranges)) {
                rowIds.add(rowId);
            }
        }
        return rowIds;
    }

    private static List<Long> concat(List<Long> first, List<Long> second) {
        List<Long> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }

    private static List<Integer> batchSizes(
            int firstBatchSize, int repeatedBatchCount, int repeatedBatchSize, int lastBatchSize) {
        List<Integer> batchSizes = new ArrayList<>();
        batchSizes.add(firstBatchSize);
        for (int i = 0; i < repeatedBatchCount; i++) {
            batchSizes.add(repeatedBatchSize);
        }
        batchSizes.add(lastBatchSize);
        return batchSizes;
    }

    private static Set<String> placeholderRows(DataFileMeta file, long rowId) {
        Set<String> keys = new HashSet<>();
        keys.add(rowKey(file, rowId));
        return keys;
    }

    private static Set<String> placeholderRows(
            DataFileMeta firstFile, long firstRowId, DataFileMeta secondFile, long secondRowId) {
        Set<String> keys = placeholderRows(firstFile, firstRowId);
        keys.add(rowKey(secondFile, secondRowId));
        return keys;
    }

    private static String rowKey(DataFileMeta file, long rowId) {
        return file.fileName() + "#" + rowId;
    }

    private static InternalRow blobRow(long rowId, long sequenceNumber, boolean placeholder) {
        GenericRow row = new GenericRow(3);
        row.setField(
                BLOB_INDEX,
                placeholder ? BlobPlaceholder.INSTANCE : new BlobData(new byte[] {(byte) rowId}));
        row.setField(1, rowId);
        row.setField(2, sequenceNumber);
        return row;
    }

    private static RecordReader<InternalRow> oneRowPerBatchReader(List<InternalRow> rows) {
        return new RecordReader<InternalRow>() {

            int index;

            @Override
            public RecordIterator<InternalRow> readBatch() {
                if (index >= rows.size()) {
                    return null;
                }
                InternalRow row = rows.get(index++);
                return new RecordIterator<InternalRow>() {

                    boolean returned;

                    @Override
                    public InternalRow next() {
                        if (returned) {
                            return null;
                        }
                        returned = true;
                        return row;
                    }

                    @Override
                    public void releaseBatch() {}
                };
            }

            @Override
            public void close() {}
        };
    }

    private static class ReadResult {
        final List<Long> rowIds = new ArrayList<>();
        final List<Long> sequenceNumbers = new ArrayList<>();
        final List<Integer> batchSizes = new ArrayList<>();
        int placeholderRowCount;

        static ReadResult read(RecordReader<InternalRow> reader) throws Exception {
            try {
                ReadResult result = new ReadResult();
                RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    int batchSize = 0;
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        result.add(row);
                        batchSize++;
                    }
                    batch.releaseBatch();
                    result.batchSizes.add(batchSize);
                }
                return result;
            } finally {
                reader.close();
            }
        }

        private void add(InternalRow row) {
            if (row.getBlob(BLOB_INDEX) == BlobPlaceholder.INSTANCE) {
                placeholderRowCount++;
            } else {
                rowIds.add(row.getLong(1));
                sequenceNumbers.add(row.getLong(2));
            }
        }
    }
}
