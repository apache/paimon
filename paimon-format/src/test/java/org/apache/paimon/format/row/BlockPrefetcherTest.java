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

package org.apache.paimon.format.row;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlockPrefetcher}. */
public class BlockPrefetcherTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCoalesceAdjacentBlocks() {
        long[] compressedSizes = {100, 100, 100, 100, 100};
        long[] uncompressedSizes = {200, 200, 200, 200, 200};
        long[] rowStarts = {0, 10, 20, 30, 40};
        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        int[] blocksToRead = {0, 1, 2, 3, 4};
        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset).isEqualTo(0);
        assertThat(ranges.get(0).length).isEqualTo(500);
        assertThat(ranges.get(0).blockIndices).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    public void testCoalesceWithLargeGap() {
        long[] compressedSizes = {100, 100, 100};
        long[] uncompressedSizes = {200, 200, 200};
        long[] rowStarts = {0, 10, 20};
        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        int[] blocksToRead = {0, 2};
        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        // gap between block 0 end (100) and block 2 start (200) is 100, within HOLE_SIZE_LIMIT
        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).blockIndices).containsExactly(0, 2);
    }

    @Test
    public void testCoalesceSplitsByHoleSize() {
        // Create blocks with large gaps (> 256KB)
        int numBlocks = 3;
        long[] compressedSizes = new long[numBlocks];
        long[] uncompressedSizes = new long[numBlocks];
        long[] rowStarts = new long[numBlocks];
        Arrays.fill(compressedSizes, 1024);
        Arrays.fill(uncompressedSizes, 2048);

        // Block offsets: 0, 300*1024 (gap=299KB > 256KB), 600*1024
        compressedSizes[0] = 1024;
        compressedSizes[1] = 1024;
        compressedSizes[2] = 1024;

        // Override to create large gaps: block 0 at offset 0 size 1024,
        // block 1 at offset 300*1024 (but offsets are derived from prefix sum)
        // So we need block 0 compressedSize = 300*1024 to put block 1 at that offset
        compressedSizes[0] = 300 * 1024;
        compressedSizes[1] = 300 * 1024;
        compressedSizes[2] = 1024;
        rowStarts[0] = 0;
        rowStarts[1] = 100;
        rowStarts[2] = 200;

        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        // blocksToRead = [0, 2]: gap between block 0 end and block 2 start
        // block 0 end = 300*1024, block 2 start = 600*1024, gap = 300*1024 > 256KB
        int[] blocksToRead = {0, 2};
        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0).blockIndices).containsExactly(0);
        assertThat(ranges.get(1).blockIndices).containsExactly(2);
    }

    @Test
    public void testCoalesceSplitsByRangeSize() {
        // Create many blocks that exceed RANGE_SIZE_LIMIT (2MB) when merged
        int numBlocks = 30;
        long[] compressedSizes = new long[numBlocks];
        long[] uncompressedSizes = new long[numBlocks];
        long[] rowStarts = new long[numBlocks];
        Arrays.fill(compressedSizes, 100 * 1024); // 100KB each
        Arrays.fill(uncompressedSizes, 200 * 1024);
        for (int i = 0; i < numBlocks; i++) {
            rowStarts[i] = i * 100L;
        }

        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        int[] blocksToRead = new int[numBlocks];
        for (int i = 0; i < numBlocks; i++) {
            blocksToRead[i] = i;
        }

        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        // 2MB / 100KB = 20 blocks per range, so 30 blocks should split into 2 ranges
        assertThat(ranges.size()).isGreaterThan(1);
        for (VectoredReadStrategy.MergedRange range : ranges) {
            assertThat(range.length).isLessThanOrEqualTo(2 * 1024 * 1024);
        }
    }

    @Test
    public void testCoalesceEmptyInput() {
        long[] compressedSizes = {100};
        long[] uncompressedSizes = {200};
        long[] rowStarts = {0};
        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(new int[0], index);

        assertThat(ranges).isEmpty();
    }

    @Test
    public void testCoalesceSingleBlock() {
        long[] compressedSizes = {1024};
        long[] uncompressedSizes = {2048};
        long[] rowStarts = {0};
        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        int[] blocksToRead = {0};
        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset).isEqualTo(0);
        assertThat(ranges.get(0).length).isEqualTo(1024);
        assertThat(ranges.get(0).blockIndices).containsExactly(0);
    }

    @Test
    public void testCoalesceNonContiguousBlocks() {
        long[] compressedSizes = {100, 100, 100, 100, 100};
        long[] uncompressedSizes = {200, 200, 200, 200, 200};
        long[] rowStarts = {0, 10, 20, 30, 40};
        RowBlockIndex index = new RowBlockIndex(compressedSizes, uncompressedSizes, rowStarts);

        int[] blocksToRead = {0, 2, 4};
        List<VectoredReadStrategy.MergedRange> ranges =
                VectoredReadStrategy.coalesceRanges(blocksToRead, index);

        // All gaps are small (100 bytes), so everything merges into one range
        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0).offset).isEqualTo(0);
        assertThat(ranges.get(0).length).isEqualTo(500);
        assertThat(ranges.get(0).blockIndices).containsExactly(0, 2, 4);
    }

    @Test
    public void testPrefetcherReadsNonContiguousBlocks() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "prefetch_non_contig.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            rows.add(GenericRow.of(i));
        }
        writeRows(format, rowType, path, rows);

        // Select only specific non-contiguous rows that span multiple blocks
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(50);
        selection.add(200);
        selection.add(499);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).containsExactly(0, 50, 200, 499);
    }

    @Test
    public void testPrefetcherWithManyMergedRanges() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(1000)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "prefetch_multi_range.row");
        Options options = new Options();
        options.setString("file.block-size", "512b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString("value_" + i)));
        }
        writeRows(format, rowType, path, rows);

        // Read all rows through the prefetcher (no selection)
        List<InternalRow> result = readAllRows(format, rowType, path);
        assertThat(result).hasSize(2000);
        for (int i = 0; i < 2000; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
            assertThat(result.get(i).getString(1).toString()).isEqualTo("value_" + i);
        }
    }

    @Test
    public void testPrefetcherWithSelectionAcrossManyRanges() throws IOException {
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(2000)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "prefetch_sel_ranges.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        // Write rows with large strings to ensure many blocks
        List<InternalRow> rows = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < 200; j++) {
            sb.append('x');
        }
        String padding = sb.toString();
        for (int i = 0; i < 1000; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString(padding + i)));
        }
        writeRows(format, rowType, path, rows);

        // Select rows spread widely across the file
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(0);
        selection.add(100);
        selection.add(500);
        selection.add(750);
        selection.add(999);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).containsExactly(0, 100, 500, 750, 999);
    }

    @Test
    public void testPrefetcherSingleBlockFile() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "single_block.row");
        FileFormat format = FileFormat.fromIdentifier("row", new Options());

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(GenericRow.of(i));
        }
        writeRows(format, rowType, path, rows);

        List<InternalRow> result = readAllRows(format, rowType, path);
        assertThat(result).hasSize(10);
        for (int i = 0; i < 10; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
        }
    }

    @Test
    public void testPrefetcherEmptyBlocksToRead() throws IOException {
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "empty_sel.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(GenericRow.of(i));
        }
        writeRows(format, rowType, path, rows);

        // Selection that matches no rows
        RoaringBitmap32 selection = new RoaringBitmap32();
        selection.add(9999);

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).isEmpty();
    }

    @Test
    public void testPrefetchSlidingWindow() throws IOException {
        // Many ranges exceeding PREFETCH_COUNT to verify sliding window works
        RowType rowType =
                RowType.builder()
                        .fields(Arrays.asList(new IntType(), new VarCharType(5000)))
                        .build();

        Path path = new Path(tempDir.toUri().toString(), "sliding_window.row");
        Options options = new Options();
        options.setString("file.block-size", "256b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        // Large rows to ensure many blocks, each block ~ 1 row
        List<InternalRow> rows = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < 500; j++) {
            sb.append('a');
        }
        String padding = sb.toString();
        for (int i = 0; i < 200; i++) {
            rows.add(GenericRow.of(i, BinaryString.fromString(padding + i)));
        }
        writeRows(format, rowType, path, rows);

        // Read all rows - will create many merged ranges, well beyond PREFETCH_COUNT
        List<InternalRow> result = readAllRows(format, rowType, path);
        assertThat(result).hasSize(200);
        for (int i = 0; i < 200; i++) {
            assertThat(result.get(i).getInt(0)).isEqualTo(i);
        }
    }

    @Test
    public void testPrefetcherWithSparseSelection() throws IOException {
        // Select 1 row per block across many blocks to stress non-contiguous iteration
        RowType rowType = RowType.builder().fields(Arrays.asList(new IntType())).build();

        Path path = new Path(tempDir.toUri().toString(), "sparse_sel.row");
        Options options = new Options();
        options.setString("file.block-size", "64b");
        FileFormat format = FileFormat.fromIdentifier("row", options);

        List<InternalRow> rows = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            rows.add(GenericRow.of(i));
        }
        writeRows(format, rowType, path, rows);

        // Select every 100th row
        RoaringBitmap32 selection = new RoaringBitmap32();
        List<Integer> expectedValues = new ArrayList<>();
        for (int i = 0; i < 1000; i += 100) {
            selection.add(i);
            expectedValues.add(i);
        }

        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path), selection));

        List<Integer> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(row.getInt(0)));
        reader.close();

        assertThat(result).containsExactlyElementsOf(expectedValues);
    }

    // ======================== Helpers ========================

    private void writeRows(FileFormat format, RowType rowType, Path path, List<InternalRow> rows)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        PositionOutputStream out = fileIO.newOutputStream(path, false);
        FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
    }

    private List<InternalRow> readAllRows(FileFormat format, RowType rowType, Path path)
            throws IOException {
        LocalFileIO fileIO = new LocalFileIO();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, new ArrayList<>());
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(
                row -> {
                    GenericRow copy = new GenericRow(rowType.getFieldCount());
                    for (int i = 0; i < rowType.getFieldCount(); i++) {
                        if (row.isNullAt(i)) {
                            copy.setField(i, null);
                        } else {
                            switch (rowType.getTypeAt(i).getTypeRoot()) {
                                case INTEGER:
                                    copy.setField(i, row.getInt(i));
                                    break;
                                case VARCHAR:
                                    copy.setField(i, row.getString(i));
                                    break;
                                default:
                                    throw new UnsupportedOperationException();
                            }
                        }
                    }
                    result.add(copy);
                });
        reader.close();
        return result;
    }
}
