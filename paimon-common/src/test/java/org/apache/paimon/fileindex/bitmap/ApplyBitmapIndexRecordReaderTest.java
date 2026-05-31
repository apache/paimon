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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ApplyBitmapIndexRecordReader}. */
public class ApplyBitmapIndexRecordReaderTest {

    private static final Path DUMMY_PATH = new Path("/dummy");

    @Test
    public void testLimitStopsAfterFirstBatchWithPreSelectedUnderlying() throws Exception {
        // Simulates DataFileRecordReader#readBatchInternal: iterator.selection(selection)
        int limit = 10;
        int batchSize = 20;
        int totalRows = 100;
        RoaringBitmap32 limitBitmap = RoaringBitmap32.bitmapOfRange(0, totalRows).limit(limit);
        BitmapIndexResult selection = new BitmapIndexResult(() -> limitBitmap);
        PreSelectedCountingFileRecordReader underlying =
                new PreSelectedCountingFileRecordReader(totalRows, batchSize, limitBitmap);
        ApplyBitmapIndexRecordReader reader =
                new ApplyBitmapIndexRecordReader(underlying, selection);

        List<Integer> rows = readViaCloseableIterator(reader);

        assertThat(rows).containsExactlyElementsOf(range(limit));
        assertThat(underlying.readBatchCount()).isEqualTo(1);
    }

    @Test
    public void testSparseBitmapWithPreSelectedUnderlying() throws Exception {
        int batchSize = 5;
        int totalRows = 20;
        RoaringBitmap32 sparseBitmap = RoaringBitmap32.bitmapOf(0, 2, 4, 6, 8);
        BitmapIndexResult selection = new BitmapIndexResult(() -> sparseBitmap);
        PreSelectedCountingFileRecordReader underlying =
                new PreSelectedCountingFileRecordReader(totalRows, batchSize, sparseBitmap);
        ApplyBitmapIndexRecordReader reader =
                new ApplyBitmapIndexRecordReader(underlying, selection);

        List<Integer> rows = readViaCloseableIterator(reader);

        assertThat(rows).containsExactly(0, 2, 4, 6, 8);
        assertThat(underlying.readBatchCount()).isEqualTo(2);
    }

    @Test
    public void testLimitStopsAfterFirstBatch() throws Exception {
        int limit = 10;
        int batchSize = 20;
        int totalRows = 100;
        CountingFileRecordReader underlying = new CountingFileRecordReader(totalRows, batchSize);
        BitmapIndexResult selection =
                new BitmapIndexResult(
                        () -> RoaringBitmap32.bitmapOfRange(0, totalRows).limit(limit));
        ApplyBitmapIndexRecordReader reader =
                new ApplyBitmapIndexRecordReader(underlying, selection);

        List<Integer> rows = readViaCloseableIterator(reader);

        assertThat(rows).containsExactlyElementsOf(range(limit));
        assertThat(underlying.readBatchCount()).isEqualTo(1);
    }

    @Test
    public void testForEachRemainingStopsAfterLimit() throws Exception {
        int limit = 10;
        int batchSize = 20;
        int totalRows = 100;
        CountingFileRecordReader underlying = new CountingFileRecordReader(totalRows, batchSize);
        BitmapIndexResult selection =
                new BitmapIndexResult(
                        () -> RoaringBitmap32.bitmapOfRange(0, totalRows).limit(limit));
        ApplyBitmapIndexRecordReader reader =
                new ApplyBitmapIndexRecordReader(underlying, selection);

        List<Integer> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(row.getInt(0)));

        assertThat(rows).containsExactlyElementsOf(range(limit));
        assertThat(underlying.readBatchCount()).isEqualTo(1);
    }

    @Test
    public void testSparseBitmapStillStopsAtLastPosition() throws Exception {
        int batchSize = 5;
        int totalRows = 20;
        CountingFileRecordReader underlying = new CountingFileRecordReader(totalRows, batchSize);
        BitmapIndexResult selection =
                new BitmapIndexResult(() -> RoaringBitmap32.bitmapOf(0, 2, 4, 6, 8));
        ApplyBitmapIndexRecordReader reader =
                new ApplyBitmapIndexRecordReader(underlying, selection);

        List<Integer> rows = readViaCloseableIterator(reader);

        assertThat(rows).containsExactly(0, 2, 4, 6, 8);
        assertThat(underlying.readBatchCount()).isEqualTo(2);
    }

    private static List<Integer> readViaCloseableIterator(ApplyBitmapIndexRecordReader reader)
            throws Exception {
        List<Integer> rows = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
            while (iterator.hasNext()) {
                rows.add(iterator.next().getInt(0));
            }
        }
        return rows;
    }

    private static List<Integer> range(int endExclusive) {
        List<Integer> result = new ArrayList<>(endExclusive);
        for (int i = 0; i < endExclusive; i++) {
            result.add(i);
        }
        return result;
    }

    /**
     * Simulates {@code DataFileRecordReader#readBatchInternal}, which applies {@code
     * iterator.selection(selection)} before wrapping with {@link ApplyBitmapIndexRecordReader}.
     */
    private static class PreSelectedCountingFileRecordReader
            implements FileRecordReader<InternalRow> {

        private final int totalRows;
        private final int batchSize;
        private final RoaringBitmap32 selectionBitmap;
        private final AtomicInteger readBatchCount = new AtomicInteger(0);
        private int nextBatchStart;

        private PreSelectedCountingFileRecordReader(
                int totalRows, int batchSize, RoaringBitmap32 selectionBitmap) {
            this.totalRows = totalRows;
            this.batchSize = batchSize;
            this.selectionBitmap = selectionBitmap;
        }

        int readBatchCount() {
            return readBatchCount.get();
        }

        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            readBatchCount.incrementAndGet();
            if (nextBatchStart >= totalRows) {
                return null;
            }
            int batchStart = nextBatchStart;
            int batchEnd = Math.min(nextBatchStart + batchSize, totalRows);
            nextBatchStart = batchEnd;
            return new PositionFileRecordIterator(batchStart, batchEnd).selection(selectionBitmap);
        }

        @Override
        public void close() {}
    }

    private static class CountingFileRecordReader implements FileRecordReader<InternalRow> {

        private final int totalRows;
        private final int batchSize;
        private final AtomicInteger readBatchCount = new AtomicInteger(0);
        private int nextBatchStart;

        private CountingFileRecordReader(int totalRows, int batchSize) {
            this.totalRows = totalRows;
            this.batchSize = batchSize;
        }

        int readBatchCount() {
            return readBatchCount.get();
        }

        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            readBatchCount.incrementAndGet();
            if (nextBatchStart >= totalRows) {
                return null;
            }
            int batchStart = nextBatchStart;
            int batchEnd = Math.min(nextBatchStart + batchSize, totalRows);
            nextBatchStart = batchEnd;
            return new PositionFileRecordIterator(batchStart, batchEnd);
        }

        @Override
        public void close() {}
    }

    private static class PositionFileRecordIterator implements FileRecordIterator<InternalRow> {

        private final int end;
        private int nextPosition;
        private int returnedPosition = -1;

        private PositionFileRecordIterator(int start, int end) {
            this.nextPosition = start;
            this.end = end;
        }

        @Override
        public InternalRow next() {
            if (nextPosition >= end) {
                return null;
            }
            returnedPosition = nextPosition;
            return GenericRow.of(nextPosition++);
        }

        @Override
        public long returnedPosition() {
            return returnedPosition;
        }

        @Override
        public Path filePath() {
            return DUMMY_PATH;
        }

        @Override
        public void releaseBatch() {}
    }
}
