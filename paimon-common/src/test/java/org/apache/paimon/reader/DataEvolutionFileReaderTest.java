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

package org.apache.paimon.reader;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataEvolutionFileReader}. */
public class DataEvolutionFileReaderTest {

    @Test
    public void testNextWithData() throws Exception {
        int[] rowOffsets = new int[] {0, 1};
        int[] fieldOffsets = new int[] {0, 0};

        SimpleRecordReader reader1 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(1)});
        SimpleRecordReader reader2 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(2)});

        @SuppressWarnings("unchecked")
        RecordReader<InternalRow>[] readers = new RecordReader[] {reader1, reader2};

        DataEvolutionFileReader evolutionFileReader =
                new DataEvolutionFileReader(rowOffsets, fieldOffsets, readers, 2);

        RecordReader.RecordIterator<InternalRow> batch = evolutionFileReader.readBatch();
        InternalRow result = batch.next();
        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getInt(1)).isEqualTo(2);

        InternalRow nullResult = batch.next();
        assertThat(nullResult).isNull();

        evolutionFileReader.close();
    }

    @Test
    public void testNextWhenFirstReaderIsEmpty() throws Exception {
        int[] rowOffsets = new int[] {0, 1};
        int[] fieldOffsets = new int[] {0, 0};

        SimpleRecordReader reader1 = new SimpleRecordReader(new InternalRow[0]);
        SimpleRecordReader reader2 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(2)});

        @SuppressWarnings("unchecked")
        RecordReader<InternalRow>[] readers = new RecordReader[] {reader1, reader2};

        DataEvolutionFileReader evolutionFileReader =
                new DataEvolutionFileReader(rowOffsets, fieldOffsets, readers, 2);

        RecordReader.RecordIterator<InternalRow> batch = evolutionFileReader.readBatch();
        InternalRow result = batch.next();

        assertThat(result).isNull();

        evolutionFileReader.close();
    }

    @Test
    public void testNextWithNullReaderInArray() throws Exception {
        int[] rowOffsets = new int[] {0, 2};
        int[] fieldOffsets = new int[] {0, 0};

        SimpleRecordReader reader1 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(1)});
        SimpleRecordReader reader2 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(2)});

        @SuppressWarnings("unchecked")
        RecordReader<InternalRow>[] readers = new RecordReader[] {reader1, null, reader2};

        DataEvolutionFileReader evolutionFileReader =
                new DataEvolutionFileReader(rowOffsets, fieldOffsets, readers, 2);

        RecordReader.RecordIterator<InternalRow> batch = evolutionFileReader.readBatch();
        InternalRow result = batch.next();

        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(result.getInt(1)).isEqualTo(2);

        InternalRow nullResult = batch.next();
        assertThat(nullResult).isNull();

        evolutionFileReader.close();
    }

    @Test
    public void testReleaseInnerIteratorsAfterEndOfInput() throws Exception {
        int[] rowOffsets = new int[] {0, 1, 2};
        int[] fieldOffsets = new int[] {0, 0, 0};

        SimpleRecordReader reader1 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(1)});
        SimpleRecordReader reader2 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(2)});
        SimpleRecordReader reader3 = new SimpleRecordReader(new InternalRow[] {GenericRow.of(3)});

        @SuppressWarnings("unchecked")
        RecordReader<InternalRow>[] readers = new RecordReader[] {reader1, null, reader2, reader3};

        DataEvolutionFileReader evolutionFileReader =
                new DataEvolutionFileReader(rowOffsets, fieldOffsets, readers, 2);

        RecordReader.RecordIterator<InternalRow> batch = evolutionFileReader.readBatch();

        // consume the single aligned row
        assertThat(batch.next()).isNotNull();
        // second call should trigger end-of-input and release underlying iterators
        assertThat(batch.next()).isNull();

        assertThat(reader1.iterator.released).isTrue();
        assertThat(reader2.iterator.released).isTrue();
        assertThat(reader3.iterator.released).isTrue();

        evolutionFileReader.close();
    }

    @Test
    public void testAlignedBatchesDoNotDropRows() throws Exception {
        int totalRows = 10;

        TestRecordReader reader1 = new TestRecordReader(new int[] {8, 2}, totalRows);
        TestRecordReader reader2 = new TestRecordReader(new int[] {5, 5}, totalRows);

        int[] rowOffsets = new int[] {0, 1};
        int[] fieldOffsets = new int[] {0, 0};
        @SuppressWarnings("unchecked")
        RecordReader<InternalRow>[] readers = new RecordReader[] {reader1, reader2};

        // Force outer batch size to 5 so that we have two batches of 5 rows each.
        DataEvolutionFileReader evolutionFileReader =
                new DataEvolutionFileReader(rowOffsets, fieldOffsets, readers, 5);

        int totalRead = 0;
        List<Integer> batchSizes = new ArrayList<>();

        RecordReader.RecordIterator<InternalRow> batch;
        while ((batch = evolutionFileReader.readBatch()) != null) {
            int currentBatch = 0;
            InternalRow row;
            while ((row = batch.next()) != null) {
                currentBatch++;
            }
            batch.releaseBatch();

            if (currentBatch == 0) {
                break;
            }

            batchSizes.add(currentBatch);
            totalRead += currentBatch;
        }

        evolutionFileReader.close();

        assertThat(batchSizes).containsExactly(5, 5);
        assertThat(totalRead).isEqualTo(totalRows);
    }

    private static class SimpleRecordReader implements RecordReader<InternalRow> {

        private final TestIterator iterator;
        private boolean batchReturned;

        SimpleRecordReader(InternalRow[] rows) {
            this.iterator = new TestIterator(rows);
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (batchReturned) {
                return null;
            }
            batchReturned = true;
            return iterator;
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    private static class TestRecordReader implements RecordReader<InternalRow> {

        private final List<InternalRow> rows;
        private final int[] batchSizes;
        private int nextBatchIndex = 0;
        private int readOffset = 0;

        TestRecordReader(int[] batchSizes, int totalRows) {
            this.batchSizes = batchSizes;
            this.rows = new ArrayList<>(totalRows);
            for (int i = 0; i < totalRows; i++) {
                this.rows.add(GenericRow.of(i));
            }
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (readOffset >= rows.size() || nextBatchIndex >= batchSizes.length) {
                return null;
            }

            int size = Math.min(batchSizes[nextBatchIndex++], rows.size() - readOffset);
            List<InternalRow> batch = rows.subList(readOffset, readOffset + size);
            readOffset += size;
            return new ListRecordIterator(batch);
        }

        @Override
        public void close() {
            // nothing to close
        }
    }

    private static class ListRecordIterator implements RecordReader.RecordIterator<InternalRow> {

        private final List<InternalRow> rows;
        private int index = 0;

        ListRecordIterator(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (index < rows.size()) {
                return rows.get(index++);
            }
            return null;
        }

        @Override
        public void releaseBatch() {
            // nothing to release
        }
    }

    private static class TestIterator implements RecordReader.RecordIterator<InternalRow> {

        private final InternalRow[] rows;
        private int index;
        private boolean released;

        TestIterator(InternalRow[] rows) {
            this.rows = rows;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (index < rows.length) {
                return rows[index++];
            }
            return null;
        }

        @Override
        public void releaseBatch() {
            released = true;
        }
    }
}
