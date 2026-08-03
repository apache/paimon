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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.SpecialFields;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowIndexGenerator}. */
public class RowIndexGeneratorTest {

    @Test
    public void testLazyRowIdAcrossBatches() {
        CountingRowIndexes indexes = new CountingRowIndexes(6);
        HeapLongVector rowIds = new HeapLongVector(3);
        rowIds.fillWithNulls();
        ColumnarBatch batch =
                new ColumnarBatch(
                        new Path("test"), new ColumnVector[] {new HeapIntVector(3), rowIds}, null);
        ColumnarRowIterator iterator = batch.vectorizedRowIterator;
        iterator.assignRowTracking(
                500_000L, 1L, Collections.singletonMap(SpecialFields.ROW_ID.name(), 1));

        RowIndexGenerator generator = newGenerator(indexes);

        batch.setNumRows(3);
        generator.populateRowIndex(batch);
        InternalRow row;
        while ((row = iterator.next()) != null) {
            row.getInt(0);
        }

        batch.setNumRows(3);
        generator.populateRowIndex(batch);
        assertThat(indexes.nextIndex).isZero();
        row = iterator.next();
        assertThat(row.getLong(1)).isEqualTo(500_003L);
        assertThat(indexes.nextIndex).isEqualTo(4);
    }

    @Test
    public void testPartiallyConsumedBatch() {
        CountingRowIndexes indexes = new CountingRowIndexes(6);
        RowIndexGenerator generator = newGenerator(indexes);
        ColumnarBatch batch = newBatch();

        generator.populateRowIndex(batch);
        assertThat(generator.next()).isZero();

        generator.populateRowIndex(batch);
        assertThat(generator.next()).isEqualTo(3);
    }

    @Test
    public void testMultipleUnconsumedBatchesStayLazy() {
        CountingRowIndexes indexes = new CountingRowIndexes(9);
        RowIndexGenerator generator = newGenerator(indexes);
        ColumnarBatch batch = newBatch();

        generator.populateRowIndex(batch);
        generator.populateRowIndex(batch);
        generator.populateRowIndex(batch);
        assertThat(indexes.nextIndex).isZero();
        assertThat(generator.next()).isEqualTo(6);
        assertThat(indexes.nextIndex).isEqualTo(7);
    }

    @Test
    public void testRowGroupResetDropsPendingPositions() {
        CountingRowIndexes firstGroup = new CountingRowIndexes(3);
        RowIndexGenerator generator = newGenerator(firstGroup);
        ColumnarBatch batch = newBatch();
        generator.populateRowIndex(batch);

        CountingRowIndexes secondGroup = new CountingRowIndexes(3);
        initGenerator(generator, secondGroup, 100);
        generator.populateRowIndex(batch);

        assertThat(firstGroup.nextIndex).isZero();
        assertThat(generator.next()).isEqualTo(100);
    }

    @Test
    public void testNonContinuousRowIndexes() {
        PrimitiveIterator.OfLong indexes = LongStream.of(1, 4, 9, 12, 20, 30).iterator();
        RowIndexGenerator generator = newGenerator(indexes, 6);
        ColumnarBatch batch = newBatch();

        generator.populateRowIndex(batch);
        generator.populateRowIndex(batch);

        assertThat(generator.next()).isEqualTo(12);
    }

    private static ColumnarBatch newBatch() {
        ColumnarBatch batch =
                new ColumnarBatch(
                        new Path("test"), new ColumnVector[] {new HeapIntVector(3)}, null);
        batch.setNumRows(3);
        return batch;
    }

    private static RowIndexGenerator newGenerator(CountingRowIndexes indexes) {
        return newGenerator(indexes, indexes.end);
    }

    private static RowIndexGenerator newGenerator(PrimitiveIterator.OfLong indexes, long rowCount) {
        RowIndexGenerator generator = new RowIndexGenerator();
        initGenerator(generator, indexes, rowCount, 0);
        return generator;
    }

    private static void initGenerator(
            RowIndexGenerator generator, CountingRowIndexes indexes, long rowIndexOffset) {
        initGenerator(generator, indexes, indexes.end, rowIndexOffset);
    }

    private static void initGenerator(
            RowIndexGenerator generator,
            PrimitiveIterator.OfLong indexes,
            long rowCount,
            long rowIndexOffset) {
        PageReadStore page =
                new PageReadStore() {
                    @Override
                    public PageReader getPageReader(ColumnDescriptor descriptor) {
                        return null;
                    }

                    @Override
                    public long getRowCount() {
                        return rowCount;
                    }

                    @Override
                    public Optional<Long> getRowIndexOffset() {
                        return Optional.of(rowIndexOffset);
                    }

                    @Override
                    public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
                        return Optional.of(indexes);
                    }
                };
        generator.initFromPageReadStore(page);
    }

    private static class CountingRowIndexes implements PrimitiveIterator.OfLong {

        private final long end;
        private long nextIndex;

        private CountingRowIndexes(long end) {
            this.end = end;
        }

        @Override
        public long nextLong() {
            return nextIndex++;
        }

        @Override
        public boolean hasNext() {
            return nextIndex < end;
        }
    }
}
