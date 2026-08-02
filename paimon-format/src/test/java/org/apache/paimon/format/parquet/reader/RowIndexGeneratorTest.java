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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowIndexGenerator}. */
public class RowIndexGeneratorTest {

    @Test
    public void testLazyRowIdAcrossBatches() {
        AtomicInteger consumed = new AtomicInteger();
        PrimitiveIterator.OfLong indexes =
                new PrimitiveIterator.OfLong() {
                    private long next;

                    @Override
                    public long nextLong() {
                        consumed.incrementAndGet();
                        return next++;
                    }

                    @Override
                    public boolean hasNext() {
                        return next < 6;
                    }
                };
        HeapLongVector rowIds = new HeapLongVector(3);
        rowIds.fillWithNulls();
        ColumnarBatch batch =
                new ColumnarBatch(
                        new Path("test"), new ColumnVector[] {new HeapIntVector(3), rowIds}, null);
        ColumnarRowIterator iterator = batch.vectorizedRowIterator;
        iterator.assignRowTracking(
                500_000L, 1L, Collections.singletonMap(SpecialFields.ROW_ID.name(), 1));

        PageReadStore page =
                new PageReadStore() {
                    @Override
                    public PageReader getPageReader(ColumnDescriptor descriptor) {
                        return null;
                    }

                    @Override
                    public long getRowCount() {
                        return 6;
                    }

                    @Override
                    public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
                        return Optional.of(indexes);
                    }
                };
        RowIndexGenerator generator = new RowIndexGenerator();
        generator.initFromPageReadStore(page);

        batch.setNumRows(3);
        generator.populateRowIndex(batch);
        InternalRow row;
        while ((row = iterator.next()) != null) {
            row.getInt(0);
        }

        batch.setNumRows(3);
        generator.populateRowIndex(batch);
        assertThat(consumed).hasValue(0);
        row = iterator.next();
        assertThat(row.getLong(1)).isEqualTo(500_003L);
        assertThat(consumed).hasValue(4);
    }
}
