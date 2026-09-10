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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.VectorizedRowIterator;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataFileRecordReader}. */
public class DataFileRecordReaderTest {

    @Test
    public void testRowTrackingIsolatedFromReusedIdentityBatch() throws Exception {
        ReusingColumnarReader delegate = new ReusingColumnarReader();
        assertThat(delegate.batch.columns).isSameAs(delegate.readerOwnedColumns);
        assertThat(delegate.readerOwnedColumns.getClass()).isEqualTo(HeapLongVector[].class);

        Map<String, Integer> systemFields = new HashMap<>();
        systemFields.put(SpecialFields.ROW_ID.name(), 1);
        systemFields.put(SpecialFields.SEQUENCE_NUMBER.name(), 2);
        DataFileRecordReader reader =
                createRowTrackingReader(
                        SpecialFields.rowTypeWithRowTracking(RowType.of(DataTypes.BIGINT())),
                        delegate,
                        new int[] {0, 1, 2},
                        systemFields);

        for (int batchIndex = 0; batchIndex < 3; batchIndex++) {
            FileRecordIterator<InternalRow> iterator = reader.readBatch();

            assertThat(iterator).isNotSameAs(delegate.iterator);
            assertThat(iterator).isInstanceOf(VectorizedRowIterator.class);
            assertReaderOwnedColumns(delegate);

            InternalRow row = iterator.next();
            assertThat(row.getFieldCount()).isEqualTo(3);
            assertThat(row.getLong(0)).isEqualTo(42L);
            assertThat(row.getLong(1)).isEqualTo(100L + batchIndex);
            assertThat(row.getLong(2)).isEqualTo(7L);
            assertThat(delegate.recycleCount).isEqualTo(batchIndex);

            iterator.releaseBatch();
            assertThat(delegate.recycleCount).isEqualTo(batchIndex + 1);
            assertReaderOwnedColumns(delegate);
        }
        reader.close();
    }

    @Test
    public void testSingletonRowTrackingFieldAssigned() throws Exception {
        ReusingColumnarReader delegate = new ReusingColumnarReader();
        DataFileRecordReader reader =
                createRowTrackingReader(
                        SpecialFields.rowTypeWithRowId(RowType.of(DataTypes.BIGINT())),
                        delegate,
                        new int[] {0, 1},
                        Collections.singletonMap(SpecialFields.ROW_ID.name(), 1));

        FileRecordIterator<InternalRow> iterator = reader.readBatch();

        assertThat(iterator).isNotSameAs(delegate.iterator);
        assertThat(iterator).isInstanceOf(VectorizedRowIterator.class);
        assertReaderOwnedColumns(delegate);

        InternalRow row = iterator.next();
        assertThat(row.getFieldCount()).isEqualTo(2);
        assertThat(row.getLong(0)).isEqualTo(42L);
        assertThat(row.getLong(1)).isEqualTo(100L);
        assertThat(delegate.recycleCount).isZero();

        iterator.releaseBatch();
        assertThat(delegate.recycleCount).isEqualTo(1);
        assertReaderOwnedColumns(delegate);
        reader.close();
    }

    @Test
    public void testEmptyRowTrackingFieldsPreserveSpecializedIdentityIterator() throws Exception {
        HeapLongVector dataVector = new HeapLongVector(1);
        dataVector.setLong(0, 42L);
        VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[] {dataVector});
        batch.setNumRows(1);
        ColumnarRowIterator specializedIterator =
                new ColumnarRowIterator(new Path("test"), new ColumnarRow(batch), null) {};
        specializedIterator.reset(0);

        FileRecordReader<InternalRow> delegate =
                new FileRecordReader<InternalRow>() {
                    @Override
                    public FileRecordIterator<InternalRow> readBatch() {
                        return specializedIterator;
                    }

                    @Override
                    public void close() {}
                };
        DataFileRecordReader reader =
                new DataFileRecordReader(
                        RowType.of(DataTypes.BIGINT()),
                        delegate,
                        false,
                        false,
                        new int[] {0},
                        null,
                        null,
                        true,
                        100L,
                        7L,
                        Collections.emptyMap(),
                        null,
                        new Path("test"));

        assertThat(reader.readBatch()).isSameAs(specializedIterator);
        reader.close();
    }

    private static DataFileRecordReader createRowTrackingReader(
            RowType rowType,
            ReusingColumnarReader delegate,
            int[] indexMapping,
            Map<String, Integer> systemFields) {
        return new DataFileRecordReader(
                rowType,
                delegate,
                false,
                false,
                indexMapping,
                null,
                null,
                true,
                100L,
                7L,
                systemFields,
                null,
                new Path("test"));
    }

    private static void assertReaderOwnedColumns(ReusingColumnarReader delegate) {
        assertThat(delegate.batch.columns).isSameAs(delegate.readerOwnedColumns);
        assertThat(delegate.readerOwnedColumns)
                .containsExactly(
                        delegate.dataVector, delegate.rowIdVector, delegate.sequenceNumberVector);
    }

    private static class ReusingColumnarReader implements FileRecordReader<InternalRow> {

        private final HeapLongVector dataVector = new HeapLongVector(1);
        private final HeapLongVector rowIdVector = new HeapLongVector(1);
        private final HeapLongVector sequenceNumberVector = new HeapLongVector(1);
        private final ColumnVector[] readerOwnedColumns =
                new HeapLongVector[] {dataVector, rowIdVector, sequenceNumberVector};
        private final VectorizedColumnBatch batch = new VectorizedColumnBatch(readerOwnedColumns);
        private final VectorizedRowIterator iterator;
        private int nextPosition;
        private int recycleCount;

        private ReusingColumnarReader() {
            dataVector.setLong(0, 42L);
            rowIdVector.fillWithNulls();
            sequenceNumberVector.fillWithNulls();
            batch.setNumRows(1);
            iterator =
                    new VectorizedRowIterator(
                            new Path("test"), new ColumnarRow(batch), () -> recycleCount++);
        }

        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            iterator.reset(nextPosition++);
            return iterator;
        }

        @Override
        public void close() {}
    }
}
