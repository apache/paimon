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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * This is a union reader which contains multiple inner reader.
 *
 * <pre> This reader, assembling multiple reader into one big and great reader. The row it produces
 * also come from the readers it contains.
 *
 * For example, the expected schema for this reader is : int, int, string, int, string, int.(Total 6 fields)
 * It contains three inner readers, we call them reader0, reader1 and reader2.
 *
 * The rowOffsets and fieldOffsets are all 6 elements long the same as
 * output schema. RowOffsets is used to indicate which inner reader the field comes from, and
 * fieldOffsets is used to indicate the offset of the field in the inner reader.
 *
 * For example, if rowOffsets is {0, 2, 0, 1, 2, 1} and fieldOffsets is {0, 0, 1, 1, 1, 0}, it means:
 * - The first field comes from reader0, and it is at offset 0 in reader1.
 * - The second field comes from reader2, and it is at offset 0 in reader3.
 * - The third field comes from reader0, and it is at offset 1 in reader1.
 * - The fourth field comes from reader1, and it is at offset 1 in reader2.
 * - The fifth field comes from reader2, and it is at offset 1 in reader3.
 * - The sixth field comes from reader1, and it is at offset 0 in reader2.
 *
 * These three readers work together, package out final and complete rows.
 *
 * </pre>
 */
public class DataEvolutionFileReader implements RecordReader<InternalRow> {

    private final int[] rowOffsets;
    private final int[] fieldOffsets;
    private final RecordReader<InternalRow>[] innerReaders;

    public DataEvolutionFileReader(
            int[] rowOffsets, int[] fieldOffsets, RecordReader<InternalRow>[] readers) {
        checkArgument(rowOffsets != null, "Row offsets must not be null");
        checkArgument(fieldOffsets != null, "Field offsets must not be null");
        checkArgument(
                rowOffsets.length == fieldOffsets.length,
                "Row offsets and field offsets must have the same length");
        checkArgument(rowOffsets.length > 0, "Row offsets must not be empty");
        checkArgument(readers != null && readers.length > 1, "Readers should be more than 1");
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
        this.innerReaders = readers;
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        DataEvolutionIterator iterator =
                new DataEvolutionIterator(innerReaders.length, rowOffsets, fieldOffsets);
        for (int i = 0; i < innerReaders.length; i++) {
            RecordIterator<InternalRow> batch = innerReaders[i].readBatch();
            if (batch == null && !(innerReaders[i] instanceof EmptyFileRecordReader)) {
                return null;
            }
            iterator.set(i, batch);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(innerReaders);
        } catch (Exception e) {
            throw new IOException("Failed to close inner readers", e);
        }
    }

    /** The batch which is made up by several batches. */
    private static class DataEvolutionIterator implements RecordIterator<InternalRow> {

        private final DataEvolutionRow dataEvolutionRow;
        private final RecordIterator<InternalRow>[] iterators;

        private DataEvolutionIterator(
                int rowNumber,
                int[] rowOffsets,
                int[] fieldOffsets) { // Initialize with empty arrays, will be set later
            this.dataEvolutionRow = new DataEvolutionRow(rowNumber, rowOffsets, fieldOffsets);
            //noinspection unchecked
            this.iterators = new RecordIterator[rowNumber];
        }

        public void set(int i, RecordIterator<InternalRow> iterator) {
            iterators[i] = iterator;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            for (int i = 0; i < iterators.length; i++) {
                if (iterators[i] != null) {
                    InternalRow next = iterators[i].next();
                    if (next == null) {
                        return null;
                    }
                    dataEvolutionRow.setRow(i, next);
                }
            }
            return dataEvolutionRow;
        }

        @Override
        public void releaseBatch() {
            for (RecordIterator<InternalRow> iterator : iterators) {
                if (iterator != null) {
                    iterator.releaseBatch();
                }
            }
        }
    }

    /** The row which is made up by several rows. */
    private static class DataEvolutionRow implements InternalRow {

        private final InternalRow[] rows;
        private final int[] rowOffsets;
        private final int[] fieldOffsets;

        private DataEvolutionRow(int rowNumber, int[] rowOffsets, int[] fieldOffsets) {
            this.rows = new InternalRow[rowNumber];
            this.rowOffsets = rowOffsets;
            this.fieldOffsets = fieldOffsets;
        }

        private void setRow(int pos, InternalRow row) {
            if (pos >= rows.length) {
                throw new IndexOutOfBoundsException(
                        "Position " + pos + " is out of bounds for rows size " + rows.length);
            } else {
                rows[pos] = row;
            }
        }

        private InternalRow chooseRow(int pos) {
            return rows[(rowOffsets[pos])];
        }

        private int offsetInRow(int pos) {
            return fieldOffsets[pos];
        }

        @Override
        public int getFieldCount() {
            return fieldOffsets.length;
        }

        @Override
        public RowKind getRowKind() {
            return rows[0].getRowKind();
        }

        @Override
        public void setRowKind(RowKind kind) {
            rows[0].setRowKind(kind);
        }

        @Override
        public boolean isNullAt(int pos) {
            if (rowOffsets[pos] == -1) {
                return true;
            }
            return chooseRow(pos).isNullAt(offsetInRow(pos));
        }

        @Override
        public boolean getBoolean(int pos) {
            return chooseRow(pos).getBoolean(offsetInRow(pos));
        }

        @Override
        public byte getByte(int pos) {
            return chooseRow(pos).getByte(offsetInRow(pos));
        }

        @Override
        public short getShort(int pos) {
            return chooseRow(pos).getShort(offsetInRow(pos));
        }

        @Override
        public int getInt(int pos) {
            return chooseRow(pos).getInt(offsetInRow(pos));
        }

        @Override
        public long getLong(int pos) {
            return chooseRow(pos).getLong(offsetInRow(pos));
        }

        @Override
        public float getFloat(int pos) {
            return chooseRow(pos).getFloat(offsetInRow(pos));
        }

        @Override
        public double getDouble(int pos) {
            return chooseRow(pos).getDouble(offsetInRow(pos));
        }

        @Override
        public BinaryString getString(int pos) {
            return chooseRow(pos).getString(offsetInRow(pos));
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return chooseRow(pos).getDecimal(offsetInRow(pos), precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return chooseRow(pos).getTimestamp(offsetInRow(pos), precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return chooseRow(pos).getBinary(offsetInRow(pos));
        }

        @Override
        public Variant getVariant(int pos) {
            return chooseRow(pos).getVariant(offsetInRow(pos));
        }

        @Override
        public InternalArray getArray(int pos) {
            return chooseRow(pos).getArray(offsetInRow(pos));
        }

        @Override
        public InternalMap getMap(int pos) {
            return chooseRow(pos).getMap(offsetInRow(pos));
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return chooseRow(pos).getRow(offsetInRow(pos), numFields);
        }
    }
}
