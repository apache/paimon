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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * This is a union reader which contains multiple inner readers.
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
    private final RecordReader<InternalRow>[] readers;

    /**
     * Cached iterators for each inner reader. An entry is non-null if the corresponding reader
     * currently has an opened batch which is not fully consumed.
     */
    private final RecordIterator<InternalRow>[] pending;

    /** Marks whether the corresponding reader has reached end of input. */
    private final boolean[] finished;

    /** Maximum number of rows produced for each outer {@link #readBatch()} call. */
    private final int batchSize;

    /** Number of rows emitted by current outer iterator. */
    private int rowsEmittedInCurrentBatch;

    /** Whether any inner reader has reached end of input. */
    private boolean endOfInput;

    @SuppressWarnings("unchecked")
    public DataEvolutionFileReader(
            int[] rowOffsets, int[] fieldOffsets, RecordReader<InternalRow>[] readers) {
        this(rowOffsets, fieldOffsets, readers, CoreOptions.READ_BATCH_SIZE.defaultValue());
    }

    @SuppressWarnings("unchecked")
    public DataEvolutionFileReader(
            int[] rowOffsets,
            int[] fieldOffsets,
            RecordReader<InternalRow>[] readers,
            int batchSize) {
        checkArgument(rowOffsets != null, "Row offsets must not be null");
        checkArgument(fieldOffsets != null, "Field offsets must not be null");
        checkArgument(
                rowOffsets.length == fieldOffsets.length,
                "Row offsets and field offsets must have the same length");
        checkArgument(rowOffsets.length > 0, "Row offsets must not be empty");
        checkArgument(readers != null && readers.length > 1, "Readers should be more than 1");
        checkArgument(batchSize > 0, "Batch size must be greater than 0");

        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
        this.readers = readers;
        this.batchSize = batchSize;

        this.pending = new RecordIterator[readers.length];
        this.finished = new boolean[readers.length];
        this.rowsEmittedInCurrentBatch = 0;
        this.endOfInput = false;
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (endOfInput) {
            return null;
        }

        rowsEmittedInCurrentBatch = 0;
        DataEvolutionRow row = new DataEvolutionRow(readers.length, rowOffsets, fieldOffsets);
        return new DataEvolutionAlignedIterator(this, row);
    }

    @Nullable
    InternalRow nextRow(DataEvolutionRow row) throws IOException {
        if (endOfInput) {
            return null;
        }

        // Fetch one row from each non-null reader.
        for (int i = 0; i < readers.length; i++) {
            if (readers[i] == null) {
                // This reader does not contribute any fields.
                continue;
            }

            InternalRow buffered = fetchNextFromReader(i);
            if (buffered == null) {
                markEndOfInput();
                return null;
            }

            row.setRow(i, buffered);
        }

        return row;
    }

    private InternalRow fetchNextFromReader(int readerIndex) throws IOException {
        while (true) {
            if (finished[readerIndex]) {
                return null;
            }

            RecordIterator<InternalRow> iterator = pending[readerIndex];
            if (iterator == null) {
                iterator = readers[readerIndex].readBatch();
                if (iterator == null) {
                    finished[readerIndex] = true;
                    return null;
                }
                pending[readerIndex] = iterator;
            }

            InternalRow next = iterator.next();
            if (next != null) {
                return next;
            }

            // current batch is exhausted, release and try next batch
            iterator.releaseBatch();
            pending[readerIndex] = null;
        }
    }

    private void markEndOfInput() {
        endOfInput = true;
        // Release all pending batches.
        for (int i = 0; i < pending.length; i++) {
            RecordIterator<InternalRow> iterator = pending[i];
            if (iterator != null) {
                iterator.releaseBatch();
                pending[i] = null;
            }
        }
    }

    boolean isEndOfInput() {
        return endOfInput;
    }

    int getBatchSize() {
        return batchSize;
    }

    int getRowsEmittedInCurrentBatch() {
        return rowsEmittedInCurrentBatch;
    }

    void incrementRowsEmittedInCurrentBatch() {
        rowsEmittedInCurrentBatch++;
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(readers);
        } catch (Exception e) {
            throw new IOException("Failed to close inner readers", e);
        }
    }

    /**
     * A {@link org.apache.paimon.reader.RecordReader.RecordIterator} which aligns rows from
     * multiple inner readers and assembles them into a {@link DataEvolutionRow}.
     */
    class DataEvolutionAlignedIterator implements RecordReader.RecordIterator<InternalRow> {

        private final DataEvolutionFileReader fileReader;
        private final DataEvolutionRow row;

        DataEvolutionAlignedIterator(DataEvolutionFileReader fileReader, DataEvolutionRow row) {
            this.fileReader = fileReader;
            this.row = row;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            if (fileReader.isEndOfInput()
                    || fileReader.getRowsEmittedInCurrentBatch() >= fileReader.getBatchSize()) {
                return null;
            }

            InternalRow nextRow = fileReader.nextRow(row);
            if (nextRow != null) {
                fileReader.incrementRowsEmittedInCurrentBatch();
            }
            return nextRow;
        }

        @Override
        public void releaseBatch() {
            // Batches of inner readers are released when they are exhausted inside
            // {@link DataEvolutionFileReader}. Nothing to do here.
        }
    }
}
