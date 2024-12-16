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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.BiFunction;

/** The reader which will pack the update before and update after message together. */
public class PackChangelogReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;
    private final BiFunction<InternalRow, InternalRow, InternalRow> function;
    private final InternalRowSerializer serializer;
    private boolean initialized = false;

    public PackChangelogReader(
            RecordReader<InternalRow> reader,
            BiFunction<InternalRow, InternalRow, InternalRow> function,
            RowType rowType) {
        this.reader = reader;
        this.function = function;
        this.serializer = new InternalRowSerializer(rowType);
    }

    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (!initialized) {
            initialized = true;
            return new InternRecordIterator(reader, function, serializer);
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static class InternRecordIterator implements RecordIterator<InternalRow> {

        private RecordIterator<InternalRow> currentBatch;

        private final BiFunction<InternalRow, InternalRow, InternalRow> function;
        private final RecordReader<InternalRow> reader;
        private final InternalRowSerializer serializer;
        private boolean endOfData;

        public InternRecordIterator(
                RecordReader<InternalRow> reader,
                BiFunction<InternalRow, InternalRow, InternalRow> function,
                InternalRowSerializer serializer) {
            this.reader = reader;
            this.function = function;
            this.serializer = serializer;
            this.endOfData = false;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            InternalRow row1 = nextRow();
            if (row1 == null) {
                return null;
            }
            InternalRow row2 = null;
            if (row1.getRowKind() == RowKind.UPDATE_BEFORE) {
                row1 = serializer.copy(row1);
                row2 = nextRow();
            }
            return function.apply(row1, row2);
        }

        @Nullable
        private InternalRow nextRow() throws IOException {
            InternalRow row = null;
            while (!endOfData && row == null) {
                RecordIterator<InternalRow> batch = nextBatch();
                if (batch == null) {
                    endOfData = true;
                    return null;
                }

                row = batch.next();
                if (row == null) {
                    releaseBatch();
                }
            }
            return row;
        }

        @Nullable
        private RecordIterator<InternalRow> nextBatch() throws IOException {
            if (currentBatch == null) {
                currentBatch = reader.readBatch();
            }
            return currentBatch;
        }

        @Override
        public void releaseBatch() {
            if (currentBatch != null) {
                currentBatch.releaseBatch();
                currentBatch = null;
            }
        }
    }
}
