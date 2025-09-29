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

package org.apache.paimon.append;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A record reader that merges all batches from a multi-batch reader into a single concatenated
 * batch. This reader wraps another RecordReader that produces multiple batches and presents them as
 * a single continuous stream of records.
 *
 * <p>The MergeAllBatchReader is particularly useful in scenarios where you need to process multiple
 * batches as a unified data stream, such as when reading from multiple files or partitions that
 * should be treated as a single logical dataset.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Concatenates all batches from the underlying reader into one continuous batch
 *   <li>Automatically handles batch transitions and resource cleanup
 *   <li>Provides a single readBatch() call that returns all data
 *   <li>Properly manages memory by releasing batches after consumption
 * </ul>
 *
 * <p>This reader is commonly used in data evolution scenarios where multiple file formats or
 * schemas need to be read as a unified stream.
 */
public class ForceSingleBatchReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> multiBatchReader;
    private ConcatBatch batch;

    public ForceSingleBatchReader(RecordReader<InternalRow> multiBatchReader) {
        this.multiBatchReader = multiBatchReader;
        this.batch = new ConcatBatch(multiBatchReader);
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> returned = batch;
        batch = null;
        return returned;
    }

    @Override
    public void close() throws IOException {
        multiBatchReader.close();
    }

    private static class ConcatBatch implements RecordIterator<InternalRow> {

        private final RecordReader<InternalRow> reader;
        private RecordIterator<InternalRow> currentBatch;

        private ConcatBatch(RecordReader<InternalRow> reader) {
            this.reader = reader;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            if (currentBatch == null) {
                currentBatch = reader.readBatch();
                if (currentBatch == null) {
                    return null;
                }
            }

            InternalRow next = currentBatch.next();

            if (next == null) {
                currentBatch.releaseBatch();
                currentBatch = null;
                return next();
            }

            return next;
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
