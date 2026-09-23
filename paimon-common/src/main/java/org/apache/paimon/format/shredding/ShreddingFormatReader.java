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

package org.apache.paimon.format.shredding;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.BatchColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.shredding.ShreddingReadPlan;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/** A reader wrapper that converts physical batches back to the logical row layout. */
public class ShreddingFormatReader implements FileRecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> delegate;
    private final ShreddingReadPlan readPlan;

    public ShreddingFormatReader(
            FileRecordReader<InternalRow> delegate, ShreddingReadPlan readPlan) {
        this.delegate = delegate;
        this.readPlan = readPlan;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = delegate.readBatch();
        if (iterator == null || readPlan.isIdentity()) {
            return iterator;
        }
        if (!(iterator instanceof BatchColumnarRowIterator)) {
            throw new UnsupportedOperationException(
                    "Shredding read plan requires a columnar batch iterator.");
        }
        BatchColumnarRowIterator columnarIterator = (BatchColumnarRowIterator) iterator;
        VectorizedColumnBatch logicalBatch =
                readPlan.batchAssembler().assemble(columnarIterator.batch());
        return columnarIterator.copy(logicalBatch.columns);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
