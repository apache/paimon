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

package org.apache.paimon.arrow.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.deletionvectors.ApplyDeletionFileRecordIterator;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.utils.IntArrayList;

import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

/** To convert {@link VectorizedColumnBatch} to Arrow format. */
public class ArrowBatchWriter extends ArrowWriter {

    private VectorizedColumnBatch batch;
    private @Nullable int[] pickedInColumn;
    private int totalNumRows;
    private int startIndex;

    public ArrowBatchWriter(VectorSchemaRoot root, ArrowFieldWriter[] fieldWriters) {
        super(root, fieldWriters);
    }

    @Override
    public void doWrite(int maxBatchRows) {
        int batchRows = Math.min(maxBatchRows, totalNumRows - startIndex);
        ColumnVector[] columns = batch.columns;
        for (int i = 0; i < columns.length; i++) {
            fieldWriters[i].write(columns[i], pickedInColumn, startIndex, batchRows);
        }
        root.setRowCount(batchRows);

        startIndex += batchRows;
        if (startIndex >= totalNumRows) {
            releaseIterator();
        }
    }

    public void reset(ApplyDeletionFileRecordIterator iterator) {
        this.iterator = iterator;

        FileRecordIterator<InternalRow> innerIterator = iterator.iterator();
        this.batch = ((VectorizedRecordIterator) innerIterator).batch();

        long firstReturnedPosition = innerIterator.returnedPosition() + 1;
        DeletionVector deletionVector = iterator.deletionVector();
        int originNumRows = this.batch.getNumRows();
        IntArrayList picked = new IntArrayList(originNumRows);
        for (int i = 0; i < originNumRows; i++) {
            long returnedPosition = firstReturnedPosition + i;
            if (!deletionVector.isDeleted(returnedPosition)) {
                picked.add(i);
            }
        }
        if (picked.size() == originNumRows) {
            this.pickedInColumn = null;
            this.totalNumRows = originNumRows;
        } else {
            this.pickedInColumn = picked.toArray();
            this.totalNumRows = this.pickedInColumn.length;
        }

        this.startIndex = 0;
    }

    public void reset(VectorizedRecordIterator iterator) {
        this.iterator = iterator;
        this.batch = iterator.batch();
        this.pickedInColumn = null;
        this.totalNumRows = this.batch.getNumRows();
        this.startIndex = 0;
    }
}
