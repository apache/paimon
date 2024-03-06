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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordWithPositionIterator;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements RecordWithPositionIterator<InternalRow> {

    private final ColumnarRow rowData;
    private final Runnable recycler;

    private int num;
    private int nextPos;
    private long nextGlobalPos;

    public ColumnarRowIterator(ColumnarRow rowData, @Nullable Runnable recycler) {
        super(recycler);
        this.rowData = rowData;
        this.recycler = recycler;
    }

    /**
     * Reset the number of rows in the vectorized batch, the start position and the global start
     * position in this batch.
     */
    public void reset(int num, long nextGlobalPos) {
        this.num = num;
        this.nextPos = 0;
        this.nextGlobalPos = nextGlobalPos;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (nextPos < num) {
            rowData.setRowId(nextPos++);
            nextGlobalPos++;
            return rowData;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        return nextGlobalPos - 1;
    }

    public ColumnarRowIterator copy(ColumnVector[] vectors) {
        ColumnarRowIterator newIterator = new ColumnarRowIterator(rowData.copy(vectors), recycler);
        newIterator.reset(num, nextGlobalPos);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = rowData.vectorizedColumnBatch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
            if (partitionInfo != null) {
                vectors = VectorMappingUtils.createPartitionMappedVectors(partitionInfo, vectors);
            }
            if (indexMapping != null) {
                vectors = VectorMappingUtils.createIndexMappedVectors(indexMapping, vectors);
            }
            return copy(vectors);
        }
        return this;
    }
}
