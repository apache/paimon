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
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow>, VectorizedRecordIterator {

    private final Path filePath;
    private final ColumnarRow row;
    private final Runnable recycler;

    private int num;
    private int nextPos;
    private long nextFilePos;

    public ColumnarRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        super(recycler);
        this.filePath = filePath;
        this.row = row;
        this.recycler = recycler;
    }

    public void reset(long nextFilePos) {
        this.num = row.batch().getNumRows();
        this.nextPos = 0;
        this.nextFilePos = nextFilePos;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (nextPos < num) {
            row.setRowId(nextPos++);
            nextFilePos++;
            return row;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        return nextFilePos - 1;
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    public ColumnarRowIterator copy(ColumnVector[] vectors) {
        ColumnarRowIterator newIterator =
                new ColumnarRowIterator(filePath, row.copy(vectors), recycler);
        newIterator.reset(nextFilePos);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable int[] trimmedKeyMapping,
            @Nullable PartitionInfo partitionInfo,
            @Nullable int[] indexMapping) {
        if (trimmedKeyMapping != null || partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = row.batch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
            if (trimmedKeyMapping != null) {
                vectors = VectorMappingUtils.createMappedVectors(trimmedKeyMapping, vectors);
            }
            if (partitionInfo != null) {
                vectors = VectorMappingUtils.createPartitionMappedVectors(partitionInfo, vectors);
            }
            if (indexMapping != null) {
                vectors = VectorMappingUtils.createMappedVectors(indexMapping, vectors);
            }
            return copy(vectors);
        }
        return this;
    }

    @Override
    public VectorizedColumnBatch batch() {
        return row.batch();
    }
}
