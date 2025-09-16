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
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link RecordReader.RecordIterator} that returns {@link InternalRow}s. The next row is set by
 * {@link ColumnarRow#setRowId}.
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow> {

    protected final Path filePath;
    protected final ColumnarRow row;
    protected final Runnable recycler;

    protected int num;
    protected int index;
    protected int returnedPositionIndex;
    protected long returnedPosition;
    protected LongIterator positionIterator;

    public ColumnarRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        super(recycler);
        this.filePath = filePath;
        this.row = row;
        this.recycler = recycler;
    }

    public void reset(long nextFilePos) {
        reset(LongIterator.fromRange(nextFilePos, nextFilePos + row.batch().getNumRows()));
    }

    public void reset(LongIterator positions) {
        this.positionIterator = positions;
        this.num = row.batch().getNumRows();
        this.index = 0;
        this.returnedPositionIndex = 0;
        this.returnedPosition = -1;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (index < num) {
            row.setRowId(index++);
            return row;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        for (int i = 0; i < index - returnedPositionIndex; i++) {
            returnedPosition = positionIterator.next();
        }
        returnedPositionIndex = index;
        if (returnedPosition == -1) {
            throw new IllegalStateException("returnedPosition() is called before next()");
        }

        return returnedPosition;
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    protected ColumnarRowIterator copy(ColumnVector[] vectors) {
        // We should call copy only when the iterator is at the beginning of the file.
        checkArgument(returnedPositionIndex == 0, "copy() should not be called after next()");
        ColumnarRowIterator newIterator =
                new ColumnarRowIterator(filePath, row.copy(vectors), recycler);
        newIterator.reset(positionIterator);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = row.batch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
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

    public ColumnarRowIterator assignRowTracking(
            Long firstRowId, Long snapshotId, Map<String, Integer> meta) {
        VectorizedColumnBatch vectorizedColumnBatch = row.batch();
        ColumnVector[] vectors = vectorizedColumnBatch.columns;

        if (meta.containsKey(SpecialFields.ROW_ID.name())) {
            Integer index = meta.get(SpecialFields.ROW_ID.name());
            final ColumnVector rowIdVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (rowIdVector.isNullAt(i)) {
                                return firstRowId + returnedPosition();
                            } else {
                                return ((LongColumnVector) rowIdVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        if (meta.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
            Integer index = meta.get(SpecialFields.SEQUENCE_NUMBER.name());
            final ColumnVector versionVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (versionVector.isNullAt(i)) {
                                return snapshotId;
                            } else {
                                return ((LongColumnVector) versionVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        copy(vectors);
        return this;
    }
}
