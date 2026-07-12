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

package org.apache.paimon.table.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.IntFunction;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Reads selected physical file positions and exposes their vector-search scores. */
public class PrimaryKeyVectorPositionReader implements RecordReader<InternalRow> {

    private final FileRecordReader<InternalRow> reader;
    private final RoaringBitmap32 rowPositions;
    private final IntFunction<Float> scoreGetter;
    private final int lastPosition;
    private boolean exhausted;

    public PrimaryKeyVectorPositionReader(
            FileRecordReader<InternalRow> reader,
            RoaringBitmap32 rowPositions,
            IntFunction<Float> scoreGetter) {
        checkArgument(!rowPositions.isEmpty(), "Selected row positions must not be empty.");
        this.reader = reader;
        this.rowPositions = rowPositions.clone();
        this.scoreGetter = scoreGetter;
        this.lastPosition = rowPositions.last();
    }

    @Nullable
    @Override
    public ScoreRecordIterator<InternalRow> readBatch() throws IOException {
        if (exhausted) {
            return null;
        }
        FileRecordIterator<InternalRow> batch = reader.readBatch();
        if (batch == null) {
            return null;
        }
        FileRecordIterator<InternalRow> selected = batch.selection(rowPositions);
        return new ScoreRecordIterator<InternalRow>() {

            private long returnedPosition = -1;
            private float returnedScore = Float.NaN;

            @Override
            public float returnedScore() {
                return returnedScore;
            }

            @Override
            public long returnedRowId() {
                return returnedPosition;
            }

            @Nullable
            @Override
            public InternalRow next() throws IOException {
                InternalRow row = selected.next();
                if (row != null) {
                    returnedPosition = selected.returnedPosition();
                    returnedScore = scoreGetter.apply((int) returnedPosition);
                    if (returnedPosition >= lastPosition) {
                        exhausted = true;
                    }
                }
                return row;
            }

            @Override
            public void releaseBatch() {
                selected.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
