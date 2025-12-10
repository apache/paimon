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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/** Return value with score. */
public class ReaderWithScore implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;
    @Nullable private final Map<Long, Float> rowIdToScore;
    private final int rowIdIndex;
    private final ProjectedRow projectedRow;

    public ReaderWithScore(
            RecordReader<InternalRow> reader,
            @Nullable Map<Long, Float> rowIdToScore,
            int rowIdIndex,
            @Nullable ProjectedRow projectedRow) {
        this.reader = reader;
        this.rowIdToScore = rowIdToScore;
        this.rowIdIndex = rowIdIndex;
        this.projectedRow = projectedRow;
    }

    @Nullable
    @Override
    public ScoreRecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        return new ScoreRecordIterator<InternalRow>() {

            private Float score = null;

            @Override
            public Float returnedScore() {
                return score;
            }

            @Override
            public InternalRow next() throws IOException {
                InternalRow row = iterator.next();
                if (row != null && rowIdToScore != null) {
                    Long rowId = row.getLong(rowIdIndex);
                    this.score = rowIdToScore.get(rowId);
                    if (projectedRow != null) {
                        projectedRow.replaceRow(row);
                        return projectedRow;
                    }
                }
                return row;
            }

            @Override
            public void releaseBatch() {
                iterator.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
