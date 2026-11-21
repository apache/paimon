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
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.reader.ScoreRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Return value with score. */
public class IndexedSplitRecordReader implements ScoreRecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;
    @Nullable private final Map<Long, Float> rowIdToScore;
    private final int rowIdIndex;
    private final ProjectedRow projectedRow;

    public IndexedSplitRecordReader(RecordReader<InternalRow> reader, Info info) {
        this.reader = reader;
        this.rowIdToScore = info.rowIdToScore;
        this.rowIdIndex = info.rowIdIndex;
        this.projectedRow = info.projectedRow;
    }

    @Nullable
    @Override
    public ScoreRecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        ScoreRecordIterator<InternalRow> scored =
                new ScoreRecordIterator<InternalRow>() {

                    private float score = Float.NaN;
                    private long rowId;

                    @Override
                    public float returnedScore() {
                        return score;
                    }

                    @Override
                    public long returnedRowId() {
                        return rowId;
                    }

                    @Override
                    public InternalRow next() throws IOException {
                        InternalRow row = iterator.next();
                        if (row != null && rowIdIndex >= 0) {
                            this.rowId = row.getLong(rowIdIndex);
                            if (rowIdToScore != null) {
                                this.score = rowIdToScore.get(rowId);
                            }
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
        return iterator instanceof FileRecordIterator
                ? new FileScoreRecordIterator<>(scored, (FileRecordIterator<?>) iterator)
                : scored;
    }

    private static class FileScoreRecordIterator<T>
            implements FileRecordIterator<T>, ScoreRecordIterator<T> {

        private final ScoreRecordIterator<T> scored;
        private final FileRecordIterator<?> file;

        private FileScoreRecordIterator(ScoreRecordIterator<T> scored, FileRecordIterator<?> file) {
            this.scored = scored;
            this.file = file;
        }

        @Override
        public long returnedPosition() {
            return file.returnedPosition();
        }

        @Override
        public Path filePath() {
            return file.filePath();
        }

        @Override
        public float returnedScore() {
            return scored.returnedScore();
        }

        @Override
        public long returnedRowId() {
            return scored.returnedRowId();
        }

        @Nullable
        @Override
        public T next() throws IOException {
            return scored.next();
        }

        @Override
        public void releaseBatch() {
            scored.releaseBatch();
        }

        @Override
        public <R> FileScoreRecordIterator<R> transform(Function<T, R> function) {
            return new FileScoreRecordIterator<>(scored.transform(function), file);
        }

        @Override
        public FileScoreRecordIterator<T> filter(Filter<T> filter) {
            return new FileScoreRecordIterator<>(scored.filter(filter), file);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static Info readInfo(RowType readRowType, IndexedSplit indexedSplit) {
        Map<Long, Float> rowIdToScore = null;
        float[] scores = indexedSplit.scores();
        if (scores != null) {
            rowIdToScore = new HashMap<>(scores.length);
            int index = 0;
            checkArgument(
                    indexedSplit.rowRanges().stream().mapToLong(Range::count).sum()
                            == scores.length,
                    "Scores length does not match row ranges in indexed split.");
            for (Range range : indexedSplit.rowRanges()) {
                for (long i = range.from; i <= range.to; i++) {
                    rowIdToScore.put(i, scores[index++]);
                }
            }
        }

        int rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        RowType actualReadType = readRowType;
        ProjectedRow projectedRow = null;

        if (rowIdToScore != null && rowIdIndex == -1) {
            actualReadType = SpecialFields.rowTypeWithRowId(readRowType);
            rowIdIndex = actualReadType.getFieldCount() - 1;
            int[] mappings = new int[readRowType.getFieldCount()];
            for (int i = 0; i < readRowType.getFieldCount(); i++) {
                mappings[i] = i;
            }
            projectedRow = ProjectedRow.from(mappings);
        }

        return new Info(rowIdToScore, rowIdIndex, actualReadType, projectedRow);
    }

    /** Information package. */
    public static class Info {
        @Nullable public final Map<Long, Float> rowIdToScore;
        public final int rowIdIndex;
        public final RowType actualReadType;
        @Nullable public final ProjectedRow projectedRow;

        public Info(
                @Nullable Map<Long, Float> rowIdToScore,
                int rowIdIndex,
                RowType actualReadType,
                @Nullable ProjectedRow projectedRow) {
            this.rowIdToScore = rowIdToScore;
            this.rowIdIndex = rowIdIndex;
            this.actualReadType = actualReadType;
            this.projectedRow = projectedRow;
        }
    }
}
