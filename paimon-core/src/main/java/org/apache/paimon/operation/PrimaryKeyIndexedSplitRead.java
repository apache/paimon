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

package org.apache.paimon.operation;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.PrimaryKeyVectorPositionReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Reads an {@link IndexedSplit} as physical positions in one primary-key data file. */
public class PrimaryKeyIndexedSplitRead implements SplitRead<InternalRow> {

    private final RawFileSplitRead rawRead;

    public PrimaryKeyIndexedSplitRead(RawFileSplitRead rawRead) {
        this.rawRead = rawRead;
    }

    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        rawRead.forceKeepDelete();
        return this;
    }

    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        rawRead.withIOManager(ioManager);
        return this;
    }

    @Override
    public SplitRead<InternalRow> withReadType(RowType readType) {
        rawRead.withReadType(readType);
        return this;
    }

    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        rawRead.withFilter(predicate);
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        IndexedSplit indexedSplit = (IndexedSplit) split;
        DataSplit dataSplit = indexedSplit.dataSplit();
        checkArgument(
                dataSplit.dataFiles().size() == 1,
                "Indexed split for a primary-key table must contain exactly one data file.");
        DataFileMeta dataFile = dataSplit.dataFiles().get(0);
        RoaringBitmap32 rowPositions = new RoaringBitmap32();
        float[] scores = indexedSplit.scores();
        Map<Integer, Float> scoreByPosition = scores == null ? null : new HashMap<>();
        int scoreIndex = 0;
        for (Range range : indexedSplit.rowRanges()) {
            checkArgument(
                    range.from >= 0 && range.to < dataFile.rowCount(),
                    "Indexed row range %s is outside data file %s row count %s.",
                    range,
                    dataFile.fileName(),
                    dataFile.rowCount());
            checkArgument(
                    range.to <= Integer.MAX_VALUE,
                    "Indexed row range %s exceeds supported physical positions.",
                    range);
            for (long position = range.from; position <= range.to; position++) {
                rowPositions.add((int) position);
                if (scoreByPosition != null) {
                    checkArgument(
                            scoreIndex < scores.length,
                            "Scores length does not match row ranges in indexed split.");
                    scoreByPosition.put((int) position, scores[scoreIndex++]);
                }
            }
        }
        checkArgument(!rowPositions.isEmpty(), "Indexed split must select at least one row.");
        if (scores != null) {
            checkArgument(
                    scoreIndex == scores.length,
                    "Scores length does not match row ranges in indexed split.");
        }
        IntFunction<Float> scoreGetter =
                scoreByPosition == null ? position -> Float.NaN : scoreByPosition::get;
        FileRecordReader<InternalRow> reader = rawRead.createFileReader(dataSplit, rowPositions);
        return new PrimaryKeyVectorPositionReader(reader, rowPositions, scoreGetter);
    }
}
