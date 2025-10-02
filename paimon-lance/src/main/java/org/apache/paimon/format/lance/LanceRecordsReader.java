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

package org.apache.paimon.format.lance;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.lance.jni.LanceReader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import com.lancedb.lance.util.Range;
import org.apache.arrow.vector.VectorSchemaRoot;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** File reade for lance. */
public class LanceRecordsReader implements FileRecordReader<InternalRow> {

    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final LanceReader reader;
    private final PositionGenerator positionGenerator;

    public LanceRecordsReader(
            Path path,
            @Nullable List<Pair<Integer, Integer>> selectionRangesArray,
            RowType projectedRowType,
            int batchSize,
            Map<String, String> storageOptions) {
        this.filePath = path;
        List<Range> ranges = null;
        if (selectionRangesArray != null) {
            ranges =
                    selectionRangesArray.stream()
                            .map(pair -> Range.of(pair.getLeft(), pair.getRight()))
                            .collect(Collectors.toList());
        }
        this.reader =
                new LanceReader(
                        filePath.toString(), projectedRowType, ranges, batchSize, storageOptions);
        this.arrowBatchReader = new ArrowBatchReader(projectedRowType, true);

        if (ranges != null) {
            this.positionGenerator = new RangePositionGenerator(ranges);
        } else {
            this.positionGenerator = new AccumulatorPositionGenerator();
        }
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        try {
            VectorSchemaRoot vsr = reader.readBatch();
            Iterator<InternalRow> rows = arrowBatchReader.readBatch(vsr).iterator();
            return new FileRecordIterator<InternalRow>() {
                @Override
                public long returnedPosition() {
                    return positionGenerator.returnedPosition();
                }

                @Override
                public Path filePath() {
                    return filePath;
                }

                @Nullable
                @Override
                public InternalRow next() {
                    if (!rows.hasNext()) {
                        return null;
                    }

                    positionGenerator.next();
                    return rows.next();
                }

                @Override
                public void releaseBatch() {
                    vsr.close();
                }
            };
        } catch (EOFException e) {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    interface PositionGenerator {

        long returnedPosition();

        void next();
    }

    private static class RangePositionGenerator implements PositionGenerator {
        private final List<Range> ranges;
        private int currentRangeIndex = 0;
        private int currentPosition = -1;

        public RangePositionGenerator(List<Range> ranges) {
            this.ranges = ranges;
        }

        @Override
        public long returnedPosition() {
            return currentPosition;
        }

        @Override
        public void next() {
            if (currentRangeIndex < ranges.size()) {
                currentPosition++;

                // Check if we have reached the end of the current range
                if (currentPosition >= ranges.get(currentRangeIndex).getEnd()) {
                    currentRangeIndex++;

                    if (currentRangeIndex < ranges.size()) {
                        // Move to the start of the next range
                        currentPosition = ranges.get(currentRangeIndex).getStart();
                    } else {
                        throw new RuntimeException("No more ranges available");
                    }
                }
            }
        }
    }

    private static class AccumulatorPositionGenerator implements PositionGenerator {
        private int currentPosition = -1;

        @Override
        public long returnedPosition() {
            return currentPosition;
        }

        @Override
        public void next() {
            currentPosition++;
        }
    }
}
