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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/** Tests physical row-position filtering with vector scores. */
class PrimaryKeyVectorPositionReaderTest {

    @Test
    void testSelectsPhysicalPositionsAndReturnsScores() throws Exception {
        PrimaryKeyVectorPositionReader reader =
                new PrimaryKeyVectorPositionReader(
                        new TestingFileReader(5),
                        RoaringBitmap32.bitmapOf(1, 3),
                        position -> position / 10F);

        ScoreRecordIterator<InternalRow> batch = reader.readBatch();
        assertThat(batch.next().getInt(0)).isEqualTo(1);
        assertThat(batch.returnedRowId()).isEqualTo(1);
        assertThat(batch.returnedScore()).isEqualTo(0.1F);
        assertThat(batch.next().getInt(0)).isEqualTo(3);
        assertThat(batch.returnedRowId()).isEqualTo(3);
        assertThat(batch.returnedScore()).isEqualTo(0.3F);
        assertThat(batch.next()).isNull();
        assertThat(reader.readBatch()).isNull();
    }

    @Test
    void testSelectedPositionsAcrossBatches() throws Exception {
        PrimaryKeyVectorPositionReader reader =
                new PrimaryKeyVectorPositionReader(
                        new TestingFileReader(5, 2),
                        RoaringBitmap32.bitmapOf(1, 3),
                        position -> position / 10F);

        ScoreRecordIterator<InternalRow> firstBatch = reader.readBatch();
        assertThat(firstBatch.next().getInt(0)).isEqualTo(1);
        assertThat(firstBatch.returnedRowId()).isEqualTo(1);
        assertThat(firstBatch.next()).isNull();

        ScoreRecordIterator<InternalRow> secondBatch = reader.readBatch();
        assertThat(secondBatch.next().getInt(0)).isEqualTo(3);
        assertThat(secondBatch.returnedScore()).isEqualTo(0.3F);
        assertThat(secondBatch.next()).isNull();
        assertThat(reader.readBatch()).isNull();
    }

    @Test
    void testKeyValueTableReadRoutesPhysicalSplitToRawReader() throws Exception {
        RawFileSplitRead rawRead = mock(RawFileSplitRead.class, RETURNS_SELF);
        IndexedSplit split = selectedSplit();
        KeyValueTableRead tableRead =
                new KeyValueTableRead(
                        () -> mock(MergeFileSplitRead.class),
                        () -> rawRead,
                        mock(TableSchema.class));

        assertThat(tableRead.createReader(split))
                .isInstanceOf(PrimaryKeyVectorPositionReader.class);
        verify(rawRead, never()).createReader(split);
    }

    private static IndexedSplit selectedSplit() {
        DataFileMeta dataFile =
                DataFileMeta.forAppend(
                        "data-file",
                        100,
                        5,
                        SimpleStats.EMPTY_STATS,
                        0,
                        0,
                        1,
                        Collections.emptyList(),
                        null,
                        FileSource.APPEND,
                        null,
                        null,
                        null,
                        null);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(3)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withTotalBuckets(1)
                        .withDataFiles(Collections.singletonList(dataFile))
                        .build();
        return new IndexedSplit(
                dataSplit, Collections.singletonList(new Range(1, 1)), new float[] {0.5F});
    }

    private static class TestingFileReader implements FileRecordReader<InternalRow> {

        private final int rowCount;
        private final int batchSize;
        private int nextPosition;

        private TestingFileReader(int rowCount) {
            this(rowCount, rowCount);
        }

        private TestingFileReader(int rowCount, int batchSize) {
            this.rowCount = rowCount;
            this.batchSize = batchSize;
        }

        @Override
        public FileRecordIterator<InternalRow> readBatch() {
            if (nextPosition == rowCount) {
                return null;
            }
            int startPosition = nextPosition;
            nextPosition = Math.min(rowCount, nextPosition + batchSize);
            return new TestingFileIterator(startPosition, nextPosition);
        }

        @Override
        public void close() {}
    }

    private static class TestingFileIterator implements FileRecordIterator<InternalRow> {

        private final int endPosition;
        private int nextPosition;
        private int returnedPosition = -1;

        private TestingFileIterator(int startPosition, int endPosition) {
            this.nextPosition = startPosition;
            this.endPosition = endPosition;
        }

        @Override
        public long returnedPosition() {
            return returnedPosition;
        }

        @Override
        public Path filePath() {
            return new Path("/test");
        }

        @Override
        public InternalRow next() {
            if (nextPosition == endPosition) {
                return null;
            }
            returnedPosition = nextPosition;
            return GenericRow.of(nextPosition++);
        }

        @Override
        public void releaseBatch() {}
    }
}
