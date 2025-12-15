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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link IndexedSplitRecordReader}. */
public class IndexedSplitRecordReaderTest {

    @Test
    public void testReadWithScores() throws IOException {
        // Create rows with ROW_ID at position 2
        // Schema: (id INT, name STRING, _ROW_ID BIGINT)
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), 0L),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 1L),
                        GenericRow.of(3, BinaryString.fromString("Charlie"), 2L));

        MockRecordReader mockReader = new MockRecordReader(rows);

        // Create IndexedSplit with scores
        IndexedSplit indexedSplit =
                createIndexedSplit(
                        Collections.singletonList(new Range(0, 2)), new float[] {0.9f, 0.8f, 0.7f});

        // Create RowType with ROW_ID
        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "_ROW_ID"});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);
        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);

        // Read and verify
        ScoreRecordIterator<InternalRow> iterator = reader.readBatch();
        assertThat(iterator).isNotNull();

        // Row 0: score = 0.9
        InternalRow row0 = iterator.next();
        assertThat(row0).isNotNull();
        assertThat(row0.getInt(0)).isEqualTo(1);
        assertThat(row0.getString(1).toString()).isEqualTo("Alice");
        assertThat(iterator.returnedScore()).isEqualTo(0.9f);

        // Row 1: score = 0.8
        InternalRow row1 = iterator.next();
        assertThat(row1).isNotNull();
        assertThat(row1.getInt(0)).isEqualTo(2);
        assertThat(row1.getString(1).toString()).isEqualTo("Bob");
        assertThat(iterator.returnedScore()).isEqualTo(0.8f);

        // Row 2: score = 0.7
        InternalRow row2 = iterator.next();
        assertThat(row2).isNotNull();
        assertThat(row2.getInt(0)).isEqualTo(3);
        assertThat(row2.getString(1).toString()).isEqualTo("Charlie");
        assertThat(iterator.returnedScore()).isEqualTo(0.7f);

        // No more rows
        assertThat(iterator.next()).isNull();

        reader.close();
    }

    @Test
    public void testReadWithoutScores() throws IOException {
        // Create rows with ROW_ID
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), 0L),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 1L));

        MockRecordReader mockReader = new MockRecordReader(rows);

        // Create IndexedSplit without scores
        IndexedSplit indexedSplit =
                createIndexedSplit(Collections.singletonList(new Range(0, 1)), null);

        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "_ROW_ID"});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);
        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);

        // Read and verify
        ScoreRecordIterator<InternalRow> iterator = reader.readBatch();
        assertThat(iterator).isNotNull();

        InternalRow row0 = iterator.next();
        assertThat(row0).isNotNull();
        assertThat(row0.getInt(0)).isEqualTo(1);
        // Score should be NaN when no scores provided
        assertThat(Float.isNaN(iterator.returnedScore())).isTrue();

        InternalRow row1 = iterator.next();
        assertThat(row1).isNotNull();
        assertThat(row1.getInt(0)).isEqualTo(2);

        assertThat(iterator.next()).isNull();

        reader.close();
    }

    @Test
    public void testReadInfoWithRowIdInReadType() {
        // RowType already contains _ROW_ID
        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()
                        },
                        new String[] {"id", "name", "_ROW_ID"});

        IndexedSplit indexedSplit =
                createIndexedSplit(
                        Collections.singletonList(new Range(0, 9)),
                        new float[] {0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f, 0.7f, 0.8f, 0.9f, 1.0f});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);

        // rowIdIndex should be 2 (the position of _ROW_ID)
        assertThat(info.rowIdIndex).isEqualTo(2);
        // actualReadType should be the same as readRowType
        assertThat(info.actualReadType).isEqualTo(readRowType);
        // No projection needed
        assertThat(info.projectedRow).isNull();
        // rowIdToScore should be populated
        assertThat(info.rowIdToScore).isNotNull();
        assertThat(info.rowIdToScore).hasSize(10); // Range [0, 9] has 10 elements
    }

    @Test
    public void testReadInfoWithoutRowIdInReadType() {
        // RowType does NOT contain _ROW_ID
        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"id", "name"});

        IndexedSplit indexedSplit =
                createIndexedSplit(
                        Collections.singletonList(new Range(0, 4)),
                        new float[] {0.1f, 0.2f, 0.3f, 0.4f, 0.5f});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);

        // actualReadType should have _ROW_ID appended
        assertThat(info.actualReadType.getFieldCount()).isEqualTo(3);
        assertThat(info.actualReadType.getFieldNames()).contains("_ROW_ID");

        // rowIdIndex should be the last field
        assertThat(info.rowIdIndex).isEqualTo(2);

        // projectedRow should be set to project out _ROW_ID
        assertThat(info.projectedRow).isNotNull();

        // rowIdToScore should be populated
        assertThat(info.rowIdToScore).isNotNull();
        assertThat(info.rowIdToScore).hasSize(5);
        assertThat(info.rowIdToScore.get(0L)).isEqualTo(0.1f);
        assertThat(info.rowIdToScore.get(4L)).isEqualTo(0.5f);
    }

    @Test
    public void testReadInfoWithoutScores() {
        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"id", "name"});

        // No scores
        IndexedSplit indexedSplit =
                createIndexedSplit(Collections.singletonList(new Range(0, 9)), null);

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);

        // rowIdToScore should be null
        assertThat(info.rowIdToScore).isNull();
        // actualReadType should be the same as input (no need to add _ROW_ID)
        assertThat(info.actualReadType).isEqualTo(readRowType);
        // projectedRow should be null
        assertThat(info.projectedRow).isNull();
    }

    @Test
    public void testReadWithProjection() throws IOException {
        // Simulate reading with projection - _ROW_ID is added internally
        // Schema: (id INT, name STRING, _ROW_ID BIGINT)
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Alice"), 10L),
                        GenericRow.of(2, BinaryString.fromString("Bob"), 11L));

        MockRecordReader mockReader = new MockRecordReader(rows);

        // User requested schema without _ROW_ID
        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.STRING()
                        },
                        new String[] {"id", "name"});

        IndexedSplit indexedSplit =
                createIndexedSplit(
                        Collections.singletonList(new Range(10, 11)), new float[] {0.95f, 0.85f});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);
        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);

        ScoreRecordIterator<InternalRow> iterator = reader.readBatch();
        assertThat(iterator).isNotNull();

        // Row should be projected to only (id, name)
        InternalRow row0 = iterator.next();
        assertThat(row0).isNotNull();
        assertThat(row0.getFieldCount()).isEqualTo(2);
        assertThat(row0.getInt(0)).isEqualTo(1);
        assertThat(row0.getString(1).toString()).isEqualTo("Alice");
        assertThat(iterator.returnedScore()).isEqualTo(0.95f);

        InternalRow row1 = iterator.next();
        assertThat(row1).isNotNull();
        assertThat(row1.getFieldCount()).isEqualTo(2);
        assertThat(row1.getInt(0)).isEqualTo(2);
        assertThat(row1.getString(1).toString()).isEqualTo("Bob");
        assertThat(iterator.returnedScore()).isEqualTo(0.85f);

        reader.close();
    }

    @Test
    public void testReadWithMultipleRanges() throws IOException {
        // Create rows with ROW_ID from multiple ranges
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, 0L), // Range [0, 1]
                        GenericRow.of(2, 1L),
                        GenericRow.of(3, 10L), // Range [10, 11]
                        GenericRow.of(4, 11L));

        MockRecordReader mockReader = new MockRecordReader(rows);

        // Two ranges with corresponding scores
        List<Range> ranges = Arrays.asList(new Range(0, 1), new Range(10, 11));
        float[] scores = new float[] {0.1f, 0.2f, 0.3f, 0.4f};

        IndexedSplit indexedSplit = createIndexedSplit(ranges, scores);

        RowType readRowType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.BIGINT()
                        },
                        new String[] {"id", "_ROW_ID"});

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);

        // Verify rowIdToScore mapping
        assertThat(info.rowIdToScore).hasSize(4);
        assertThat(info.rowIdToScore.get(0L)).isEqualTo(0.1f);
        assertThat(info.rowIdToScore.get(1L)).isEqualTo(0.2f);
        assertThat(info.rowIdToScore.get(10L)).isEqualTo(0.3f);
        assertThat(info.rowIdToScore.get(11L)).isEqualTo(0.4f);

        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);
        ScoreRecordIterator<InternalRow> iterator = reader.readBatch();

        // Verify reading with correct scores
        InternalRow row0 = iterator.next();
        assertThat(iterator.returnedScore()).isEqualTo(0.1f);

        InternalRow row1 = iterator.next();
        assertThat(iterator.returnedScore()).isEqualTo(0.2f);

        InternalRow row2 = iterator.next();
        assertThat(iterator.returnedScore()).isEqualTo(0.3f);

        InternalRow row3 = iterator.next();
        assertThat(iterator.returnedScore()).isEqualTo(0.4f);

        reader.close();
    }

    @Test
    public void testReadBatchReturnsNullWhenEmpty() throws IOException {
        MockRecordReader mockReader = new MockRecordReader(Collections.emptyList());

        IndexedSplit indexedSplit = createIndexedSplit(Arrays.asList(new Range(0, 0)), null);

        RowType readRowType = RowType.of(DataTypes.INT());

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);
        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);

        // First batch returns empty iterator, second batch returns null
        ScoreRecordIterator<InternalRow> iterator = reader.readBatch();
        assertThat(iterator.next()).isNull();

        ScoreRecordIterator<InternalRow> iterator2 = reader.readBatch();
        assertThat(iterator2).isNull();

        reader.close();
    }

    @Test
    public void testClose() throws IOException {
        MockRecordReader mockReader = new MockRecordReader(Collections.emptyList());

        IndexedSplit indexedSplit =
                createIndexedSplit(Collections.singletonList(new Range(0, 0)), null);
        RowType readRowType = RowType.of(DataTypes.INT());

        IndexedSplitRecordReader.Info info =
                IndexedSplitRecordReader.readInfo(readRowType, indexedSplit);
        IndexedSplitRecordReader reader = new IndexedSplitRecordReader(mockReader, info);

        assertThat(mockReader.isClosed()).isFalse();
        reader.close();
        assertThat(mockReader.isClosed()).isTrue();
    }

    private IndexedSplit createIndexedSplit(List<Range> ranges, @Nullable float[] scores) {
        DataFileMeta file = DataFileTestUtils.newFile("test-file", 0, 1, 100, 1000L);
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1L)
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withBucketPath("bucket-0")
                        .withDataFiles(Collections.singletonList(file))
                        .build();
        return new IndexedSplit(dataSplit, ranges, scores);
    }

    /** Mock RecordReader for testing. */
    private static class MockRecordReader implements RecordReader<InternalRow> {

        private final List<InternalRow> rows;
        private boolean consumed = false;
        private boolean closed = false;

        MockRecordReader(List<InternalRow> rows) {
            this.rows = new ArrayList<>(rows);
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() {
            if (consumed) {
                return null;
            }
            consumed = true;
            return new MockRecordIterator(rows.iterator());
        }

        @Override
        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /** Mock RecordIterator for testing. */
    private static class MockRecordIterator implements RecordReader.RecordIterator<InternalRow> {

        private final Iterator<InternalRow> iterator;

        MockRecordIterator(Iterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        @Nullable
        @Override
        public InternalRow next() {
            if (iterator.hasNext()) {
                return iterator.next();
            }
            return null;
        }

        @Override
        public void releaseBatch() {
            // no-op
        }
    }
}
