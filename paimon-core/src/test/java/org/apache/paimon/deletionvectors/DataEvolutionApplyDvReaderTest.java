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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataEvolutionApplyDvReader}. */
public class DataEvolutionApplyDvReaderTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    public void testApplyUnorderedRowRangeDeletionVectors() throws IOException {
        Range laterRange = new Range(110, 119);
        Range earlierRange = new Range(100, 109);

        Map<DeletionFileKey, DeletionVector> input = new LinkedHashMap<>();
        input.put(DeletionFileKey.ofRange(earlierRange), deletionVector(3));
        input.put(DeletionFileKey.ofRange(laterRange), deletionVector(0, 4));

        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        DeletionVectorsIndexFile indexFile =
                new DeletionVectorsIndexFile(
                        fileIO, pathFactory, MemorySize.ofBytes(Long.MAX_VALUE), false);
        IndexFileMeta indexFileMeta = indexFile.writeSingleFile(input);

        Map<Range, DeletionFile> deletionFiles = new LinkedHashMap<>();
        deletionFiles.put(laterRange, deletionFile(pathFactory, indexFileMeta, laterRange));
        deletionFiles.put(earlierRange, deletionFile(pathFactory, indexFileMeta, earlierRange));

        RowType readType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {DataTypes.INT()},
                        new String[] {"value"});
        DataEvolutionApplyDvReader.Info info =
                DataEvolutionApplyDvReader.readInfo(fileIO, readType, deletionFiles);

        assertThat(info.actualReadType.getFieldNames()).containsExactly("value", "_ROW_ID");

        DataEvolutionApplyDvReader reader =
                new DataEvolutionApplyDvReader(new MockRecordReader(rows(100, 115)), info);

        assertThat(readValues(reader))
                .containsExactly(100, 101, 102, 104, 105, 106, 107, 108, 109, 111, 112, 113, 115);
    }

    @Test
    public void testApplySparseReturnedRowRanges() throws IOException {
        Range firstRange = new Range(100, 109);
        Range secondRange = new Range(1000, 1009);
        Range thirdRange = new Range(5000, 5009);

        Map<DeletionFileKey, DeletionVector> input = new LinkedHashMap<>();
        input.put(DeletionFileKey.ofRange(firstRange), deletionVector(1));
        input.put(DeletionFileKey.ofRange(secondRange), deletionVector(5));
        input.put(DeletionFileKey.ofRange(thirdRange), deletionVector(0, 9));

        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        DeletionVectorsIndexFile indexFile =
                new DeletionVectorsIndexFile(
                        fileIO, pathFactory, MemorySize.ofBytes(Long.MAX_VALUE), false);
        IndexFileMeta indexFileMeta = indexFile.writeSingleFile(input);

        Map<Range, DeletionFile> deletionFiles = new LinkedHashMap<>();
        deletionFiles.put(firstRange, deletionFile(pathFactory, indexFileMeta, firstRange));
        deletionFiles.put(secondRange, deletionFile(pathFactory, indexFileMeta, secondRange));
        deletionFiles.put(thirdRange, deletionFile(pathFactory, indexFileMeta, thirdRange));

        RowType readType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {DataTypes.INT()},
                        new String[] {"value"});
        DataEvolutionApplyDvReader.Info info =
                DataEvolutionApplyDvReader.readInfo(fileIO, readType, deletionFiles);

        DataEvolutionApplyDvReader reader =
                new DataEvolutionApplyDvReader(
                        new MockRecordReader(
                                rows(
                                        100, 101, 109, 500, 999, 1000, 1005, 1009, 3000, 5000, 5001,
                                        5009, 6000)),
                        info);

        assertThat(readValues(reader))
                .containsExactly(100, 109, 500, 999, 1000, 1009, 3000, 5001, 6000);
    }

    private DeletionFile deletionFile(
            IndexPathFactory pathFactory, IndexFileMeta indexFileMeta, Range range) {
        DeletionVectorMeta meta = indexFileMeta.dvRanges().get(DeletionFileKey.ofRange(range));
        return new DeletionFile(
                pathFactory.toPath(indexFileMeta).toString(),
                meta.offset(),
                meta.length(),
                meta.cardinality());
    }

    private static DeletionVector deletionVector(long... positions) {
        DeletionVector deletionVector = new BitmapDeletionVector();
        for (long position : positions) {
            deletionVector.delete(position);
        }
        return deletionVector;
    }

    private static List<InternalRow> rows(int from, int to) {
        List<InternalRow> rows = new ArrayList<>();
        for (int i = from; i <= to; i++) {
            rows.add(GenericRow.of(i, (long) i));
        }
        return rows;
    }

    private static List<InternalRow> rows(int... rowIds) {
        List<InternalRow> rows = new ArrayList<>();
        for (int rowId : rowIds) {
            rows.add(GenericRow.of(rowId, (long) rowId));
        }
        return rows;
    }

    private static List<Integer> readValues(RecordReader<InternalRow> reader) throws IOException {
        List<Integer> values = new ArrayList<>();
        try {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        values.add(row.getInt(0));
                        assertThat(row.getFieldCount()).isEqualTo(1);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        } finally {
            reader.close();
        }
        return values;
    }

    private IndexPathFactory pathFactory() {
        Path dir = new Path(tempPath.toUri());
        return new IndexPathFactory() {

            @Override
            public Path toPath(String fileName) {
                return new Path(dir, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(dir, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    /** Mock RecordReader for testing. */
    private static class MockRecordReader implements RecordReader<InternalRow> {

        private final List<InternalRow> rows;
        private boolean consumed;

        private MockRecordReader(List<InternalRow> rows) {
            this.rows = rows;
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
        public void close() {}
    }

    /** Mock RecordIterator for testing. */
    private static class MockRecordIterator implements RecordReader.RecordIterator<InternalRow> {

        private final Iterator<InternalRow> iterator;

        private MockRecordIterator(Iterator<InternalRow> iterator) {
            this.iterator = iterator;
        }

        @Nullable
        @Override
        public InternalRow next() {
            return iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        public void releaseBatch() {}
    }
}
