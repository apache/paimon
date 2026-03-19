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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.parquet.ParquetReaderFactory;
import org.apache.paimon.format.parquet.ParquetWriterFactory;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests INT96 timestamp decoding when selective row reads trigger skip paths. */
public class ParquetInt96RowSelectionTest {

    private static final int TIMESTAMP_PRECISION = 9;
    private static final int BATCH_SIZE = 2;
    private static final int TOTAL_ROWS = 500;
    private static final int[] SELECTED_POSITIONS = new int[] {1, 101, 201, 301, 401};
    private static final RowType ROW_TYPE = RowType.of(DataTypes.TIMESTAMP(TIMESTAMP_PRECISION));

    @TempDir private java.nio.file.Path tempDir;

    @Test
    public void testVectorizedBatchAndRowIteratorReadSelectedInt96Rows() throws IOException {
        List<Timestamp> timestamps = createTimestamps(TOTAL_ROWS);
        List<Timestamp> expected =
                Arrays.stream(SELECTED_POSITIONS)
                        .mapToObj(timestamps::get)
                        .collect(Collectors.toList());

        Path file = writeInt96File(timestamps);
        LocalFileIO fileIO = LocalFileIO.create();
        FormatReaderContext context =
                new FormatReaderContext(
                        fileIO,
                        file,
                        fileIO.getFileSize(file),
                        RoaringBitmap32.bitmapOf(SELECTED_POSITIONS));
        ParquetReaderFactory readerFactory =
                new ParquetReaderFactory(new Options(), ROW_TYPE, BATCH_SIZE, FilterCompat.NOOP);

        try (RecordReader<InternalRow> reader = readerFactory.createReader(context)) {
            int expectedIndex = 0;
            RecordReader.RecordIterator<InternalRow> iterator;
            while ((iterator = reader.readBatch()) != null) {
                assertThat(iterator).isInstanceOf(VectorizedRecordIterator.class);
                assertThat(iterator).isInstanceOf(FileRecordIterator.class);

                VectorizedColumnBatch batch = ((VectorizedRecordIterator) iterator).batch();
                assertThat(batch.getNumRows()).isPositive().isLessThanOrEqualTo(BATCH_SIZE);

                for (int i = 0; i < batch.getNumRows(); i++) {
                    assertThat(batch.getTimestamp(i, 0, TIMESTAMP_PRECISION))
                            .isEqualTo(expected.get(expectedIndex + i));
                }

                FileRecordIterator<InternalRow> fileIterator =
                        (FileRecordIterator<InternalRow>) iterator;
                for (int i = 0; i < batch.getNumRows(); i++) {
                    InternalRow row = iterator.next();
                    assertThat(row).isNotNull();
                    assertThat(row.getTimestamp(0, TIMESTAMP_PRECISION))
                            .isEqualTo(expected.get(expectedIndex));
                    assertThat(fileIterator.returnedPosition())
                            .isEqualTo((long) SELECTED_POSITIONS[expectedIndex]);
                    expectedIndex++;
                }

                assertThat(iterator.next()).isNull();
                iterator.releaseBatch();
            }

            assertThat(expectedIndex).isEqualTo(expected.size());
        }
    }

    private Path writeInt96File(List<Timestamp> timestamps) throws IOException {
        Path path = new Path(tempDir.toString(), UUID.randomUUID().toString());
        LocalFileIO fileIO = LocalFileIO.create();

        Options options = new Options();
        // A tiny row-group size forces multiple row groups; combined with one selected row per row
        // group this deterministically exercises INT96 skipValues before the selected row.
        options.setInteger("parquet.block.size", 1);
        options.setString("parquet.enable.dictionary", "false");

        ParquetWriterFactory writerFactory =
                new ParquetWriterFactory(new RowDataParquetBuilder(ROW_TYPE, options));
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(path, false), "uncompressed")) {
            for (Timestamp timestamp : timestamps) {
                writer.addElement(GenericRow.of(timestamp));
            }
        }

        return path;
    }

    private List<Timestamp> createTimestamps(int rowCount) {
        List<Timestamp> timestamps = new ArrayList<>(rowCount);
        long baseMillis = 1_704_067_200_000L;
        for (int i = 0; i < rowCount; i++) {
            timestamps.add(
                    Timestamp.fromEpochMillis(baseMillis + i * 17L, (i * 11_111) % 1_000_000));
        }
        return timestamps;
    }
}
