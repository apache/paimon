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

package org.apache.paimon.format.vortex;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.VectorizedRecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for file positions and batch ownership of {@link VortexRecordsReader}. */
class VortexRecordsReaderPositionTest {

    private static final RowType ROW_TYPE =
            RowType.builder()
                    .field("id", DataTypes.INT())
                    .field("name", DataTypes.STRING())
                    .build();

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testPositionsFollowFileRowsWhenFilterDropsRows() throws Exception {
        Path file = write(10_000, 1024);
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);

        // Deletion vectors map rows by these positions, so they must be file positions even when
        // the native filter drops rows.
        List<long[]> rows =
                read(
                        file,
                        new VortexReaderFactory(
                                ROW_TYPE,
                                ROW_TYPE,
                                Collections.singletonList(builder.greaterOrEqual(0, 9_990)),
                                4),
                        null);

        assertThat(rows).hasSize(10);
        for (long[] row : rows) {
            assertThat(row[1]).isEqualTo(row[0]);
            assertThat(row[0]).isGreaterThanOrEqualTo(9_990);
        }
    }

    @Test
    void testPositionsWithSelectionAndFilter() throws Exception {
        Path file = write(5_000, 1024);
        PredicateBuilder builder = new PredicateBuilder(ROW_TYPE);
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 7, 2_000, 2_001, 4_999);

        List<long[]> rows =
                read(
                        file,
                        new VortexReaderFactory(
                                ROW_TYPE,
                                ROW_TYPE,
                                Collections.singletonList(builder.greaterThan(0, 1)),
                                4),
                        selection);

        assertThat(rows).extracting(row -> row[0]).containsExactly(7L, 2_000L, 2_001L, 4_999L);
        for (long[] row : rows) {
            assertThat(row[1]).isEqualTo(row[0]);
        }
    }

    @Test
    void testBatchesAreVectorizedAndOutliveTheNextBatch() throws Exception {
        Path file = write(50_000, 1024);
        FileIO fileIO = LocalFileIO.create();

        try (FileRecordReader<InternalRow> reader =
                new VortexReaderFactory(ROW_TYPE, ROW_TYPE, null, 4)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            List<FileRecordIterator<InternalRow>> batches = new ArrayList<>();
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                assertThat(batch).isInstanceOf(VectorizedRecordIterator.class);
                batches.add(batch);
            }

            // All batches are still held, so none may have been overwritten by a later one.
            long expected = 0;
            for (FileRecordIterator<InternalRow> held : batches) {
                InternalRow row;
                while ((row = held.next()) != null) {
                    assertThat(row.getInt(0)).isEqualTo((int) expected);
                    assertThat(row.getString(1).toString()).isEqualTo("name-" + expected);
                    assertThat(held.returnedPosition()).isEqualTo(expected);
                    expected++;
                }
                held.releaseBatch();
            }
            assertThat(expected).isEqualTo(50_000);
        }
    }

    @Test
    void testProjectionWithoutPhysicalColumnsCountsRows() throws Exception {
        Path file = write(3_000, 1024);
        RowType empty = RowType.builder().build();

        List<long[]> rows = read(file, new VortexReaderFactory(ROW_TYPE, empty, null, 4), null);

        assertThat(rows).hasSize(3_000);
        assertThat(rows.get(2_999)[1]).isEqualTo(2_999);
    }

    private Path write(int rowCount, int batchSize) throws Exception {
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(
                                new FileFormatFactory.FormatContext(
                                        new Options(), 1024, batchSize));
        Path file = new Path(new Path(tempDir.toUri()), "positions-" + System.nanoTime());
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(LocalFileIO.create(), file, "")) {
            for (int i = 0; i < rowCount; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("name-" + i)));
            }
        }
        return file;
    }

    /** Reads {id, position} pairs, or {-1, position} when no column is projected. */
    private static List<long[]> read(
            Path file, VortexReaderFactory factory, RoaringBitmap32 selection) throws Exception {
        FileIO fileIO = LocalFileIO.create();
        List<long[]> result = new ArrayList<>();
        try (FileRecordReader<InternalRow> reader =
                factory.createReader(
                        new FormatReaderContext(
                                fileIO, file, fileIO.getFileSize(file), selection, null))) {
            FileRecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow row;
                while ((row = batch.next()) != null) {
                    long id = row.getFieldCount() > 0 ? row.getInt(0) : -1;
                    result.add(new long[] {id, batch.returnedPosition()});
                }
                batch.releaseBatch();
            }
        }
        return result;
    }
}
