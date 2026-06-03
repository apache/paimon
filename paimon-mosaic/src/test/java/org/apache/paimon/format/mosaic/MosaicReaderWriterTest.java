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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Integration tests for Mosaic reader and writer. */
class MosaicReaderWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Mosaic native library not available");
    }

    @Test
    void testWriteAndRead() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, BinaryString.fromString("hello")),
                GenericRow.of(2, BinaryString.fromString("world")));

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).getString(1).toString()).isEqualTo("hello");
        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).getString(1).toString()).isEqualTo("world");
    }

    @Test
    void testNullValues() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, null),
                GenericRow.of(null, BinaryString.fromString("test")),
                GenericRow.of(null, null));

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(3);
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(1).isNullAt(0)).isTrue();
        assertThat(result.get(2).isNullAt(0)).isTrue();
        assertThat(result.get(2).isNullAt(1)).isTrue();
    }

    @Test
    void testColumnProjection() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .field("f_double", DataTypes.DOUBLE())
                        .build();
        RowType readType = RowType.builder().field("f_string", DataTypes.STRING()).build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("aaa"), 1.1),
                GenericRow.of(2, BinaryString.fromString("bbb"), 2.2));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getString(0).toString()).isEqualTo("aaa");
        assertThat(result.get(1).getString(0).toString()).isEqualTo("bbb");
    }

    @Test
    void testLargeDataset() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        int numRows = 10000;
        GenericRow[] rows = new GenericRow[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = GenericRow.of(i, BinaryString.fromString("row" + i));
        }
        writeRows(rowType, path, rows);

        List<InternalRow> result = readAll(rowType, rowType, path, null);
        assertThat(result).hasSize(numRows);
        assertThat(result.get(0).getInt(0)).isEqualTo(0);
        assertThat(result.get(numRows - 1).getInt(0)).isEqualTo(numRows - 1);
    }

    @Test
    void testRowGroupPredicateFiltering() throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();

        int numRows = 10000;
        GenericRow[] rows = new GenericRow[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = GenericRow.of(i, BinaryString.fromString("v" + i));
        }
        writeRows(rowType, path, "f_int", rows);

        // Predicate that cannot match any row group (all values are 0..9999)
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.greaterThan(0, 99999);
        List<InternalRow> result =
                readAll(rowType, rowType, path, Collections.singletonList(predicate));
        assertThat(result).isEmpty();

        // Predicate that matches the row group (values include range 0..9999)
        Predicate matchPredicate = builder.greaterThan(0, 5000);
        List<InternalRow> matchResult =
                readAll(rowType, rowType, path, Collections.singletonList(matchPredicate));
        assertThat(matchResult).hasSize(numRows);
    }

    @Test
    void testReturnedPosition() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();

        writeRows(
                rowType,
                path,
                GenericRow.of(1, BinaryString.fromString("a")),
                GenericRow.of(2, BinaryString.fromString("b")),
                GenericRow.of(3, BinaryString.fromString("c")));

        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        RecordReader.RecordIterator<InternalRow> batch = reader.readBatch();
        assertThat(batch).isNotNull();
        FileRecordIterator<InternalRow> fileIter = (FileRecordIterator<InternalRow>) batch;

        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(0);
        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(1);
        fileIter.next();
        assertThat(fileIter.returnedPosition()).isEqualTo(2);

        reader.close();
    }

    @Test
    void testProjectionWithMissingColumns() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        // Read type has a column that doesn't exist in the file (schema evolution)
        RowType readType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_new_col", DataTypes.BIGINT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("aaa")),
                GenericRow.of(2, BinaryString.fromString("bbb")));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(0).getString(2).toString()).isEqualTo("aaa");
        assertThat(result.get(1).getInt(0)).isEqualTo(2);
        assertThat(result.get(1).isNullAt(1)).isTrue();
        assertThat(result.get(1).getString(2).toString()).isEqualTo("bbb");
    }

    @Test
    void testProjectionAllColumnsMissing() throws IOException {
        RowType writeType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();
        // Read type has only columns that don't exist in the file
        RowType readType =
                RowType.builder()
                        .field("f_new_a", DataTypes.INT())
                        .field("f_new_b", DataTypes.STRING())
                        .build();
        Path path = newPath();

        writeRows(
                writeType,
                path,
                GenericRow.of(1, BinaryString.fromString("x")),
                GenericRow.of(2, BinaryString.fromString("y")));

        List<InternalRow> result = readAll(writeType, readType, path, null);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).isNullAt(0)).isTrue();
        assertThat(result.get(0).isNullAt(1)).isTrue();
        assertThat(result.get(1).isNullAt(0)).isTrue();
        assertThat(result.get(1).isNullAt(1)).isTrue();
    }

    @Test
    void testUnsupportedCompressionThrows() {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();

        assertThatThrownBy(() -> writerFactory.create(fileIO.newOutputStream(path, false), "lz4"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("lz4");
    }

    @Test
    void testReachTargetSize() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT(), DataTypes.STRING());
        Path path = newPath();
        MosaicFileFormat format = createFormat();
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);

        LocalFileIO fileIO = new LocalFileIO();
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");

        boolean reached = false;
        for (int i = 0; i < 100000; i++) {
            writer.addElement(GenericRow.of(i, BinaryString.fromString("value_" + i + "_padding")));
            if (writer.reachTargetSize(true, 1024)) {
                reached = true;
                break;
            }
        }
        writer.close();
        assertThat(reached).isTrue();
    }

    private Path newPath() {
        return new Path(tempDir.toUri().toString(), UUID.randomUUID() + ".mosaic");
    }

    private void writeRows(RowType rowType, Path path, GenericRow... rows) throws IOException {
        writeRows(rowType, path, "", rows);
    }

    private void writeRows(RowType rowType, Path path, String statsColumns, GenericRow... rows)
            throws IOException {
        MosaicFileFormat format = createFormat(statsColumns);
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        LocalFileIO fileIO = new LocalFileIO();
        FormatWriter writer = writerFactory.create(fileIO.newOutputStream(path, false), "zstd");
        for (GenericRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
    }

    private List<InternalRow> readAll(
            RowType dataType, RowType readType, Path path, List<Predicate> predicates)
            throws IOException {
        MosaicFileFormat format = createFormat();
        FormatReaderFactory readerFactory =
                format.createReaderFactory(dataType, readType, predicates);
        LocalFileIO fileIO = new LocalFileIO();
        RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)));

        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        List<InternalRow> result = new ArrayList<>();
        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        reader.close();
        return result;
    }

    private static MosaicFileFormat createFormat() {
        return createFormat("");
    }

    private static MosaicFileFormat createFormat(String statsColumns) {
        Options options = new Options();
        if (!statsColumns.isEmpty()) {
            options.set(MosaicFileFormat.STATS_COLUMNS, statsColumns);
        }
        return new MosaicFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));
    }

    private static boolean isNativeAvailable() {
        try {
            Class.forName("org.apache.paimon.mosaic.NativeLib");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
