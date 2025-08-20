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

package org.apache.paimon.format.csv;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for CSV compression functionality. */
class CsvCompressionTest {

    @TempDir java.nio.file.Path tempDir;

    private final RowType rowType =
            RowType.of(
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.BOOLEAN());

    private final List<InternalRow> testData =
            Arrays.asList(
                    GenericRow.of(1, BinaryString.fromString("Alice"), 100.5, true),
                    GenericRow.of(2, BinaryString.fromString("Bob"), 200.75, false),
                    GenericRow.of(3, BinaryString.fromString("Charlie"), 300.25, true),
                    GenericRow.of(4, BinaryString.fromString("Diana"), 400.0, false));

    @Test
    void testGzipCompression() throws IOException {
        testCompressionRoundTrip("gzip", "test_gzip.csv.gz");
    }

    @Test
    void testDeflateCompression() throws IOException {
        testCompressionRoundTrip("deflate", "test_deflate.csv.deflate");
    }

    @Test
    void testNoCompression() throws IOException {
        testCompressionRoundTrip("none", "test_none.csv");
    }

    @Test
    void testCompressionDetectionFromFileName() throws IOException {
        // Test that compression is automatically detected from file extension during read
        // operations
        testAutoCompressionDetection("test_auto.csv.gz", "gzip");
        testAutoCompressionDetection("test_auto.csv.deflate", "deflate");
        testAutoCompressionDetection("test_auto.csv", "none");
    }

    @Test
    void testUnsupportedCompressionFormat() {
        Options options = new Options();
        options.set(CsvOptions.COMPRESSION, "unsupported");

        FormatContext context = createFormatContext(options);
        FileFormat format = new CsvFileFormat(context);

        assertThatThrownBy(() -> format.validateDataFields(rowType))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported compression format: unsupported");
    }

    @Test
    void testCompressionWithCustomOptions() throws IOException {
        Options options = new Options();
        options.set(CsvOptions.COMPRESSION, "gzip");
        options.set(CsvOptions.FIELD_DELIMITER, ";");
        options.set(CsvOptions.INCLUDE_HEADER, true);

        String fileName = "test_custom_options.csv.gz";
        testCompressionRoundTripWithOptions(options, fileName);
    }

    @Test
    void testBzip2Behavior() throws IOException {
        Options options = new Options();
        options.set(CsvOptions.COMPRESSION, "bzip2");

        FormatContext context = createFormatContext(options);
        FileFormat format = new CsvFileFormat(context);

        // Test validation - this should work or throw appropriate exception
        try {
            format.validateDataFields(rowType);
            // If validation passes, bzip2 codec is available
            testCompressionRoundTrip("bzip2", "test_bzip2.csv.bz2");
        } catch (IllegalArgumentException e) {
            // If validation fails, check that it's due to unsupported compression
            assertThat(e.getMessage()).contains("Unsupported compression format: bzip2");
        }
    }

    private void testCompressionRoundTrip(String compression, String fileName) throws IOException {
        Options options = new Options();
        options.set(CsvOptions.COMPRESSION, compression);
        testCompressionRoundTripWithOptions(options, fileName);
    }

    private void testCompressionRoundTripWithOptions(Options options, String fileName)
            throws IOException {
        FormatContext context = createFormatContext(options);
        FileFormat format = new CsvFileFormat(context);

        // Validate the format and compression
        format.validateDataFields(rowType);

        Path filePath = new Path(tempDir.resolve(fileName).toString());
        FileIO fileIO = new LocalFileIO();

        // Write data with compression
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(
                        fileIO.newOutputStream(filePath, false),
                        options.get(CsvOptions.COMPRESSION))) {
            for (InternalRow row : testData) {
                writer.addElement(row);
            }
        }

        // Read data back
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, null);
        List<InternalRow> readData = new ArrayList<>();

        try (RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, filePath, fileIO.getFileSize(filePath)))) {
            reader.forEachRemaining(readData::add);
        }

        // Verify data integrity
        assertThat(readData).hasSize(testData.size());
        for (int i = 0; i < testData.size(); i++) {
            InternalRow expected = testData.get(i);
            InternalRow actual = readData.get(i);

            assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
            assertThat(actual.getString(1).toString()).isEqualTo(expected.getString(1).toString());
            assertThat(actual.getDouble(2)).isEqualTo(expected.getDouble(2));
            assertThat(actual.getBoolean(3)).isEqualTo(expected.getBoolean(3));
        }
    }

    private void testAutoCompressionDetection(String fileName, String compression)
            throws IOException {
        // Write file with compression
        Options writeOptions = new Options();
        writeOptions.set(CsvOptions.COMPRESSION, compression);

        FormatContext context = createFormatContext(writeOptions);
        FileFormat format = new CsvFileFormat(context);
        Path filePath = new Path(tempDir.resolve(fileName).toString());
        FileIO fileIO = new LocalFileIO();

        // Write compressed data
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(filePath, false), compression)) {
            writer.addElement(testData.get(0)); // Write just one row for this test
        }

        // Read back with auto-detection (no compression specified in read options)
        Options readOptions = new Options();
        readOptions.set(CsvOptions.COMPRESSION, "none"); // Default to none

        FormatContext readContext = createFormatContext(readOptions);
        FileFormat readFormat = new CsvFileFormat(readContext);
        FormatReaderFactory readerFactory = readFormat.createReaderFactory(rowType, null);

        List<InternalRow> readData = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderContext(fileIO, filePath, fileIO.getFileSize(filePath)))) {
            reader.forEachRemaining(readData::add);
        }

        // Should successfully read the data regardless of compression
        assertThat(readData).hasSize(1);
        InternalRow expected = testData.get(0);
        InternalRow actual = readData.get(0);
        assertThat(actual.getInt(0)).isEqualTo(expected.getInt(0));
        assertThat(actual.getString(1).toString()).isEqualTo(expected.getString(1).toString());
    }

    private FormatContext createFormatContext(Options options) {
        return new FormatContext(options, 1024, 1024);
    }
}
