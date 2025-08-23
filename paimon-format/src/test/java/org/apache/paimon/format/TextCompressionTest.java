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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Base class for compression tests across different file formats. */
public abstract class TextCompressionTest {

    @TempDir protected java.nio.file.Path tempDir;

    protected final RowType rowType =
            RowType.of(
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.BOOLEAN());

    protected final List<InternalRow> testData =
            Arrays.asList(
                    GenericRow.of(1, BinaryString.fromString("Alice"), 100.5, true),
                    GenericRow.of(2, BinaryString.fromString("Bob"), 200.75, false),
                    GenericRow.of(3, BinaryString.fromString("Charlie"), 300.25, true),
                    GenericRow.of(4, BinaryString.fromString("Diana"), 400.0, false));

    /** Returns the file format for testing. */
    protected abstract FileFormat createFileFormat(Options options);

    /** Returns the file extension for the format. */
    protected abstract String getFormatExtension();

    @Disabled // TODO fix dependencies
    @ParameterizedTest(name = "compression = {0}")
    @EnumSource(HadoopCompressionType.class)
    void testCompression(HadoopCompressionType compression) throws IOException {
        testCompressionRoundTrip(
                compression.value(),
                String.format(
                        "test_compress.%s.%s", getFormatExtension(), compression.fileExtension()));
    }

    @Disabled // TODO fix dependencies
    @ParameterizedTest(name = "compression = {0}")
    @EnumSource(HadoopCompressionType.class)
    void testCompressionDetectionFromFileName(HadoopCompressionType type) throws IOException {
        testAutoCompressionDetection(
                "test_auto." + getFormatExtension() + "." + type.fileExtension(), type.value());
    }

    protected void testCompressionRoundTrip(String compression, String fileName)
            throws IOException {
        Options options = new Options();
        options.set(CoreOptions.FILE_COMPRESSION, compression);
        testCompressionRoundTripWithOptions(options, fileName);
    }

    protected void testCompressionRoundTripWithOptions(Options options, String fileName)
            throws IOException {
        FileFormat format = createFileFormat(options);

        // Validate the format and compression
        format.validateDataFields(rowType);

        Path filePath = new Path(tempDir.resolve(fileName).toString());
        FileIO fileIO = new LocalFileIO();

        // Write data with compression
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(
                        fileIO.newOutputStream(filePath, false),
                        options.get(CoreOptions.FILE_COMPRESSION))) {
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

    protected void testAutoCompressionDetection(String fileName, String compression)
            throws IOException {
        // Write file with compression
        Options writeOptions = new Options();
        writeOptions.set(CoreOptions.FILE_COMPRESSION, compression);

        FileFormat format = createFileFormat(writeOptions);
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
        readOptions.set(CoreOptions.FILE_COMPRESSION, "none"); // Default to none

        FileFormat readFormat = createFileFormat(readOptions);
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

    protected FormatContext createFormatContext(Options options) {
        return new FormatContext(options, 1024, 1024);
    }
}
