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

package org.apache.paimon.format.json;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link JsonFileFormat}. */
public class JsonFileFormatTest extends FormatReadWriteTest {

    protected JsonFileFormatTest() {
        super("json");
    }

    @Override
    protected FileFormat fileFormat() {
        return new JsonFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Test
    public void testDifferentLineDelimiters() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        // Test with default line delimiter (JSON format uses \n by default)
        Options options = new Options();
        options.set(JsonOptions.LINE_DELIMITER, "\n");

        FileFormat format =
                new JsonFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Path testFile = new Path(parent, "test_delimiter_" + UUID.randomUUID() + ".json");

        // Write test data
        FormatWriterFactory writerFactory = format.createWriterFactory(rowType);
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false);
                FormatWriter writer = writerFactory.create(out, "none")) {
            writer.addElement(GenericRow.of(1, BinaryString.fromString("Alice")));
            writer.addElement(GenericRow.of(2, BinaryString.fromString("Bob")));
            writer.addElement(GenericRow.of(3, BinaryString.fromString("Charlie")));
        }

        // Read the raw file content to verify delimiter is used in writing
        byte[] fileContent =
                java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(testFile.toUri()));
        String content = new String(fileContent);

        // Verify that the newline delimiter is used in the written file
        assertThat(content).contains("\n");

        // For reading, JSON format should be able to read the data correctly
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile)))) {

            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));

            // Should be able to read all 3 rows correctly
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Bob");
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");
        }
    }

    @Test
    public void testIgnoreParseErrorsEnabled() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        Options options = new Options();
        options.set(JsonOptions.JSON_IGNORE_PARSE_ERRORS, true);

        FileFormat format =
                new JsonFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Path testFile = new Path(parent, "test_ignore_errors_" + UUID.randomUUID() + ".json");

        // Write test data with some malformed JSON lines
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false)) {
            String validJson1 = "{\"f0\":1,\"f1\":\"Alice\"}\n";
            String invalidJson =
                    "{\"f0\":invalid,\"f1\":\"Bob\"\n"; // Missing closing brace and invalid value
            String validJson2 = "{\"f0\":3,\"f1\":\"Charlie\"}\n";
            String anotherInvalidJson = "not a json at all\n";
            String validJson3 = "{\"f0\":4,\"f1\":\"David\"}\n";

            out.write(validJson1.getBytes());
            out.write(invalidJson.getBytes());
            out.write(validJson2.getBytes());
            out.write(anotherInvalidJson.getBytes());
            out.write(validJson3.getBytes());
        }

        // Read data - should skip malformed lines and return only valid ones
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile)))) {

            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(
                    row -> {
                        if (row != null) { // ignoreParseErrors returns null for malformed lines
                            result.add(serializer.copy(row));
                        }
                    });

            // Should only have 3 valid rows (Alice, Charlie, David)
            assertThat(result).hasSize(3);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(1).getInt(0)).isEqualTo(3);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Charlie");
            assertThat(result.get(2).getInt(0)).isEqualTo(4);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("David");
        }
    }

    @Test
    public void testIgnoreParseErrorsDisabled() throws IOException {
        RowType rowType = DataTypes.ROW(DataTypes.INT().notNull(), DataTypes.STRING());

        Options options = new Options();
        options.set(JsonOptions.JSON_IGNORE_PARSE_ERRORS, false); // Explicitly disable

        FileFormat format =
                new JsonFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Path testFile = new Path(parent, "test_no_ignore_errors_" + UUID.randomUUID() + ".json");

        // Write test data with some malformed JSON lines
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false)) {
            String validJson1 = "{\"f0\":1,\"f1\":\"Alice\"}\n";
            String invalidJson =
                    "{\"f0\":invalid,\"f1\":\"Bob\"\n"; // Missing closing brace and invalid value

            out.write(validJson1.getBytes());
            out.write(invalidJson.getBytes());
        }

        // Read data - should throw exception on malformed JSON
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile)))) {

            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            List<InternalRow> result = new ArrayList<>();

            // Should throw IOException when encountering malformed JSON
            assertThrows(
                    IOException.class,
                    () -> {
                        reader.forEachRemaining(row -> result.add(serializer.copy(row)));
                    });

            // Should have read the first valid row before encountering the error
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
        }
    }

    @Test
    public void testIgnoreParseErrorsWithComplexTypes() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.INT().notNull(),
                        DataTypes.STRING(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()));

        Options options = new Options();
        options.set(JsonOptions.JSON_IGNORE_PARSE_ERRORS, true);

        FileFormat format =
                new JsonFileFormat(new FileFormatFactory.FormatContext(options, 1024, 1024));

        Path testFile =
                new Path(parent, "test_complex_ignore_errors_" + UUID.randomUUID() + ".json");

        // Write test data with some malformed JSON lines
        try (PositionOutputStream out = fileIO.newOutputStream(testFile, false)) {
            String validJson1 =
                    "{\"f0\":1,\"f1\":\"Alice\",\"f2\":[\"a\",\"b\"],\"f3\":{\"key1\":1,\"key2\":2}}\n";
            String invalidArrayJson =
                    "{\"f0\":2,\"f1\":\"Bob\",\"f2\":\"not_an_array\",\"f3\":{\"key1\":1}}\n"; // Invalid array
            String validJson2 =
                    "{\"f0\":3,\"f1\":\"Charlie\",\"f2\":[\"c\",\"d\"],\"f3\":{\"key3\":3}}\n";
            String invalidMapJson =
                    "{\"f0\":4,\"f1\":\"David\",\"f2\":[\"e\"],\"f3\":\"not_a_map\"}\n"; // Invalid
            // map

            out.write(validJson1.getBytes());
            out.write(invalidArrayJson.getBytes());
            out.write(validJson2.getBytes());
            out.write(invalidMapJson.getBytes());
        }

        // Read data - should handle type conversion errors gracefully
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType)
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile)))) {

            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            List<InternalRow> result = new ArrayList<>();
            reader.forEachRemaining(
                    row -> {
                        if (row != null) {
                            result.add(serializer.copy(row));
                        }
                    });

            // Should have valid rows, with null values for failed conversions
            assertThat(result).hasSize(4);

            // First row should be completely valid
            assertThat(result.get(0).getInt(0)).isEqualTo(1);
            assertThat(result.get(0).getString(1).toString()).isEqualTo("Alice");
            assertThat(result.get(0).getArray(2)).isNotNull();
            assertThat(result.get(0).getMap(3)).isNotNull();

            // Second row should have null array due to type mismatch
            assertThat(result.get(1).getInt(0)).isEqualTo(2);
            assertThat(result.get(1).getString(1).toString()).isEqualTo("Bob");
            assertThat(result.get(1).isNullAt(2))
                    .isTrue(); // Array should be null due to conversion error

            // Third row should be completely valid
            assertThat(result.get(2).getInt(0)).isEqualTo(3);
            assertThat(result.get(2).getString(1).toString()).isEqualTo("Charlie");

            // Fourth row should have null map due to type mismatch
            assertThat(result.get(3).getInt(0)).isEqualTo(4);
            assertThat(result.get(3).getString(1).toString()).isEqualTo("David");
            assertThat(result.get(3).isNullAt(3))
                    .isTrue(); // Map should be null due to conversion error
        }
    }

    @Override
    public boolean supportDataFileWithoutExtension() {
        return true;
    }
}
