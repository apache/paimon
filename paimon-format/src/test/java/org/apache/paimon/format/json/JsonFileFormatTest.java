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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JsonFileFormat}. */
public class JsonFileFormatTest {

    @Test
    public void testWriteAndRead(@TempDir java.nio.file.Path tempDir) throws IOException {
        RowType rowType =
                RowType.of(DataTypes.INT().notNull(), DataTypes.STRING(), DataTypes.BOOLEAN());

        // Create test data
        List<InternalRow> testData = new ArrayList<>();
        testData.add(GenericRow.of(1, BinaryString.fromString("Alice"), true));
        testData.add(GenericRow.of(2, BinaryString.fromString("Bob"), false));
        testData.add(GenericRow.of(3, BinaryString.fromString("Charlie"), true));

        // Create file format
        FileFormat jsonFormat =
                new JsonFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        // Write data
        Path filePath = new Path(tempDir.toUri().toString(), "test.json");
        FileIO fileIO = LocalFileIO.create();

        FormatWriterFactory writerFactory = jsonFormat.createWriterFactory(rowType);
        try (FormatWriter writer =
                writerFactory.create(fileIO.newOutputStream(filePath, false), "none")) {
            for (InternalRow row : testData) {
                writer.addElement(row);
            }
        }

        // Read data
        FormatReaderFactory readerFactory = jsonFormat.createReaderFactory(rowType, null);
        FileRecordReader<InternalRow> reader =
                readerFactory.createReader(
                        new FormatReaderFactory.Context() {
                            @Override
                            public FileIO fileIO() {
                                return fileIO;
                            }

                            @Override
                            public Path filePath() {
                                return filePath;
                            }

                            @Override
                            public long fileSize() {
                                try {
                                    return fileIO.getFileSize(filePath);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }

                            @Override
                            public org.apache.paimon.utils.RoaringBitmap32 selection() {
                                return null;
                            }
                        });

        List<InternalRow> readData = new ArrayList<>();
        RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();
        while (iterator != null) {
            InternalRow row;
            while ((row = iterator.next()) != null) {
                readData.add(GenericRow.of(row.getInt(0), row.getString(1), row.getBoolean(2)));
            }
            iterator.releaseBatch();
            iterator = reader.readBatch();
        }
        reader.close();

        // Verify data
        assertThat(readData).hasSize(3);
        assertThat(readData.get(0).getInt(0)).isEqualTo(1);
        assertThat(readData.get(0).getString(1).toString()).isEqualTo("Alice");
        assertThat(readData.get(0).getBoolean(2)).isTrue();

        assertThat(readData.get(1).getInt(0)).isEqualTo(2);
        assertThat(readData.get(1).getString(1).toString()).isEqualTo("Bob");
        assertThat(readData.get(1).getBoolean(2)).isFalse();

        assertThat(readData.get(2).getInt(0)).isEqualTo(3);
        assertThat(readData.get(2).getString(1).toString()).isEqualTo("Charlie");
        assertThat(readData.get(2).getBoolean(2)).isTrue();
    }

    @Test
    public void testDataTypeValidation() {
        JsonFileFormat format =
                new JsonFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));

        // Test that all supported types are validated correctly
        RowType supportedRowType =
                RowType.of(
                        DataTypes.BOOLEAN(),
                        DataTypes.TINYINT(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.STRING(),
                        DataTypes.DATE(),
                        DataTypes.TIMESTAMP(),
                        DataTypes.ARRAY(DataTypes.STRING()),
                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                        DataTypes.ROW(new DataField(0, "nested", DataTypes.STRING())));

        // Should not throw exception
        format.validateDataFields(supportedRowType);
    }
}
