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

package org.apache.paimon.format.lance;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Test read write for lance file format. */
public class LanceReaderWriterTest {

    @Test
    public void testWriteAndRead(@TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        Options options = new Options();
        LanceFileFormat format =
                new LanceFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(tempDir.resolve("test_data_" + UUID.randomUUID()).toString());

        // Write data
        List<InternalRow> expectedRows = new ArrayList<>();
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            expectedRows.add(GenericRow.of(1, BinaryString.fromString("hello")));
            writer.addElement(expectedRows.get(0));
            expectedRows.add(GenericRow.of(2, BinaryString.fromString("world")));
            writer.addElement(expectedRows.get(1));
        }

        InternalRowSerializer internalRowSerializer = new InternalRowSerializer(rowType);
        // Read data and check
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile), null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            assertNotNull(reader);

            List<InternalRow> actualRows = new ArrayList<>();
            while (iterator.hasNext()) {
                actualRows.add(internalRowSerializer.copy(iterator.next()));
            }

            assertEquals(expectedRows.size(), actualRows.size());
            for (int i = 0; i < expectedRows.size(); i++) {
                assertEquals(expectedRows.get(i).getInt(0), actualRows.get(i).getInt(0));
                assertEquals(expectedRows.get(i).getString(1), actualRows.get(i).getString(1));
            }
        }
    }

    @Test
    public void testWriteAndReadVector(@TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.VECTOR(3, DataTypes.FLOAT()));
        Options options = new Options();
        LanceFileFormat format =
                new LanceFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(tempDir.resolve("test_vector_" + UUID.randomUUID()).toString());

        float[] values1 = new float[] {1.0f, 2.0f, 3.0f};
        float[] values2 = new float[] {4.0f, 5.0f, 6.0f};

        // Write data
        List<InternalRow> expectedRows = new ArrayList<>();
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            expectedRows.add(GenericRow.of(1, BinaryVector.fromPrimitiveArray(values1)));
            writer.addElement(expectedRows.get(0));
            expectedRows.add(GenericRow.of(2, BinaryVector.fromPrimitiveArray(values2)));
            writer.addElement(expectedRows.get(1));
        }

        InternalRowSerializer internalRowSerializer = new InternalRowSerializer(rowType);
        // Read data and check
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile), null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {
            assertNotNull(reader);

            List<InternalRow> actualRows = new ArrayList<>();
            while (iterator.hasNext()) {
                actualRows.add(internalRowSerializer.copy(iterator.next()));
            }

            assertEquals(expectedRows.size(), actualRows.size());
            assertEquals(expectedRows.get(0).getInt(0), actualRows.get(0).getInt(0));
            assertArrayEquals(values1, actualRows.get(0).getVector(1).toFloatArray(), 0.0f);
            assertEquals(expectedRows.get(1).getInt(0), actualRows.get(1).getInt(0));
            assertArrayEquals(values2, actualRows.get(1).getVector(1).toFloatArray(), 0.0f);
        }
    }
}
