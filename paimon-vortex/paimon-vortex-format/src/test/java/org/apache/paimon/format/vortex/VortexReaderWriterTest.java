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
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Test read write for Vortex file format. */
public class VortexReaderWriterTest {

    @BeforeAll
    static void checkNativeLibrary() {
        assumeTrue(isNativeAvailable(), "Vortex native library not available, skipping tests");
    }

    private static boolean isNativeAvailable() {
        try {
            dev.vortex.jni.NativeLoader.loadJni();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Test
    public void testWriteAndRead(@TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        Options options = new Options();
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(new Path(tempDir.toUri()), "test_data_" + UUID.randomUUID());

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
    public void testWriteAndReadMultipleTypes(@TempDir java.nio.file.Path tempDir)
            throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_bigint", DataTypes.BIGINT())
                        .field("f_double", DataTypes.DOUBLE())
                        .field("f_boolean", DataTypes.BOOLEAN())
                        .field("f_string", DataTypes.STRING())
                        .build();

        Options options = new Options();
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(new Path(tempDir.toUri()), "test_multi_" + UUID.randomUUID());

        // Write data
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            writer.addElement(GenericRow.of(1, 100L, 1.5D, true, BinaryString.fromString("row1")));
            writer.addElement(GenericRow.of(2, 200L, 2.5D, false, BinaryString.fromString("row2")));
            writer.addElement(GenericRow.of(3, 300L, 3.5D, true, BinaryString.fromString("row3")));
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);
        // Read data and check
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile), null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {

            List<InternalRow> actualRows = new ArrayList<>();
            while (iterator.hasNext()) {
                actualRows.add(serializer.copy(iterator.next()));
            }

            assertEquals(3, actualRows.size());
            assertEquals(1, actualRows.get(0).getInt(0));
            assertEquals(100L, actualRows.get(0).getLong(1));
            assertEquals(1.5D, actualRows.get(0).getDouble(2));
            assertEquals(true, actualRows.get(0).getBoolean(3));
            assertEquals(BinaryString.fromString("row1"), actualRows.get(0).getString(4));

            assertEquals(3, actualRows.get(2).getInt(0));
            assertEquals(300L, actualRows.get(2).getLong(1));
        }
    }

    @Test
    public void testReadWithSelection(@TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
        Options options = new Options();
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(new Path(tempDir.toUri()), "test_selection_" + UUID.randomUUID());

        // Write 5 rows
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            for (int i = 0; i < 5; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("row" + i)));
            }
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);

        // Read with selection: only rows 1 and 3
        RoaringBitmap32 selection = RoaringBitmap32.bitmapOf(1, 3);
        FormatReaderFactory readerFactory = format.createReaderFactory(rowType, rowType, null);
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO,
                                        testFile,
                                        fileIO.getFileSize(testFile),
                                        selection));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {

            List<InternalRow> actualRows = new ArrayList<>();
            while (iterator.hasNext()) {
                actualRows.add(serializer.copy(iterator.next()));
            }

            assertEquals(2, actualRows.size());
            assertEquals(1, actualRows.get(0).getInt(0));
            assertEquals(BinaryString.fromString("row1"), actualRows.get(0).getString(1));
            assertEquals(3, actualRows.get(1).getInt(0));
            assertEquals(BinaryString.fromString("row3"), actualRows.get(1).getString(1));
        }
    }

    @Test
    public void testReadWithPredicate(@TempDir java.nio.file.Path tempDir) throws Exception {
        RowType rowType =
                RowType.builder()
                        .field("f_int", DataTypes.INT())
                        .field("f_string", DataTypes.STRING())
                        .build();

        Options options = new Options();
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(options, 1024, 1024));

        FileIO fileIO = new LocalFileIO();
        Path testFile = new Path(new Path(tempDir.toUri()), "test_predicate_" + UUID.randomUUID());

        // Write 5 rows: (0, "row0"), (1, "row1"), ..., (4, "row4")
        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(rowType))
                        .create(fileIO, testFile, "")) {
            for (int i = 0; i < 5; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("row" + i)));
            }
        }

        InternalRowSerializer serializer = new InternalRowSerializer(rowType);

        // Read with predicate: f_int > 2 (should return rows 3 and 4)
        PredicateBuilder builder = new PredicateBuilder(rowType);
        Predicate predicate = builder.greaterThan(0, 2);
        FormatReaderFactory readerFactory =
                format.createReaderFactory(rowType, rowType, Collections.singletonList(predicate));
        try (RecordReader<InternalRow> reader =
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, testFile, fileIO.getFileSize(testFile), null));
                RecordReaderIterator<InternalRow> iterator = new RecordReaderIterator<>(reader)) {

            List<InternalRow> actualRows = new ArrayList<>();
            while (iterator.hasNext()) {
                actualRows.add(serializer.copy(iterator.next()));
            }

            assertEquals(2, actualRows.size());
            assertEquals(3, actualRows.get(0).getInt(0));
            assertEquals(BinaryString.fromString("row3"), actualRows.get(0).getString(1));
            assertEquals(4, actualRows.get(1).getInt(0));
            assertEquals(BinaryString.fromString("row4"), actualRows.get(1).getString(1));
        }
    }
}
