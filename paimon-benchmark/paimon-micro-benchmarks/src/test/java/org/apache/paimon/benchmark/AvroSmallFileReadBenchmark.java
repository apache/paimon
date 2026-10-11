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

package org.apache.paimon.benchmark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.avro.AvroBulkFormat;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

/** Benchmark for opening and reading small Avro files through {@link AvroBulkFormat}. */
public class AvroSmallFileReadBenchmark {

    @TempDir java.nio.file.Path tempDir;

    /**
     * Run with the following command.
     *
     * <pre>
     * mvn -pl paimon-benchmark/paimon-micro-benchmarks -am -Pfast-build \
     *   -DfailIfNoTests=false -Dtest=AvroSmallFileReadBenchmark package
     * </pre>
     */
    @Test
    public void benchmarkSmallFileRead() throws Exception {
        int readsPerIteration = 1_000;
        int iterations = 5;
        int fieldCount = 50;
        int rowCount = 3;

        RowType rowType = rowType(fieldCount);
        LocalFileIO fileIO = LocalFileIO.create();
        Path file = new Path(tempDir.toUri().toString(), "small.avro");
        writeFile(fileIO, file, rowType, fieldCount, rowCount);
        long fileSize = fileIO.getFileSize(file);

        System.out.printf(
                "Avro small-file input: size=%d bytes, fields=%d, rows=%d, reads/iteration=%d%n",
                fileSize, fieldCount, rowCount, readsPerIteration);

        FormatReaderFactory readerFactory = new AvroBulkFormat(rowType);
        FormatReaderContext context = new FormatReaderContext(fileIO, file, fileSize, null, null);
        Benchmark benchmark =
                new Benchmark("avro-small-file-read", (long) readsPerIteration * rowCount)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(true);
        benchmark.addCase(
                "read",
                iterations,
                () -> readFiles(readerFactory, context, readsPerIteration, rowCount));
        benchmark.run();
    }

    private static RowType rowType(int fieldCount) {
        RowType.Builder builder = RowType.builder().field("id", DataTypes.INT().notNull());
        for (int i = 1; i < fieldCount; i++) {
            builder.field(String.format("profile_attribute_%03d", i), DataTypes.STRING().notNull());
        }
        return builder.build();
    }

    private static void writeFile(
            LocalFileIO fileIO, Path file, RowType rowType, int fieldCount, int rowCount)
            throws IOException {
        AvroFileFormat fileFormat =
                new AvroFileFormat(new FormatContext(new Options(), 1024, 1024));
        BinaryString empty = BinaryString.fromString("");
        try (PositionOutputStream out = fileIO.newOutputStream(file, false);
                FormatWriter writer = fileFormat.createWriterFactory(rowType).create(out, "null")) {
            for (int rowId = 0; rowId < rowCount; rowId++) {
                GenericRow row = new GenericRow(fieldCount);
                row.setField(0, rowId);
                for (int field = 1; field < fieldCount; field++) {
                    row.setField(field, empty);
                }
                writer.addElement(row);
            }
        }
    }

    private static void readFiles(
            FormatReaderFactory readerFactory,
            FormatReaderContext context,
            int readsPerIteration,
            int expectedRowsPerFile) {
        long rowCount = 0;
        long checksum = 0;
        try {
            for (int i = 0; i < readsPerIteration; i++) {
                try (FileRecordReader<InternalRow> reader = readerFactory.createReader(context)) {
                    RecordIterator<InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        try {
                            InternalRow row;
                            while ((row = batch.next()) != null) {
                                checksum += row.getInt(0);
                                rowCount++;
                            }
                        } finally {
                            batch.releaseBatch();
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        long expectedRows = (long) readsPerIteration * expectedRowsPerFile;
        long expectedChecksum =
                (long) readsPerIteration * expectedRowsPerFile * (expectedRowsPerFile - 1) / 2;
        if (rowCount != expectedRows || checksum != expectedChecksum) {
            throw new AssertionError(
                    String.format(
                            "Expected %d rows with checksum %d, but got %d rows with checksum %d.",
                            expectedRows, expectedChecksum, rowCount, checksum));
        }
    }
}
