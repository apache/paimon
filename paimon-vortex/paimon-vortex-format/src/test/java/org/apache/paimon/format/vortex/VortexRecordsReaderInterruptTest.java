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
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InterruptedIOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Interrupts a reader while it waits for its prefetched batch. The reader must keep the prefetch,
 * so that the next read returns its batch and close releases all native memory.
 */
class VortexRecordsReaderInterruptTest {

    private static final RowType ROW_TYPE =
            RowType.builder().field("id", DataTypes.INT()).field("str", DataTypes.STRING()).build();

    private static final int ROWS = 100_000;
    private static final int ATTEMPTS = 30;

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testInterruptedReadKeepsThePrefetch() throws Exception {
        VortexFileFormat format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        FileIO fileIO = LocalFileIO.create();
        Path file = new Path(new Path(tempDir.toUri()), "interrupt-" + System.nanoTime());

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < ROWS; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString("value-" + i)));
            }
        }

        // The interrupt can come before, during or just after the prefetch completes. Several
        // attempts with different delays make all of these orders probable.
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            FileRecordReader<InternalRow> reader =
                    format.createReaderFactory(ROW_TYPE, ROW_TYPE, null)
                            .createReader(
                                    new FormatReaderContext(
                                            fileIO, file, fileIO.getFileSize(file), null, null));

            long next = 0;
            try {
                if (attempt % 3 != 0) {
                    Thread.sleep(attempt % 5);
                }

                Thread.currentThread().interrupt();
                try {
                    next = readAll(reader.readBatch(), next);
                } catch (InterruptedIOException expected) {
                    // The read is interrupted. The next read must return the same batch.
                }

                Thread.interrupted();
                FileRecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    next = readAll(batch, next);
                }
            } finally {
                Thread.interrupted();

                // Fails when an Arrow batch is lost, because the allocator then leaks memory.
                reader.close();
            }

            assertThat(next).isEqualTo(ROWS);
        }
    }

    private static long readAll(FileRecordIterator<InternalRow> batch, long next) throws Exception {
        if (batch == null) {
            return next;
        }

        InternalRow row;
        while ((row = batch.next()) != null) {
            assertThat(row.getInt(0)).isEqualTo(next);
            next++;
        }
        batch.releaseBatch();
        return next;
    }
}
