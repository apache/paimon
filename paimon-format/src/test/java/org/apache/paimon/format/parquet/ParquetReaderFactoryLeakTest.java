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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link ParquetReaderFactory#createReader} closes the file exactly once. */
class ParquetReaderFactoryLeakTest {

    @TempDir java.nio.file.Path tempDir;

    private final RowType rowType = RowType.of(DataTypes.INT());

    @Test
    void testSetupFailureAfterReaderExistsClosesFile() throws IOException {
        Path file = writeParquet();
        CountingFileIO fileIO = new CountingFileIO();

        // computeBatchSize is called after the ParquetFileReader has been constructed, so a
        // zero batch size fails the check inside the region this test is about.
        ParquetReaderFactory factory =
                new ParquetReaderFactory(new Options(), rowType, 1024, null) {
                    @Override
                    protected int computeBatchSize(
                            ParquetFileReader reader, MessageType requestedSchema) {
                        return 0;
                    }
                };

        assertThatThrownBy(() -> factory.createReader(context(fileIO, file)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Parquet read batch size should be positive");
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testSuccessfulReadClosesFileOnce() throws IOException {
        Path file = writeParquet();
        CountingFileIO fileIO = new CountingFileIO();

        ParquetReaderFactory factory = new ParquetReaderFactory(new Options(), rowType, 1024, null);
        try (FileRecordReader<InternalRow> reader = factory.createReader(context(fileIO, file))) {
            assertThat(reader.readBatch().next().getInt(0)).isEqualTo(1);
        }
        assertThat(fileIO.closed).hasValue(1);
    }

    private FormatReaderContext context(CountingFileIO fileIO, Path file) throws IOException {
        return new FormatReaderContext(fileIO, file, fileIO.getFileSize(file), null, null);
    }

    private Path writeParquet() throws IOException {
        Path file = new Path(tempDir.toUri().toString(), "a.parquet");
        FileFormat format = FileFormat.fromIdentifier("parquet", new Options());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            writer.addElement(GenericRow.of(1));
            writer.close();
        }
        return file;
    }

    private static class CountingFileIO extends LocalFileIO {

        private final AtomicInteger closed = new AtomicInteger();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream inner = super.newInputStream(path);
            return new SeekableInputStream() {

                @Override
                public void seek(long desired) throws IOException {
                    inner.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return inner.getPos();
                }

                @Override
                public int read() throws IOException {
                    return inner.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return inner.read(b, off, len);
                }

                @Override
                public void close() throws IOException {
                    closed.incrementAndGet();
                    inner.close();
                }
            };
        }
    }
}
