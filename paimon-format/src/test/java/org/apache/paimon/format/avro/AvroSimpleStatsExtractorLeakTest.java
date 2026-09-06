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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.avro.AvroRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link AvroSimpleStatsExtractor} closes its input stream exactly once. */
class AvroSimpleStatsExtractorLeakTest {

    @TempDir java.nio.file.Path tempDir;

    private final RowType rowType = RowType.of(DataTypes.INT());

    @Test
    void testNonAvroFile() throws IOException {
        Path file = write("this is not an avro file".getBytes(StandardCharsets.UTF_8));
        CountingFileIO fileIO = new CountingFileIO();

        assertThatThrownBy(() -> extractor().extract(fileIO, file, size(file)))
                .isInstanceOf(IOException.class);
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testEmptyFile() throws IOException {
        Path file = write(new byte[0]);
        CountingFileIO fileIO = new CountingFileIO();

        assertThatThrownBy(() -> extractor().extract(fileIO, file, 0))
                .isInstanceOf(IOException.class);
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testUnknownCodec() throws IOException {
        // Rename the codec inside a valid file, keeping the length so the header still
        // parses. CodecFactory.fromString then throws from the DataFileStream
        // constructor, and it throws AvroRuntimeException rather than an IOException.
        byte[] valid = Files.readAllBytes(Paths.get(writeAvro(1).toUri()));
        String header = new String(valid, StandardCharsets.ISO_8859_1);
        int at = header.indexOf("zstandard");
        assertThat(at).isGreaterThan(0);
        valid[at + 8] = 'X';
        Path file = write(valid);
        CountingFileIO fileIO = new CountingFileIO();

        assertThatThrownBy(() -> extractor().extract(fileIO, file, size(file)))
                .isInstanceOf(AvroRuntimeException.class)
                .hasMessageContaining("Unrecognized codec");
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testValidFileClosesOnce() throws IOException {
        Path file = writeAvro(3);
        CountingFileIO fileIO = new CountingFileIO();

        assertThat(
                        extractor()
                                .extractWithFileInfo(fileIO, file, size(file))
                                .getRight()
                                .getRowCount())
                .isEqualTo(3);
        assertThat(fileIO.closed).hasValue(1);
    }

    private AvroSimpleStatsExtractor extractor() {
        return new AvroSimpleStatsExtractor(
                rowType, new SimpleColStatsCollector.Factory[] {NoneSimpleColStatsCollector::new});
    }

    private long size(Path file) throws IOException {
        return LocalFileIO.create().getFileSize(file);
    }

    private Path write(byte[] content) throws IOException {
        Path file = new Path(tempDir.toUri().toString(), UUID.randomUUID().toString());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            out.write(content);
        }
        return file;
    }

    private Path writeAvro(int rows) throws IOException {
        Path file = new Path(tempDir.toUri().toString(), UUID.randomUUID().toString() + ".avro");
        FileFormat format = FileFormat.fromIdentifier("avro", new Options());
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            FormatWriter writer = format.createWriterFactory(rowType).create(out, "zstd");
            for (int i = 0; i < rows; i++) {
                writer.addElement(GenericRow.of(i));
            }
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
