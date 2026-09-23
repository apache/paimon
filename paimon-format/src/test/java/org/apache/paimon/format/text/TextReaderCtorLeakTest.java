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

package org.apache.paimon.format.text;

import org.apache.paimon.format.csv.CsvFileReader;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that a failing text-reader construction closes the stream it opened, exactly once. */
class TextReaderCtorLeakTest {

    @TempDir java.nio.file.Path tempDir;

    private final RowType rowType = RowType.of(DataTypes.STRING());

    @Test
    void testCorruptGzipClosesStream() throws IOException {
        // A .gz suffix makes the reader wrap the stream in a gzip codec, and
        // StandardLineReader reads in its constructor, so the header check fails there.
        Path file = write("a.csv.gz", "this is not gzip".getBytes(StandardCharsets.UTF_8));
        CountingFileIO fileIO = new CountingFileIO();

        assertThatThrownBy(() -> reader(fileIO, file, new Options(), 0L))
                .isInstanceOf(IOException.class);
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testCustomDelimiterSplitClosesStream() throws IOException {
        // Defensive path: SplitEnumerator refuses to split a file read with a custom line
        // delimiter, so only a direct reader call reaches this.
        Path file = write("a.csv", "a|b".getBytes(StandardCharsets.UTF_8));
        CountingFileIO fileIO = new CountingFileIO();
        Options options = new Options();
        options.set(CsvOptions.LINE_DELIMITER, "|");

        assertThatThrownBy(() -> reader(fileIO, file, options, 1L))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(fileIO.closed).hasValue(1);
    }

    @Test
    void testSuccessfulReadClosesStreamOnce() throws IOException {
        Path file = write("a.csv", "hello".getBytes(StandardCharsets.UTF_8));
        CountingFileIO fileIO = new CountingFileIO();

        try (CsvFileReader reader = reader(fileIO, file, new Options(), 0L)) {
            assertThat(reader.readBatch().next().getString(0).toString()).isEqualTo("hello");
        }
        assertThat(fileIO.closed).hasValue(1);
    }

    private CsvFileReader reader(CountingFileIO fileIO, Path file, Options options, long offset)
            throws IOException {
        return new CsvFileReader(
                fileIO, file, rowType, rowType, new CsvOptions(options), offset, null);
    }

    private Path write(String name, byte[] content) throws IOException {
        Path file = new Path(tempDir.toUri().toString(), UUID.randomUUID() + "_" + name);
        try (PositionOutputStream out = LocalFileIO.create().newOutputStream(file, false)) {
            out.write(content);
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
