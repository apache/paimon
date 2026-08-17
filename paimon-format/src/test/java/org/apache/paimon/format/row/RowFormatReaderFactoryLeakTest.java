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

package org.apache.paimon.format.row;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RowFormatReaderFactory#createReader} opens the input stream before parsing a footer and a
 * block index whose offsets and lengths come from the file itself. Ownership only passes to {@link
 * RowFormatReader} on the last line, so a corrupt file must not leave the stream open.
 *
 * <p>This matters because {@code DataFileRecordReader.createReader} treats an {@code IOException}
 * or {@code RuntimeException} from here as an ignorable corrupt file when {@code
 * ignore-corrupt-files} is set: it logs, returns null, and the scan moves to the next file. One
 * leaked descriptor per corrupt file, for as long as the scan runs.
 */
class RowFormatReaderFactoryLeakTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void aCorruptFooterDoesNotLeakTheInputStream() throws Exception {
        Path path = new Path(new Path(tempDir.toString()), "corrupt.row");
        CountingFileIO fileIO = new CountingFileIO();

        // A file long enough to be read as a footer, but with none of the expected content.
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write(new byte[128]);
        }

        RowType rowType = RowType.of(DataTypes.INT());
        FormatReaderFactory readerFactory =
                FileFormat.fromIdentifier("row", new Options())
                        .createReaderFactory(rowType, rowType, new ArrayList<>());

        assertThatThrownBy(
                        () ->
                                readerFactory.createReader(
                                        new FormatReaderContext(
                                                fileIO,
                                                path,
                                                fileIO.getFileSize(path),
                                                null,
                                                null)))
                .hasMessageContaining("Invalid row file magic");

        assertThat(fileIO.openInputStreams()).isZero();
    }

    @Test
    void anEmptyFileDoesNotLeakTheInputStream() throws Exception {
        Path path = new Path(new Path(tempDir.toString()), "empty.row");
        CountingFileIO fileIO = new CountingFileIO();
        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            // nothing: an aborted write leaves a zero-length object behind
        }

        RowType rowType = RowType.of(DataTypes.INT());
        FormatReaderFactory readerFactory =
                FileFormat.fromIdentifier("row", new Options())
                        .createReaderFactory(rowType, rowType, new ArrayList<>());

        assertThatThrownBy(
                () ->
                        readerFactory.createReader(
                                new FormatReaderContext(
                                        fileIO, path, fileIO.getFileSize(path), null, null)));

        assertThat(fileIO.openInputStreams()).isZero();
    }

    /** Counts input streams that have been opened and not yet closed. */
    private static class CountingFileIO extends LocalFileIO {

        private final AtomicInteger open = new AtomicInteger();

        int openInputStreams() {
            return open.get();
        }

        @Override
        public SeekableInputStream newInputStream(Path f) throws IOException {
            SeekableInputStream delegate = super.newInputStream(f);
            open.incrementAndGet();
            return new SeekableInputStream() {

                @Override
                public void seek(long desired) throws IOException {
                    delegate.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return delegate.getPos();
                }

                @Override
                public int read() throws IOException {
                    return delegate.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    return delegate.read(b, off, len);
                }

                @Override
                public void close() throws IOException {
                    open.decrementAndGet();
                    delegate.close();
                }
            };
        }
    }
}
