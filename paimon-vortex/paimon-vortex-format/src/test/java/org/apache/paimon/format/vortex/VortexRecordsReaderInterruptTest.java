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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Interrupts a reader while its prefetch is blocked in a file read. The reader must keep the
 * prefetch, so that the next read returns its batch, and close must wait for it before it releases
 * the native resources.
 */
class VortexRecordsReaderInterruptTest {

    private static final RowType ROW_TYPE =
            RowType.builder().field("id", DataTypes.INT()).field("str", DataTypes.STRING()).build();

    private static final int ROWS = 300_000;

    @TempDir java.nio.file.Path tempDir;

    private VortexFileFormat format;
    private Path file;
    private GatedFileIO fileIO;

    @BeforeEach
    void before() throws Exception {
        format =
                new VortexFileFormatFactory()
                        .create(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
        // A scheme without a native storage client, so that all reads go through the FileIO.
        file = new Path("gated://" + tempDir.toAbsolutePath() + "/interrupt.vortex");
        fileIO = new GatedFileIO();

        try (FormatWriter writer =
                ((SupportsDirectWrite) format.createWriterFactory(ROW_TYPE))
                        .create(fileIO, file, "")) {
            for (int i = 0; i < ROWS; i++) {
                writer.addElement(GenericRow.of(i, BinaryString.fromString(value(i))));
            }
        }

        // The reader reads the footer at the end of the file when it opens, and the data at the
        // start of the file in the prefetch. Only reads of the data are blocked.
        fileIO.blockBelow = fileIO.getFileSize(file) / 2;
    }

    @Test
    void testInterruptedReadKeepsThePrefetch() throws Exception {
        FileRecordReader<InternalRow> reader = open();
        try {
            assertThat(fileIO.blocked.await(30, TimeUnit.SECONDS)).isTrue();

            Thread.currentThread().interrupt();
            assertThatThrownBy(reader::readBatch).isInstanceOf(InterruptedIOException.class);
            assertThat(Thread.interrupted()).isTrue();

            // The prefetch completes after the interrupted wait. Its batch must not be lost.
            fileIO.gate.countDown();
            assertThat(readAll(reader)).isEqualTo(ROWS);
        } finally {
            fileIO.gate.countDown();

            // Fails when an Arrow batch is lost, because the allocator then leaks memory.
            reader.close();
        }
    }

    @Test
    void testCloseWaitsForTheInterruptedPrefetch() throws Exception {
        FileRecordReader<InternalRow> reader = open();
        assertThat(fileIO.blocked.await(30, TimeUnit.SECONDS)).isTrue();

        Thread.currentThread().interrupt();
        assertThatThrownBy(reader::readBatch).isInstanceOf(InterruptedIOException.class);
        assertThat(Thread.interrupted()).isTrue();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptKept = new AtomicBoolean();
        Thread closer =
                new Thread(
                        () -> {
                            // An interrupted close must also wait for the prefetch.
                            Thread.currentThread().interrupt();
                            try {
                                reader.close();
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                            interruptKept.set(Thread.currentThread().isInterrupted());
                        });
        closer.start();

        closer.join(500);
        assertThat(closer.isAlive()).isTrue();
        assertThat(fileIO.closedStreams.get()).isZero();

        fileIO.gate.countDown();
        closer.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(closer.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(interruptKept.get()).isTrue();
        assertThat(fileIO.closedStreams.get()).isPositive();
    }

    private FileRecordReader<InternalRow> open() throws IOException {
        return format.createReaderFactory(ROW_TYPE, ROW_TYPE, null)
                .createReader(
                        new FormatReaderContext(
                                fileIO, file, fileIO.getFileSize(file), null, null));
    }

    private static long readAll(FileRecordReader<InternalRow> reader) throws IOException {
        long next = 0;
        FileRecordIterator<InternalRow> batch;
        while ((batch = reader.readBatch()) != null) {
            InternalRow row;
            while ((row = batch.next()) != null) {
                assertThat(row.getInt(0)).isEqualTo(next);
                next++;
            }
            batch.releaseBatch();
        }
        return next;
    }

    /** A value that does not compress well, so that the data is much larger than the footer. */
    private static String value(int i) {
        long hash = i * 0x9E3779B97F4A7C15L;
        return Long.toHexString(hash) + "-" + Long.toHexString(hash * 31 + i);
    }

    /** Blocks reads below {@link #blockBelow} until {@link #gate} opens. */
    private static class GatedFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;

        private final transient CountDownLatch blocked = new CountDownLatch(1);
        private final transient CountDownLatch gate = new CountDownLatch(1);
        private final transient AtomicInteger closedStreams = new AtomicInteger();
        private volatile long blockBelow = -1;

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            SeekableInputStream in = super.newInputStream(path);
            return new SeekableInputStream() {

                @Override
                public void seek(long desired) throws IOException {
                    in.seek(desired);
                }

                @Override
                public long getPos() throws IOException {
                    return in.getPos();
                }

                @Override
                public int read() throws IOException {
                    awaitGate();
                    return in.read();
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    awaitGate();
                    return in.read(b, off, len);
                }

                @Override
                public void close() throws IOException {
                    closedStreams.incrementAndGet();
                    in.close();
                }

                private void awaitGate() throws IOException {
                    if (in.getPos() >= blockBelow) {
                        return;
                    }

                    blocked.countDown();
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedIOException();
                    }
                }
            };
        }
    }
}
