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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileAwareFormatWriter;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link SingleFileWriter}. */
public class SingleFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path path;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        path = new Path(tempDir.toString(), "data-0.orc");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRuntimeExceptionWhileOpeningDeletesFile(boolean asyncWrite) throws IOException {
        // for example an unknown value of file.compression, which ORC rejects with
        // IllegalArgumentException from CompressionKind.valueOf
        assertThatThrownBy(
                        () ->
                                newWriter(
                                        (out, compression) -> {
                                            throw new IllegalArgumentException("bad compression");
                                        },
                                        asyncWrite))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad compression");

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testIOExceptionWhileOpeningDeletesFile() throws IOException {
        assertThatThrownBy(
                        () ->
                                newWriter(
                                        (out, compression) -> {
                                            throw new IOException("boom");
                                        }))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testExistingFileIsKeptWhenOpeningFails() throws IOException {
        fileIO.writeFile(path, "keep me", false);

        // newOutputStream refuses to overwrite, and that file is not ours to delete
        assertThatThrownBy(() -> newWriter((out, compression) -> new NoOpFormatWriter()))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(fileIO.exists(path)).isTrue();
        assertThat(fileIO.readFileUtf8(path)).isEqualTo("keep me");
    }

    @Test
    public void testCleanupWhenOnlyFormatWriterWasCreated() throws IOException {
        DirectWriteFactory factory = new DirectWriteFactory();

        assertThatThrownBy(() -> newWriter(factory))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot set file");

        assertThat(factory.writer.isClosed()).isTrue();
        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testSubclassAbortIsNotCalledWhileOpening() throws IOException {
        // a subclass whose abort() touches state assigned after super(...) must not be driven from
        // the super constructor, otherwise the real failure is replaced by a NullPointerException
        assertThatThrownBy(
                        () ->
                                new LateFieldWriter(
                                        fileIO,
                                        (out, compression) -> {
                                            throw new IllegalArgumentException("bad compression");
                                        },
                                        path))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bad compression");

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testSubclassAbortIsNotCalledWhileOpeningOnIOException() throws IOException {
        assertThatThrownBy(
                        () ->
                                new LateFieldWriter(
                                        fileIO,
                                        (out, compression) -> {
                                            throw new IOException("boom");
                                        },
                                        path))
                .isInstanceOf(UncheckedIOException.class);

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testRuntimeExceptionWhileClosingDeletesFile() throws IOException {
        // several format writers wrap IO failures in unchecked exceptions on the close path
        TestSingleFileWriter writer =
                newWriter(
                        (out, compression) ->
                                new ThrowingCloseWriter(new IllegalStateException("cannot close")));

        assertThatThrownBy(writer::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot close");

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testIOExceptionWhileClosingDeletesFile() throws IOException {
        TestSingleFileWriter writer =
                newWriter((out, compression) -> new ThrowingCloseWriter(new IOException("boom")));

        // the checked failure must still reach the caller unwrapped
        assertThatThrownBy(writer::close).isInstanceOf(IOException.class).hasMessage("boom");

        assertThat(fileIO.exists(path)).isFalse();
    }

    @Test
    public void testRuntimeExceptionWhileFlushingClosesStream() throws IOException {
        // the output stream can fail with an unchecked exception too, for example
        // AsyncPositionOutputStream when the writing thread is interrupted
        FileIO trackedFileIO = new FlushFailingFileIO();
        TestSingleFileWriter writer =
                new TestSingleFileWriter(
                        trackedFileIO, (out, compression) -> new NoOpFormatWriter(), path, false);

        assertThatThrownBy(writer::close)
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("cannot flush");

        assertThat(TraceableFileIO.openOutputStreams(path::equals)).isEmpty();
        assertThat(trackedFileIO.exists(path)).isFalse();
    }

    @Test
    public void testCleanupFailureDoesNotReplaceOriginalException() {
        TestSingleFileWriter writer =
                new TestSingleFileWriter(
                        new DeleteFailingFileIO(),
                        (out, compression) ->
                                new ThrowingCloseWriter(new IllegalStateException("cannot close")),
                        path,
                        false);

        assertThatThrownBy(writer::close)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("cannot close")
                .hasSuppressedException(new RuntimeException("cannot delete"));
    }

    @Test
    public void testSuccessfulOpenKeepsFile() throws IOException {
        NoOpFormatWriter formatWriter = new NoOpFormatWriter();
        TestSingleFileWriter writer = newWriter((out, compression) -> formatWriter);

        assertThat(fileIO.exists(path)).isTrue();
        assertThat(formatWriter.isClosed()).isFalse();

        writer.close();

        assertThat(fileIO.exists(path)).isTrue();
        assertThat(formatWriter.isClosed()).isTrue();
    }

    private TestSingleFileWriter newWriter(FormatWriterFactory factory) {
        return newWriter(factory, false);
    }

    private TestSingleFileWriter newWriter(FormatWriterFactory factory, boolean asyncWrite) {
        return new TestSingleFileWriter(fileIO, factory, path, asyncWrite);
    }

    private static class TestSingleFileWriter extends SingleFileWriter<InternalRow, Void> {

        private TestSingleFileWriter(
                FileIO fileIO, FormatWriterFactory factory, Path path, boolean asyncWrite) {
            super(fileIO, factory, path, Function.identity(), "zstd", asyncWrite);
        }

        @Override
        public Void result() {
            return null;
        }
    }

    /** Mirrors {@link RowDataFileWriter}, whose auxiliary writers are assigned after super(...). */
    private static class LateFieldWriter extends SingleFileWriter<InternalRow, Void> {

        private final List<String> assignedAfterSuper;

        private LateFieldWriter(FileIO fileIO, FormatWriterFactory factory, Path path) {
            super(fileIO, factory, path, Function.identity(), "zstd", false);
            this.assignedAfterSuper = Collections.emptyList();
        }

        @Override
        public void abort() {
            if (!assignedAfterSuper.isEmpty()) {
                throw new IllegalStateException("unreachable");
            }
            super.abort();
        }

        @Override
        public Void result() {
            return null;
        }
    }

    private static class NoOpFormatWriter implements FormatWriter {

        private boolean closed;

        boolean isClosed() {
            return closed;
        }

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class ThrowingCloseWriter implements FormatWriter {

        private final Throwable failure;

        private ThrowingCloseWriter(Throwable failure) {
            this.failure = failure;
        }

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() throws IOException {
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            throw (RuntimeException) failure;
        }
    }

    private static class DeleteFailingFileIO extends LocalFileIO {

        @Override
        public boolean delete(Path f, boolean recursive) {
            throw new RuntimeException("cannot delete");
        }
    }

    private static class FlushFailingFileIO extends TraceableFileIO {

        @Override
        public PositionOutputStream newOutputStream(Path f, boolean overwrite) throws IOException {
            return new PositionOutputStreamWrapper(super.newOutputStream(f, overwrite)) {
                @Override
                public void flush() {
                    throw new RuntimeException("cannot flush");
                }
            };
        }
    }

    private static class DirectWriteFactory implements FormatWriterFactory, SupportsDirectWrite {

        private final FileAwareWriter writer = new FileAwareWriter();

        @Override
        public FormatWriter create(PositionOutputStream out, String compression) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FormatWriter create(FileIO fileIO, Path path, String compression)
                throws IOException {
            // the format owns the file here, so it is created before the writer is handed out
            fileIO.writeFile(path, "partial", false);
            return writer;
        }
    }

    private static class FileAwareWriter extends NoOpFormatWriter implements FileAwareFormatWriter {

        @Override
        public void setFile(Path file) {
            throw new IllegalStateException("cannot set file");
        }

        @Override
        public boolean deleteFileUponAbort() {
            return true;
        }
    }
}
