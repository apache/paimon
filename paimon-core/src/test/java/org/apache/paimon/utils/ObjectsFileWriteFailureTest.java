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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * When both the write and the closing of the output stream fail, {@link
 * ObjectsFile#writeWithoutRolling} must report the write failure and keep the close failure as a
 * suppressed exception rather than letting the latter replace the former.
 */
class ObjectsFileWriteFailureTest {

    @Test
    void writeFailureSurvivesAFailingStreamClose(@TempDir java.nio.file.Path tempDir) {
        FailingStream stream = new FailingStream();
        ObjectsFile<String> file = objectsFile(tempDir, stream, true);

        assertThatThrownBy(() -> file.writeWithoutRolling(Collections.emptyIterator()))
                .isInstanceOf(RuntimeException.class)
                .cause()
                .hasMessage("writer close failed")
                .satisfies(
                        cause ->
                                assertThat(cause.getSuppressed())
                                        .extracting(Throwable::getMessage)
                                        .containsExactly("stream close failed"));

        assertThat(stream.closed).isTrue();
    }

    /** The stream is closed even when the write succeeds and the position read is the last step. */
    @Test
    void streamIsClosedOnTheSuccessPath(@TempDir java.nio.file.Path tempDir) throws Exception {
        FailingStream stream = new FailingStream();
        stream.failOnClose = false;
        ObjectsFile<String> file = objectsFile(tempDir, stream, false);

        assertThat(file.writeWithoutRolling(Collections.emptyIterator()).getValue()).isEqualTo(7L);
        assertThat(stream.closed).isTrue();
    }

    private static ObjectsFile<String> objectsFile(
            java.nio.file.Path tempDir, PositionOutputStream stream, boolean writerCloseFails) {
        Path path = new Path(tempDir.toUri().toString(), "manifest-0");
        FileIO fileIO =
                new LocalFileIO() {
                    @Override
                    public PositionOutputStream newOutputStream(Path file, boolean overwrite) {
                        return stream;
                    }
                };
        return new ObjectsFile<String>(
                fileIO,
                null,
                null,
                (f, size) -> {
                    throw new UnsupportedOperationException();
                },
                (out, compression) -> new StubWriter(writerCloseFails),
                "none",
                new PathFactory() {
                    @Override
                    public Path newPath() {
                        return path;
                    }

                    @Override
                    public Path toPath(String fileName) {
                        return path;
                    }
                },
                null) {};
    }

    private static class StubWriter implements FormatWriter {

        private final boolean failOnClose;

        private StubWriter(boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public void addElement(InternalRow element) {}

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() throws IOException {
            if (failOnClose) {
                throw new IOException("writer close failed");
            }
        }
    }

    private static class FailingStream extends PositionOutputStream {

        private boolean failOnClose = true;
        private boolean closed = false;

        @Override
        public long getPos() {
            return 7L;
        }

        @Override
        public void write(int b) {}

        @Override
        public void write(byte[] b) {}

        @Override
        public void write(byte[] b, int off, int len) {}

        @Override
        public void flush() {}

        @Override
        public void close() throws IOException {
            closed = true;
            if (failOnClose) {
                throw new IOException("stream close failed");
            }
        }
    }
}
