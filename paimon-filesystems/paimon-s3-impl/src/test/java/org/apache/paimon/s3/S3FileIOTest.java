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

package org.apache.paimon.s3;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link S3FileIO}. */
public class S3FileIOTest {

    @Test
    public void testOverwriteOutputStreamBypassesHadoopCreate() throws Exception {
        TestingS3FileIO fileIO = new TestingS3FileIO();
        Path path = new Path("s3://bucket/table/snapshot/snapshot-1");

        try (PositionOutputStream out = fileIO.newOutputStream(path, true)) {
            out.write("snapshot".getBytes(StandardCharsets.UTF_8));
        }

        assertThat(fileIO.directOutputStreamCalls).isEqualTo(1);
        assertThat(fileIO.createFileSystemCalls).isEqualTo(0);
        assertThat(fileIO.outputStream.content()).isEqualTo("snapshot");
        assertThat(fileIO.outputStream.overwrite).isTrue();
        assertThat(fileIO.outputStream.closed).isTrue();
    }

    @Test
    public void testHintOverwriteOutputStreamBypassesHadoopCreate() throws Exception {
        TestingS3FileIO fileIO = new TestingS3FileIO();
        Path path = new Path("s3://bucket/table/snapshot/LATEST");

        try (PositionOutputStream out = fileIO.newOutputStream(path, true)) {
            out.write("1".getBytes(StandardCharsets.UTF_8));
        }

        assertThat(fileIO.directOutputStreamCalls).isEqualTo(1);
        assertThat(fileIO.createFileSystemCalls).isEqualTo(0);
        assertThat(fileIO.outputStream.content()).isEqualTo("1");
        assertThat(fileIO.outputStream.overwrite).isTrue();
        assertThat(fileIO.outputStream.closed).isTrue();
    }

    @Test
    public void testNoOverwriteSnapshotOutputStreamBypassesHadoopCreate() throws Exception {
        TestingS3FileIO fileIO = new TestingS3FileIO();
        Path path = new Path("s3://bucket/table/snapshot/snapshot-1");

        assertThat(fileIO.supportsAtomicCreateWithoutOverwrite(path)).isTrue();

        try (PositionOutputStream out = fileIO.newOutputStream(path, false)) {
            out.write("snapshot".getBytes(StandardCharsets.UTF_8));
        }

        assertThat(fileIO.directOutputStreamCalls).isEqualTo(1);
        assertThat(fileIO.createFileSystemCalls).isEqualTo(0);
        assertThat(fileIO.outputStream.content()).isEqualTo("snapshot");
        assertThat(fileIO.outputStream.overwrite).isFalse();
        assertThat(fileIO.outputStream.closed).isTrue();
    }

    @Test
    public void testNoOverwriteSnapshotOutputStreamPreservesConflictSignal() throws Exception {
        TestingS3FileIO fileIO = new TestingS3FileIO();
        fileIO.failDirectOutputStreamClose =
                new FileAlreadyExistsException("s3://bucket/table/snapshot/snapshot-1");
        Path path = new Path("s3://bucket/table/snapshot/snapshot-1");

        PositionOutputStream out = fileIO.newOutputStream(path, false);
        out.write("snapshot".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(out::close)
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessageContaining("snapshot-1");
        assertThat(fileIO.directOutputStreamCalls).isEqualTo(1);
        assertThat(fileIO.createFileSystemCalls).isEqualTo(0);
        assertThat(fileIO.outputStream.overwrite).isFalse();
    }

    @Test
    public void testNoOverwritePutObjectRequestUsesIfNoneMatchHeader() {
        PutObjectRequest request =
                PutObjectRequest.builder()
                        .bucket("bucket")
                        .key("table/snapshot/snapshot-1")
                        .build();

        assertThat(S3FileIO.applyOverwriteMode(request, false).ifNoneMatch()).isEqualTo("*");
        assertThat(S3FileIO.applyOverwriteMode(request, true).ifNoneMatch()).isNull();
    }

    @Test
    public void testPreconditionFailedIsFileExistsSignal() {
        IOException exception =
                new IOException(
                        S3Exception.builder()
                                .statusCode(412)
                                .message("Precondition Failed")
                                .build());

        assertThat(S3FileIO.isPreconditionFailed(exception)).isTrue();
    }

    @Test
    public void testConditionalRequestConflictIsFileExistsSignal() {
        IOException exception =
                new IOException(
                        S3Exception.builder()
                                .statusCode(409)
                                .message("ConditionalRequestConflict")
                                .build());

        assertThat(S3FileIO.isPreconditionFailed(exception)).isTrue();
    }

    @Test
    public void testNonSnapshotOverwriteOutputStreamKeepsHadoopCreatePath() {
        TestingS3FileIO fileIO = new TestingS3FileIO();
        Path path = new Path("s3://bucket/table/data/file-1");

        assertThat(fileIO.supportsAtomicCreateWithoutOverwrite(path)).isFalse();

        assertThatThrownBy(() -> fileIO.newOutputStream(path, true))
                .isInstanceOf(UncheckedIOException.class)
                .hasCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("Hadoop create called");

        assertThat(fileIO.directOutputStreamCalls).isEqualTo(0);
        assertThat(fileIO.createFileSystemCalls).isEqualTo(1);
    }

    private static class TestingS3FileIO extends S3FileIO {

        private int directOutputStreamCalls;
        private int createFileSystemCalls;
        private CapturingPositionOutputStream outputStream;
        private IOException failDirectOutputStreamClose;

        @Override
        protected PositionOutputStream newDirectOutputStream(Path path, boolean overwrite) {
            directOutputStreamCalls++;
            outputStream = new CapturingPositionOutputStream(overwrite);
            outputStream.failOnClose = failDirectOutputStreamClose;
            return outputStream;
        }

        @Override
        protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
            createFileSystemCalls++;
            throw new UncheckedIOException(new IOException("Hadoop create called"));
        }
    }

    private static class CapturingPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final boolean overwrite;
        private IOException failOnClose;
        private boolean closed;

        private CapturingPositionOutputStream(boolean overwrite) {
            this.overwrite = overwrite;
        }

        @Override
        public long getPos() {
            return buffer.size();
        }

        @Override
        public void write(int b) throws IOException {
            checkOpen();
            buffer.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkOpen();
            buffer.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            checkOpen();
        }

        @Override
        public void close() throws IOException {
            if (failOnClose != null) {
                throw failOnClose;
            }
            closed = true;
        }

        private String content() {
            return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream is closed");
            }
        }
    }
}
