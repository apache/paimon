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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.api.RequestFactory;
import org.apache.hadoop.fs.s3a.audit.AuditSpanS3A;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.StoreContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
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

    private static final AuditSpanS3A TEST_AUDIT_SPAN = new TestingAuditSpan();

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
    public void testNoOverwriteDirectPutPreconditionFailureMapsToFileAlreadyExists()
            throws Exception {
        for (int statusCode : new int[] {409, 412}) {
            IOException failure = s3Failure(statusCode);
            DirectPutTestingS3FileIO fileIO = new DirectPutTestingS3FileIO(failure);
            Path path = new Path("s3://bucket/table/snapshot/snapshot-1");

            PositionOutputStream out = fileIO.newOutputStream(path, false);
            out.write("snapshot".getBytes(StandardCharsets.UTF_8));

            assertThatThrownBy(out::close)
                    .isInstanceOf(FileAlreadyExistsException.class)
                    .hasMessageContaining("snapshot-1")
                    .hasCause(failure);
            assertThat(fileIO.createFileSystemCalls).isEqualTo(1);
            assertThat(fileIO.fileSystem.writeHelper.request.ifNoneMatch()).isEqualTo("*");
        }
    }

    @Test
    public void testOverwriteDirectPutPreconditionFailureKeepsOriginalIOException()
            throws Exception {
        for (int statusCode : new int[] {409, 412}) {
            IOException failure = s3Failure(statusCode);
            DirectPutTestingS3FileIO fileIO = new DirectPutTestingS3FileIO(failure);
            Path path = new Path("s3://bucket/table/snapshot/snapshot-1");

            PositionOutputStream out = fileIO.newOutputStream(path, true);
            out.write("snapshot".getBytes(StandardCharsets.UTF_8));

            assertThatThrownBy(out::close)
                    .isSameAs(failure)
                    .isNotInstanceOf(FileAlreadyExistsException.class);
            assertThat(fileIO.createFileSystemCalls).isEqualTo(1);
            assertThat(fileIO.fileSystem.writeHelper.request.ifNoneMatch()).isNull();
        }
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

    @Test
    public void testSnapshotMetadataPathClassification() throws Exception {
        S3FileIO fileIO = new S3FileIO();

        assertThat(
                        fileIO.supportsAtomicCreateWithoutOverwrite(
                                new Path("s3://bucket/table/snapshot/EARLIEST")))
                .isTrue();
        assertThat(
                        fileIO.supportsAtomicCreateWithoutOverwrite(
                                new Path("s3://bucket/table/snapshot/not-snapshot")))
                .isFalse();
        assertThat(
                        fileIO.supportsAtomicCreateWithoutOverwrite(
                                new Path("s3://bucket/table/metadata/snapshot-1")))
                .isFalse();
    }

    private static IOException s3Failure(int statusCode) {
        return new IOException(
                S3Exception.builder().statusCode(statusCode).message("S3 failure").build());
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

    private static class DirectPutTestingS3FileIO extends S3FileIO {

        private final FailingS3AFileSystem fileSystem;
        private int createFileSystemCalls;

        private DirectPutTestingS3FileIO(IOException failure) {
            this.fileSystem = new FailingS3AFileSystem(failure);
        }

        @Override
        protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
            createFileSystemCalls++;
            return fileSystem;
        }
    }

    private static class FailingS3AFileSystem extends S3AFileSystem {

        private final FailingWriteOperationHelper writeHelper;

        private FailingS3AFileSystem(IOException failure) {
            this.writeHelper = new FailingWriteOperationHelper(this, failure);
        }

        @Override
        public AuditSpanS3A getActiveAuditSpan() {
            return TEST_AUDIT_SPAN;
        }

        @Override
        public WriteOperationHelper createWriteOperationHelper(AuditSpan auditSpan) {
            return writeHelper;
        }

        @Override
        public String pathToKey(org.apache.hadoop.fs.Path path) {
            String uriPath = path.toUri().getPath();
            return uriPath.startsWith("/") ? uriPath.substring(1) : uriPath;
        }

        @Override
        public String getBucket() {
            return "bucket";
        }

        @Override
        public StoreContext createStoreContext() {
            return null;
        }

        @Override
        public RequestFactory getRequestFactory() {
            return null;
        }
    }

    private static class FailingWriteOperationHelper extends WriteOperationHelper {

        private final IOException failure;
        private PutObjectRequest request;

        private FailingWriteOperationHelper(S3AFileSystem fileSystem, IOException failure) {
            super(fileSystem, new Configuration(), null, null, TEST_AUDIT_SPAN, null);
            this.failure = failure;
        }

        @Override
        public PutObjectRequest createPutObjectRequest(
                String key, long length, PutObjectOptions options) {
            return PutObjectRequest.builder()
                    .bucket("bucket")
                    .key(key)
                    .contentLength(length)
                    .build();
        }

        @Override
        public PutObjectResponse putObject(
                PutObjectRequest request,
                PutObjectOptions options,
                S3ADataBlocks.BlockUploadData uploadData,
                DurationTrackerFactory durationTrackerFactory)
                throws IOException {
            this.request = request;
            throw failure;
        }
    }

    private static class TestingAuditSpan implements AuditSpanS3A {

        @Override
        public String getSpanId() {
            return "test-span";
        }

        @Override
        public String getOperationName() {
            return "test";
        }

        @Override
        public long getTimestamp() {
            return 0L;
        }

        @Override
        public AuditSpan activate() {
            return this;
        }

        @Override
        public void deactivate() {}
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
