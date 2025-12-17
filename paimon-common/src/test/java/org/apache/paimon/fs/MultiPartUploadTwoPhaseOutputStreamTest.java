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

package org.apache.paimon.fs;

import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MultiPartUploadTwoPhaseOutputStream}. */
class MultiPartUploadTwoPhaseOutputStreamTest {

    private FakeMultiPartUploadStore store;
    private LocalFileIO fileIO;
    private org.apache.hadoop.fs.Path objectPath;

    @BeforeEach
    void setUp() {
        this.store = new FakeMultiPartUploadStore();
        this.fileIO = new LocalFileIO();
        this.objectPath = new org.apache.hadoop.fs.Path("folder/file.txt");
    }

    @Test
    void testWriteFlushAndCommit() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 5);

        stream.write("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(store.getUploadedParts()).hasSize(1);
        assertThat(store.getUploadedParts()).extracting(TestPart::getPartNumber).containsExactly(1);
        assertThat(store.getUploadedParts())
                .extracting(TestPart::getContent)
                .containsExactly("hello");

        stream.write(" world!".getBytes(StandardCharsets.UTF_8));
        assertThat(store.getUploadedParts()).hasSize(2);
        assertThat(stream.getPos())
                .isEqualTo("hello world!".getBytes(StandardCharsets.UTF_8).length);

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        assertThat(store.getUploadedParts()).hasSize(3);
        assertThat(committer.targetPath().toString()).isEqualTo(store.getStartedObjectName());

        committer.commit(fileIO);

        assertThat(store.getCompletedUploadId()).isEqualTo(store.getStartedUploadId());
        assertThat(store.getCompletedObjectName()).isEqualTo(store.getStartedObjectName());
        assertThat(store.getCompletedParts()).containsExactlyElementsOf(store.getUploadedParts());
        assertThat(store.getCompletedBytes()).isEqualTo(stream.getPos());
        assertThat(store.getAbortedUploadId()).isNull();
    }

    @Test
    void testCloseForCommitFlushesRemainingBuffer() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 5);

        stream.write("abc".getBytes(StandardCharsets.UTF_8));
        assertThat(store.getUploadedParts()).isEmpty();

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        assertThat(store.getUploadedParts()).hasSize(1);
        assertThat(store.getUploadedParts().get(0).getContent()).isEqualTo("abc");

        committer.commit(fileIO);

        assertThat(store.getCompletedBytes()).isEqualTo(3);
    }

    @Test
    void testDiscardAbortsUpload() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 5);

        stream.write("data".getBytes(StandardCharsets.UTF_8));
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        committer.discard(fileIO);

        assertThat(store.getAbortedUploadId()).isEqualTo(store.getStartedUploadId());
        assertThat(store.getCompletedUploadId()).isNull();
    }

    @Test
    void testCloseForCommitIdempotent() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 5);

        TwoPhaseOutputStream.Committer first = stream.closeForCommit();

        assertThatThrownBy(stream::closeForCommit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("already closed");
        first.commit(fileIO);
    }

    @Test
    void testBigWriteSplitByThreshold() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 5);

        byte[] data1 = "abc".getBytes(StandardCharsets.UTF_8);
        stream.write(data1);
        byte[] data2 = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        stream.write(data2);

        assertThat(store.getUploadedParts()).hasSize(2);
        assertThat(store.getUploadedParts())
                .extracting(TestPart::getPartNumber)
                .containsExactly(1, 2);
        assertThat(store.getUploadedParts())
                .extracting(TestPart::getContent)
                .containsExactly("abcab", "cdefg");
        assertThat(stream.getPos()).isEqualTo(data1.length + data2.length);
        stream.flush();
        assertThat(store.getUploadedParts())
                .extracting(TestPart::getContent)
                .containsExactly("abcab", "cdefg");
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        assertThat(store.getUploadedParts()).hasSize(3);

        committer.commit(fileIO);

        assertThat(store.getCompletedUploadId()).isEqualTo(store.getStartedUploadId());
        assertThat(store.getCompletedObjectName()).isEqualTo(store.getStartedObjectName());
        assertThat(store.getCompletedParts()).containsExactlyElementsOf(store.getUploadedParts());
        assertThat(store.getCompletedBytes()).isEqualTo(stream.getPos());
        assertThat(store.getAbortedUploadId()).isNull();
    }

    @Test
    void testFlushWhenBufferSizeIsSmallerThanThresholdDoesNotUpload() throws IOException {
        TestMultiPartUploadTwoPhaseOutputStream stream =
                new TestMultiPartUploadTwoPhaseOutputStream(store, objectPath, 10);

        // Write 3 bytes, which is less than the threshold of 10
        stream.write("abc".getBytes(StandardCharsets.UTF_8));
        assertThat(store.getUploadedParts()).isEmpty();

        // Flush should not trigger upload since buffer size (3) < threshold (10)
        stream.flush();
        assertThat(store.getUploadedParts()).isEmpty();
        assertThat(stream.getPos()).isEqualTo(3);

        // Write another 4 bytes, total buffer size is 7, still less than threshold
        stream.write("defg".getBytes(StandardCharsets.UTF_8));
        assertThat(store.getUploadedParts()).isEmpty();

        // Flush again, should still not upload since buffer size (7) < threshold (10)
        stream.flush();
        assertThat(store.getUploadedParts()).isEmpty();
        assertThat(stream.getPos()).isEqualTo(7);

        // Only when closeForCommit is called, the remaining buffer should be uploaded
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        assertThat(store.getUploadedParts()).hasSize(1);
        assertThat(store.getUploadedParts().get(0).getContent()).isEqualTo("abcdefg");
        assertThat(store.getUploadedParts().get(0).getPartNumber()).isEqualTo(1);

        committer.commit(fileIO);
        assertThat(store.getCompletedBytes()).isEqualTo(7);
    }

    /** Fake store implementation for testing. */
    private static class FakeMultiPartUploadStore
            implements MultiPartUploadStore<TestPart, String> {

        private final List<TestPart> uploadedParts = new ArrayList<>();
        private String startedUploadId;
        private String startedObjectName;
        private String completedUploadId;
        private String completedObjectName;
        private List<TestPart> completedParts = Collections.emptyList();
        private Long completedBytes;
        private String abortedUploadId;

        @Override
        public org.apache.hadoop.fs.Path workingDirectory() {
            return new org.apache.hadoop.fs.Path("/bucket");
        }

        @Override
        public String startMultiPartUpload(String objectName) {
            this.startedObjectName = objectName;
            this.startedUploadId = UUID.randomUUID().toString();
            this.uploadedParts.clear();
            this.completedUploadId = null;
            this.completedObjectName = null;
            this.completedParts = Collections.emptyList();
            this.completedBytes = null;
            this.abortedUploadId = null;
            return startedUploadId;
        }

        @Override
        public String completeMultipartUpload(
                String objectName,
                String uploadId,
                List<TestPart> partETags,
                long numBytesInParts) {
            this.completedObjectName = objectName;
            this.completedUploadId = uploadId;
            this.completedParts = new ArrayList<>(partETags);
            this.completedBytes = numBytesInParts;
            return uploadId + "-completed";
        }

        @Override
        public TestPart uploadPart(
                String objectName, String uploadId, int partNumber, File file, int byteLength)
                throws IOException {
            byte[] bytes = Files.readAllBytes(file.toPath());
            String content = new String(bytes, StandardCharsets.UTF_8);
            TestPart part = new TestPart(partNumber, content, byteLength);
            uploadedParts.add(part);
            return part;
        }

        @Override
        public void abortMultipartUpload(String objectName, String uploadId) {
            this.abortedUploadId = uploadId;
        }

        public String getStartedUploadId() {
            return startedUploadId;
        }

        public String getStartedObjectName() {
            return startedObjectName;
        }

        public List<TestPart> getUploadedParts() {
            return Collections.unmodifiableList(uploadedParts);
        }

        public String getCompletedUploadId() {
            return completedUploadId;
        }

        public String getCompletedObjectName() {
            return completedObjectName;
        }

        public List<TestPart> getCompletedParts() {
            return completedParts;
        }

        public Long getCompletedBytes() {
            return completedBytes;
        }

        public String getAbortedUploadId() {
            return abortedUploadId;
        }
    }

    private static class TestMultiPartUploadTwoPhaseOutputStream
            extends MultiPartUploadTwoPhaseOutputStream<TestPart, String> {

        private final FakeMultiPartUploadStore store;
        private final int threshold;

        private TestMultiPartUploadTwoPhaseOutputStream(
                FakeMultiPartUploadStore store, org.apache.hadoop.fs.Path path, int threshold)
                throws IOException {
            super(store, path, new Path(path.toString()));
            this.store = store;
            this.threshold = threshold;
        }

        @Override
        public int partSizeThreshold() {
            return threshold;
        }

        @Override
        public Committer committer() {
            return new TestCommitter(store, uploadId, uploadedParts, objectName, position);
        }
    }

    private static class TestCommitter implements TwoPhaseOutputStream.Committer {

        private final FakeMultiPartUploadStore store;
        private final String uploadId;
        private final List<TestPart> parts;
        private final String objectName;
        private final long byteLength;
        private boolean committed;
        private boolean discarded;

        private TestCommitter(
                FakeMultiPartUploadStore store,
                String uploadId,
                List<TestPart> parts,
                String objectName,
                long position) {
            this.store = store;
            this.uploadId = uploadId;
            this.parts = new ArrayList<>(parts);
            this.objectName = objectName;
            this.byteLength = position;
        }

        @Override
        public void commit(FileIO fileIO) throws IOException {
            if (discarded) {
                throw new IOException("Cannot commit after discard");
            }
            if (!committed) {
                store.completeMultipartUpload(objectName, uploadId, parts, byteLength);
                committed = true;
            }
        }

        @Override
        public void discard(FileIO fileIO) throws IOException {
            if (!discarded) {
                store.abortMultipartUpload(objectName, uploadId);
                discarded = true;
            }
        }

        @Override
        public Path targetPath() {
            return new Path(objectName);
        }

        @Override
        public void clean(FileIO fileIO) throws IOException {}
    }

    private static final class TestPart {
        private final int partNumber;
        private final String content;
        private final long byteLength;

        private TestPart(int partNumber, String content, long byteLength) {
            this.partNumber = partNumber;
            this.content = content;
            this.byteLength = byteLength;
        }

        int getPartNumber() {
            return partNumber;
        }

        String getContent() {
            return content;
        }
    }
}
