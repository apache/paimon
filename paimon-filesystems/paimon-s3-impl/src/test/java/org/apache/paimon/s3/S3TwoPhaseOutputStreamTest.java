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

import org.apache.paimon.fs.TwoPhaseOutputStream;

import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link S3TwoPhaseOutputStream}. */
class S3TwoPhaseOutputStreamTest {

    @TempDir java.nio.file.Path tempDir;

    private MockS3MultiPartUpload mockAccessor;
    private S3TwoPhaseOutputStream stream;
    private File targetFile;
    private org.apache.hadoop.fs.Path hadoopPath;

    @BeforeEach
    void setUp() throws IOException {
        hadoopPath = new org.apache.hadoop.fs.Path("/test/file.parquet");
        targetFile = tempDir.resolve("target-file.parquet").toFile();
        targetFile.getParentFile().mkdirs();

        mockAccessor = new MockS3MultiPartUpload(targetFile);
    }

    private S3TwoPhaseOutputStream createStream() throws IOException {
        return new S3TwoPhaseOutputStream(mockAccessor, hadoopPath);
    }

    @Test
    void testLargeDataMultipleParts() throws IOException {
        stream = createStream();

        // Create data larger than MIN_PART_SIZE (5MB) to trigger multiple part uploads
        byte[] bigData = new byte[6 * 1024 * 1024]; // 6MB
        for (int i = 0; i < bigData.length; i++) {
            bigData[i] = (byte) (i % 256);
        }

        // Write the large data
        stream.write(bigData);

        // This should trigger automatic part uploads during write
        assertThat(mockAccessor.startMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.uploadPartCalls).isGreaterThan(0);

        // Add more data to ensure closeForCommit creates another part
        String additionalData = "Additional data for final part";
        stream.write(additionalData.getBytes());

        assertThat(stream.getPos()).isEqualTo(bigData.length + additionalData.length());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Should have uploaded multiple parts
        assertThat(mockAccessor.uploadPartCalls).isGreaterThan(1);
        assertThat(mockAccessor.completeMultipartUploadCalled).isFalse();

        // Target file should not exist yet
        assertThat(targetFile.exists()).isFalse();

        // Commit
        committer.commit();

        // Verify complete multipart upload was called
        assertThat(mockAccessor.completeMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.abortMultipartUploadCalled).isFalse();

        // Target file should now exist with correct content
        assertThat(targetFile.exists()).isTrue();

        // Verify the content is correct by reading it back
        byte[] writtenContent = Files.readAllBytes(targetFile.toPath());
        assertThat(writtenContent).hasSize(bigData.length + additionalData.length());

        // Check first part (bigData)
        for (int i = 0; i < bigData.length; i++) {
            assertThat(writtenContent[i]).isEqualTo(bigData[i]);
        }

        // Check additional data at the end
        byte[] additionalBytes = additionalData.getBytes();
        for (int i = 0; i < additionalBytes.length; i++) {
            assertThat(writtenContent[bigData.length + i]).isEqualTo(additionalBytes[i]);
        }
    }

    @Test
    void testDiscard() throws IOException {
        stream = createStream();
        stream.write("Hello S3 World!".getBytes());

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Verify initial state
        assertThat(mockAccessor.startMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.uploadPartCalls).isEqualTo(1);
        assertThat(targetFile.exists()).isFalse();

        // Discard instead of commit
        committer.discard();

        // Verify abort was called
        assertThat(mockAccessor.abortMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.completeMultipartUploadCalled).isFalse();

        // Target file should not exist
        assertThat(targetFile.exists()).isFalse();
    }

    @Test
    void testCommitAfterDiscard() throws IOException {
        stream = createStream();
        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        committer.discard();

        assertThatThrownBy(() -> committer.commit())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot commit: committer has been discarded");
    }

    @Test
    void testSimpleCommit() throws IOException {
        stream = createStream();

        String testData = "Hello S3 World!";
        stream.write(testData.getBytes());

        assertThat(stream.getPos()).isEqualTo(testData.length());

        // Close for commit
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Target file should not exist yet
        assertThat(targetFile.exists()).isFalse();

        // Commit
        committer.commit();

        // Verify upload completed
        assertThat(mockAccessor.startMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.uploadPartCalls).isEqualTo(1);
        assertThat(mockAccessor.completeMultipartUploadCalled).isTrue();

        // Target file should exist with correct content
        assertThat(targetFile.exists()).isTrue();
        String writtenContent = new String(Files.readAllBytes(targetFile.toPath()));
        assertThat(writtenContent).isEqualTo(testData);
    }

    @Test
    void testDoubleDiscard() throws IOException {
        stream = createStream();
        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        committer.discard();
        // Second discard should be safe (no-op)
        committer.discard();

        // Abort should only be called once
        assertThat(mockAccessor.abortMultipartUploadCallCount).isEqualTo(1);

        // Target file should not exist
        assertThat(targetFile.exists()).isFalse();
    }

    /**
     * Mock implementation that uses local files to simulate S3 multipart upload behavior. Extends
     * S3Accessor but overrides all methods to avoid initialization issues.
     */
    private static class MockS3MultiPartUpload extends S3MultiPartUpload {
        private final List<File> tempPartFiles = new ArrayList<>();
        private final File targetFile;
        private final String mockUploadId = "mock-upload-id-12345";

        // Test tracking variables
        boolean startMultipartUploadCalled = false;
        int uploadPartCalls = 0;
        boolean completeMultipartUploadCalled = false;
        boolean abortMultipartUploadCalled = false;
        int abortMultipartUploadCallCount = 0;

        @SuppressWarnings("unused")
        public MockS3MultiPartUpload(File targetFile) {
            super(createStubFileSystem());
            this.targetFile = targetFile;
        }

        private static S3AFileSystem createStubFileSystem() {
            // Create minimal stub to avoid NullPointerException during initialization
            return new StubS3AFileSystem();
        }

        @Override
        public String pathToObject(org.apache.hadoop.fs.Path hadoopPath) {
            return hadoopPath.toUri().getPath().substring(1);
        }

        @Override
        public String startMultiPartUpload(String key) {
            startMultipartUploadCalled = true;
            return mockUploadId;
        }

        @Override
        public CompletedPart uploadPart(
                String key,
                String uploadId,
                int partNumber,
                boolean isLastPart,
                File inputFile,
                long byteLength)
                throws IOException {
            uploadPartCalls++;

            // Create a temporary copy of the part file
            File tempPartFile = Files.createTempFile("s3-part-" + partNumber, ".tmp").toFile();
            try (FileInputStream fis = new FileInputStream(inputFile);
                    FileOutputStream fos = new FileOutputStream(tempPartFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }
            tempPartFiles.add(tempPartFile);

            // Return mock UploadPartResult
            return CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag("etag-" + partNumber)
                    .build();
        }

        @Override
        public CompleteMultipartUploadResponse completeMultipartUpload(
                String destKey, String uploadId, List<CompletedPart> partETags, long length) {
            completeMultipartUploadCalled = true;

            // Simulate combining all parts into the final target file
            try {
                try (FileOutputStream fos = new FileOutputStream(targetFile)) {
                    for (File partFile : tempPartFiles) {
                        try (FileInputStream fis = new FileInputStream(partFile)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = fis.read(buffer)) != -1) {
                                fos.write(buffer, 0, bytesRead);
                            }
                        }
                    }
                }

                // Clean up temp files
                for (File partFile : tempPartFiles) {
                    if (partFile.exists()) {
                        partFile.delete();
                    }
                }
                tempPartFiles.clear();

                return CompleteMultipartUploadResponse.builder()
                        .bucket("mock-bucket")
                        .key(destKey)
                        .eTag("mock-etag")
                        .build();

            } catch (IOException e) {
                throw new RuntimeException("Failed to complete multipart upload", e);
            }
        }

        @Override
        public void abortMultipartUpload(String destKey, String uploadId) {
            abortMultipartUploadCalled = true;
            abortMultipartUploadCallCount++;

            // Clean up temp files
            for (File partFile : tempPartFiles) {
                if (partFile.exists()) {
                    partFile.delete();
                }
            }
            tempPartFiles.clear();

            // Clean up target file if it exists
            if (targetFile.exists()) {
                targetFile.delete();
            }
        }
    }

    /** Mock implementation of PartETag. */
    private static class MockPartETag extends PartETag {
        private final String eTag;

        public MockPartETag(String eTag, int partNumber) {
            super(partNumber, eTag);
            this.eTag = eTag;
        }

        @Override
        public String getETag() {
            return eTag;
        }
    }

    /**
     * Minimal stub implementation to avoid NullPointerException during S3Accessor initialization.
     */
    private static class StubS3AFileSystem extends S3AFileSystem {
        // Minimal stub - no implementation needed for our mock
    }
}
