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

package org.apache.paimon.oss;

import org.apache.paimon.fs.TwoPhaseOutputStream;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link OssTwoPhaseOutputStream}. */
public class OssTwoPhaseOutputStreamTest {

    @TempDir java.nio.file.Path tempDir;

    private OssTwoPhaseOutputStream stream;
    private MockOSSMultiPartUpload mockAccessor;
    private org.apache.hadoop.fs.Path hadoopPath;
    private File targetFile;

    @BeforeEach
    void setup() throws IOException {
        hadoopPath = new org.apache.hadoop.fs.Path("/test/file.parquet");
        targetFile = tempDir.resolve("target-file.parquet").toFile();
        targetFile.getParentFile().mkdirs();

        mockAccessor = new MockOSSMultiPartUpload(targetFile);
    }

    private OssTwoPhaseOutputStream createStream() throws IOException {
        return new OssTwoPhaseOutputStream(mockAccessor, hadoopPath);
    }

    @Test
    void testLargeDataMultipleParts() throws IOException {
        stream = createStream();

        // Write data larger than MIN_PART_SIZE to trigger automatic part upload,
        // plus some extra data to ensure there's remaining data for final upload
        byte[] largeData = new byte[120 * 1024 * 1024]; // 120MB - will trigger first upload
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }

        stream.write(largeData);

        // Write additional data that will remain in buffer for final upload
        byte[] extraData = "Additional data for final part".getBytes();
        stream.write(extraData);

        assertThat(stream.getPos()).isEqualTo(largeData.length + extraData.length);

        // Should have triggered automatic part upload
        assertThat(mockAccessor.startMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.uploadPartCalls).isEqualTo(1); // One part uploaded automatically

        // Close for commit (uploads remaining data)
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Should have uploaded the remaining data as a final part
        assertThat(mockAccessor.uploadPartCalls).isEqualTo(2); // Initial + final part

        // Commit
        committer.commit();

        assertThat(mockAccessor.completeMultipartUploadCalled).isTrue();

        // Verify target file contains all the data
        assertThat(targetFile.exists()).isTrue();
        byte[] writtenContent = Files.readAllBytes(targetFile.toPath());

        // Combine expected data
        byte[] expectedData = new byte[largeData.length + extraData.length];
        System.arraycopy(largeData, 0, expectedData, 0, largeData.length);
        System.arraycopy(extraData, 0, expectedData, largeData.length, extraData.length);

        assertThat(writtenContent).isEqualTo(expectedData);
    }

    @Test
    void testDiscard() throws IOException {
        stream = createStream();

        stream.write("Some data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        // Discard instead of commit
        committer.discard();

        // Verify abort was called, not complete
        assertThat(mockAccessor.abortMultipartUploadCalled).isTrue();
        assertThat(mockAccessor.completeMultipartUploadCalled).isFalse();

        // Target file should not exist
        assertThat(targetFile.exists()).isFalse();
    }

    @Test
    void testCommitFailure() throws IOException {
        stream = createStream();
        mockAccessor.completeMultipartUploadShouldFail = true;

        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        assertThatThrownBy(committer::commit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to commit multipart upload");

        // Target file should not exist on failed commit
        assertThat(targetFile.exists()).isFalse();
    }

    @Test
    void testUploadPartFailure() throws IOException {
        stream = createStream();
        mockAccessor.uploadPartShouldFail = true;

        // Write data and then close to trigger uploadPart during closeForCommit
        stream.write("test data".getBytes());

        assertThatThrownBy(() -> stream.closeForCommit())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to upload part");
    }

    @Test
    void testPositionTracking() throws IOException {
        stream = createStream();

        assertThat(stream.getPos()).isEqualTo(0);

        stream.write("Hello".getBytes());
        assertThat(stream.getPos()).isEqualTo(5);

        stream.write(" OSS".getBytes());
        assertThat(stream.getPos()).isEqualTo(9);

        stream.write(" World!".getBytes());
        assertThat(stream.getPos()).isEqualTo(16);

        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();
        committer.commit();

        assertThat(mockAccessor.completeMultipartUploadCalled).isTrue();

        // Verify final content
        String writtenContent = new String(Files.readAllBytes(targetFile.toPath()));
        assertThat(writtenContent).isEqualTo("Hello OSS World!");
    }

    @Test
    void testCommitAfterDiscard() throws IOException {
        stream = createStream();
        stream.write("data".getBytes());
        TwoPhaseOutputStream.Committer committer = stream.closeForCommit();

        committer.discard();

        assertThatThrownBy(committer::commit)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot commit: committer has been discarded");

        // Target file should not exist
        assertThat(targetFile.exists()).isFalse();
    }

    /**
     * Mock implementation that actually uses local files to simulate OSS multipart upload behavior.
     * Extends OSSAccessor but overrides all methods to avoid initialization issues.
     */
    private static class MockOSSMultiPartUpload extends OSSMultiPartUpload {

        boolean startMultipartUploadCalled = false;
        int uploadPartCalls = 0;
        boolean completeMultipartUploadCalled = false;
        int completeMultipartUploadCallCount = 0;
        boolean abortMultipartUploadCalled = false;

        boolean uploadPartShouldFail = false;
        boolean completeMultipartUploadShouldFail = false;

        private final String mockUploadId = "mock-upload-" + UUID.randomUUID();
        private final List<File> tempPartFiles = new ArrayList<>();
        private final File targetFile;

        @SuppressWarnings("unused")
        public MockOSSMultiPartUpload(File targetFile) {
            super(createStubFileSystem()); // Create minimal stub to avoid null pointer
            this.targetFile = targetFile;
        }

        private static org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem createStubFileSystem() {
            // Create a minimal stub to avoid NullPointerException during initialization
            return new StubAliyunOSSFileSystem();
        }

        @Override
        public String pathToObject(org.apache.hadoop.fs.Path hadoopPath) {
            return hadoopPath.toUri().getPath().substring(1);
        }

        @Override
        public String startMultiPartUpload(String objectName) {
            startMultipartUploadCalled = true;
            return mockUploadId;
        }

        @Override
        public PartETag uploadPart(
                String objectName,
                String uploadId,
                int partNumber,
                boolean isLastPart,
                File file,
                long byteLength)
                throws IOException {
            uploadPartCalls++;

            if (uploadPartShouldFail) {
                throw new IOException("Mock upload part failure");
            }

            // Verify file exists and has content
            if (!file.exists() || file.length() == 0) {
                throw new IOException("Invalid file for upload: " + file);
            }

            // Store the part file in a temporary location (simulating storing in OSS)
            File partFile =
                    Files.createTempFile("mock-oss-part-" + partNumber + "-", ".tmp").toFile();
            Files.copy(file.toPath(), partFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            tempPartFiles.add(partFile);

            MockPartETag mockPartETag = new MockPartETag(partNumber, "mock-etag-" + partNumber);
            return mockPartETag;
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(
                String objectName,
                String uploadId,
                List<PartETag> partETags,
                long numBytesInParts) {
            completeMultipartUploadCalled = true;
            completeMultipartUploadCallCount++;

            if (completeMultipartUploadShouldFail) {
                throw new RuntimeException("Mock complete multipart upload failure");
            }

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
            } catch (IOException e) {
                throw new RuntimeException("Failed to complete multipart upload", e);
            }

            // Clean up temp part files
            for (File partFile : tempPartFiles) {
                partFile.delete();
            }
            tempPartFiles.clear();

            return new MockCompleteMultipartUploadResult(objectName, "mock-final-etag");
        }

        @Override
        public void abortMultipartUpload(String objectName, String uploadId) {
            abortMultipartUploadCalled = true;

            // Clean up temp part files on abort
            for (File partFile : tempPartFiles) {
                partFile.delete();
            }
            tempPartFiles.clear();

            // Ensure target file doesn't exist
            if (targetFile.exists()) {
                targetFile.delete();
            }
        }
    }

    /** Mock implementation of PartETag. */
    private static class MockPartETag extends PartETag {
        private final int partNumber;
        private final String eTag;

        public MockPartETag(int partNumber, String eTag) {
            super(partNumber, eTag);
            this.partNumber = partNumber;
            this.eTag = eTag;
        }

        @Override
        public int getPartNumber() {
            return partNumber;
        }

        @Override
        public String getETag() {
            return eTag;
        }
    }

    /** Mock implementation of CompleteMultipartUploadResult. */
    private static class MockCompleteMultipartUploadResult extends CompleteMultipartUploadResult {
        private final String key;
        private final String eTag;

        public MockCompleteMultipartUploadResult(String key, String eTag) {
            this.key = key;
            this.eTag = eTag;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getETag() {
            return eTag;
        }
    }

    /**
     * Minimal stub implementation to avoid NullPointerException during OSSAccessor initialization.
     */
    private static class StubAliyunOSSFileSystem
            extends org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem {
        private final StubAliyunOSSFileSystemStore stubStore = new StubAliyunOSSFileSystemStore();

        @Override
        public org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore getStore() {
            return stubStore;
        }
    }

    /** Minimal stub implementation for the store. */
    private static class StubAliyunOSSFileSystemStore
            extends org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore {
        // Empty stub - we override all methods in MockOSSAccessor anyway
    }
}
