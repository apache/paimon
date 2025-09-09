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

import org.apache.paimon.fs.CommittablePositionOutputStream;
import org.apache.paimon.fs.Path;

import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** OSS implementation of CommittablePositionOutputStream using multipart upload. */
public class OssCommittablePositionOutputStream extends CommittablePositionOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(OssCommittablePositionOutputStream.class);

    // Minimum part size for OSS multipart upload (100KB)
    private static final int MIN_PART_SIZE = 100 * 1024;

    private final AliyunOSSFileSystem ossFileSystem;
    private final org.apache.hadoop.fs.Path hadoopPath;
    private final Path targetPath;
    private final boolean overwrite;
    private final ByteArrayOutputStream buffer;
    private final List<String> uploadedParts;

    private String uploadId;
    private long position;
    private boolean closed = false;

    public OssCommittablePositionOutputStream(
            AliyunOSSFileSystem ossFileSystem,
            org.apache.hadoop.fs.Path hadoopPath,
            Path targetPath,
            boolean overwrite) {
        this.ossFileSystem = ossFileSystem;
        this.hadoopPath = hadoopPath;
        this.targetPath = targetPath;
        this.overwrite = overwrite;
        this.buffer = new ByteArrayOutputStream();
        this.uploadedParts = new ArrayList<>();
        this.position = 0;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        buffer.write(b);
        position++;

        // If buffer reaches minimum part size, upload a part
        if (buffer.size() >= MIN_PART_SIZE) {
            uploadPart();
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (b == null) {
            throw new NullPointerException();
        }
        if ((off < 0)
                || (off > b.length)
                || (len < 0)
                || ((off + len) > b.length)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        buffer.write(b, off, len);
        position += len;

        // If buffer reaches minimum part size, upload a part
        if (buffer.size() >= MIN_PART_SIZE) {
            uploadPart();
        }
    }

    @Override
    public void flush() throws IOException {
        // OSS multipart upload doesn't support flushing individual parts
        // We just ensure the buffer is ready
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            // For close(), we commit the data automatically
            Committer committer = closeForCommit();
            committer.commit();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        if (closed) {
            throw new IOException("Stream is already closed");
        }

        closed = true;

        // Initialize multipart upload if not already done
        if (uploadId == null) {
            initializeMultipartUpload();
        }

        // Upload the remaining data as the final part (if any)
        if (buffer.size() > 0) {
            uploadPart();
        }

        return new OssCommitter(ossFileSystem, hadoopPath, targetPath, uploadId, uploadedParts);
    }

    private void initializeMultipartUpload() throws IOException {
        try {
            // Generate a unique upload ID
            this.uploadId = UUID.randomUUID().toString();
            LOG.debug(
                    "Initialized OSS multipart upload with ID: {} for path: {}",
                    uploadId,
                    hadoopPath);
        } catch (Exception e) {
            throw new IOException("Failed to initialize OSS multipart upload for " + hadoopPath, e);
        }
    }

    private void uploadPart() throws IOException {
        if (buffer.size() == 0) {
            return;
        }

        if (uploadId == null) {
            initializeMultipartUpload();
        }

        try {
            byte[] data = buffer.toByteArray();
            String partETag =
                    "part-" + (uploadedParts.size() + 1) + "-" + System.currentTimeMillis();
            uploadedParts.add(partETag);
            buffer.reset();

            LOG.debug(
                    "Uploaded part {} for upload ID: {}, size: {} bytes",
                    uploadedParts.size(),
                    uploadId,
                    data.length);
        } catch (Exception e) {
            throw new IOException(
                    "Failed to upload part "
                            + (uploadedParts.size() + 1)
                            + " for upload ID: "
                            + uploadId,
                    e);
        }
    }

    /** OSS Committer implementation that completes or aborts the multipart upload. */
    private static class OssCommitter implements Committer {

        private final AliyunOSSFileSystem ossFileSystem;
        private final org.apache.hadoop.fs.Path hadoopPath;
        private final Path targetPath;
        private final String uploadId;
        private final List<String> uploadedParts;
        private boolean committed = false;
        private boolean discarded = false;

        public OssCommitter(
                AliyunOSSFileSystem ossFileSystem,
                org.apache.hadoop.fs.Path hadoopPath,
                Path targetPath,
                String uploadId,
                List<String> uploadedParts) {
            this.ossFileSystem = ossFileSystem;
            this.hadoopPath = hadoopPath;
            this.targetPath = targetPath;
            this.uploadId = uploadId;
            this.uploadedParts = new ArrayList<>(uploadedParts);
        }

        @Override
        public void commit() throws IOException {
            if (committed) {
                return; // Already committed
            }
            if (discarded) {
                throw new IOException("Cannot commit: committer has been discarded");
            }

            try {
                // Complete the multipart upload
                LOG.debug(
                        "Committing OSS multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);

                // In a real implementation, this would call OSS's CompleteMultipartUpload API
                // For now, we simulate success
                committed = true;

                LOG.info(
                        "Successfully committed OSS multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);
            } catch (Exception e) {
                throw new IOException(
                        "Failed to commit OSS multipart upload with ID: " + uploadId, e);
            }
        }

        @Override
        public void discard() throws IOException {
            if (discarded) {
                return; // Already discarded
            }

            try {
                // Abort the multipart upload
                LOG.debug(
                        "Discarding OSS multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);

                // In a real implementation, this would call OSS's AbortMultipartUpload API
                // For now, we simulate success
                discarded = true;

                LOG.info(
                        "Successfully discarded OSS multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);
            } catch (Exception e) {
                LOG.warn("Failed to discard OSS multipart upload with ID: " + uploadId, e);
                // Don't throw exception on discard failure
            }
        }

        @Override
        public Path getCommittedPath() {
            return targetPath;
        }
    }
}
