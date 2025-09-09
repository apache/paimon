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

import com.aliyun.oss.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** OSS implementation of CommittablePositionOutputStream using multipart upload. */
public class OssCommittablePositionOutputStream extends CommittablePositionOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(OssCommittablePositionOutputStream.class);

    private static final int MIN_PART_SIZE = 100 * 1024 * 1024;

    private final org.apache.hadoop.fs.Path hadoopPath;
    private final Path targetPath;
    private final ByteArrayOutputStream buffer;
    private final List<PartETag> uploadedParts;
    private final OSSAccessor ossAccessor;

    private String uploadId;
    private long position;
    private boolean closed = false;

    public OssCommittablePositionOutputStream(
            OSSAccessor ossAccessor,
            org.apache.hadoop.fs.Path hadoopPath,
            Path targetPath,
            boolean overwrite) {
        this.ossAccessor = ossAccessor;
        this.hadoopPath = hadoopPath;
        this.targetPath = targetPath;
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
        buffer.write(b, off, len);
        position += len;
        if (buffer.size() >= MIN_PART_SIZE) {
            uploadPart();
        }
    }

    @Override
    public void flush() throws IOException {
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
        if (uploadId == null) {
            initializeMultipartUpload();
        }
        if (buffer.size() > 0) {
            uploadPart();
        }

        return new OssCommitter(ossAccessor, hadoopPath, targetPath, uploadId, uploadedParts);
    }

    private void initializeMultipartUpload() throws IOException {
        try {
            this.uploadId = ossAccessor.startMultipartUpload(ossAccessor.pathToObject(hadoopPath));
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

        File tempFile = null;
        try {
            byte[] data = buffer.toByteArray();
            tempFile = Files.createTempFile("oss-part-" + UUID.randomUUID(), ".tmp").toFile();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.write(data);
                fos.flush();
            }
            PartETag partETag =
                    ossAccessor.uploadPart(
                            tempFile,
                            ossAccessor.pathToObject(hadoopPath),
                            uploadId,
                            uploadedParts.size() + 1);
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
        } finally {
            if (tempFile != null && tempFile.exists()) {
                if (!tempFile.delete()) {
                    LOG.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    private static class OssCommitter implements Committer {

        private final OSSAccessor ossAccessor;
        private final org.apache.hadoop.fs.Path hadoopPath;
        private final Path targetPath;
        private final String uploadId;
        private final List<PartETag> uploadedParts;
        private boolean committed = false;
        private boolean discarded = false;

        public OssCommitter(
                OSSAccessor ossAccessor,
                org.apache.hadoop.fs.Path hadoopPath,
                Path targetPath,
                String uploadId,
                List<PartETag> uploadedParts) {
            this.ossAccessor = ossAccessor;
            this.hadoopPath = hadoopPath;
            this.targetPath = targetPath;
            this.uploadId = uploadId;
            this.uploadedParts = new ArrayList<>(uploadedParts);
        }

        @Override
        public void commit() throws IOException {
            if (committed) {
                return;
            }
            if (discarded) {
                throw new IOException("Cannot commit: committer has been discarded");
            }

            try {
                ossAccessor.completeMultipartUpload(
                        ossAccessor.pathToObject(hadoopPath), uploadId, uploadedParts);
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
                return;
            }

            try {
                ossAccessor.abortMultipartUpload(ossAccessor.pathToObject(hadoopPath), uploadId);
                discarded = true;
                LOG.info(
                        "Successfully discarded OSS multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);
            } catch (Exception e) {
                LOG.warn("Failed to discard OSS multipart upload with ID: {}", uploadId, e);
            }
        }

        @Override
        public Path getCommittedPath() {
            return targetPath;
        }
    }
}
