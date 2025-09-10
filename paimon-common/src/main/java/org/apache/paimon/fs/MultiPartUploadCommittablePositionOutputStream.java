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

/** MultiPartUploadCommittablePositionOutputStream. */
public abstract class MultiPartUploadCommittablePositionOutputStream<T, C>
        extends CommittablePositionOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiPartUploadCommittablePositionOutputStream.class);

    private final org.apache.hadoop.fs.Path hadoopPath;
    private final Path targetPath;
    private final ByteArrayOutputStream buffer;
    private final List<T> uploadedParts;
    private final MultiPartUploadStore<T, C> multiPartUploadStore;
    private final String objectName;

    private String uploadId;
    private long position;
    private boolean closed = false;

    public MultiPartUploadCommittablePositionOutputStream(
            MultiPartUploadStore<T, C> multiPartUploadStore,
            org.apache.hadoop.fs.Path hadoopPath,
            Path targetPath,
            boolean overwrite) {
        this.multiPartUploadStore = multiPartUploadStore;
        this.hadoopPath = hadoopPath;
        this.targetPath = targetPath;
        this.buffer = new ByteArrayOutputStream();
        this.uploadedParts = new ArrayList<>();
        this.objectName = multiPartUploadStore.pathToObject(hadoopPath);
        this.position = 0;
    }

    public abstract long partSizeThreshold();

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
        if (buffer.size() >= partSizeThreshold()) {
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
        if (buffer.size() >= partSizeThreshold()) {
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

        return new MultiPartUploadCommitter(
                multiPartUploadStore,
                hadoopPath,
                targetPath,
                uploadId,
                uploadedParts,
                objectName,
                position);
    }

    private void initializeMultipartUpload() throws IOException {
        try {
            this.uploadId = multiPartUploadStore.startMultiPartUpload(objectName);
            LOG.debug(
                    "Initialized multipart upload with ID: {} for path: {}", uploadId, hadoopPath);
        } catch (Exception e) {
            throw new IOException("Failed to initialize multipart upload for " + hadoopPath, e);
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
            tempFile = Files.createTempFile("multi-part-" + UUID.randomUUID(), ".tmp").toFile();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.write(data);
                fos.flush();
            }
            T partETag =
                    multiPartUploadStore.uploadPart(
                            objectName, uploadId, uploadedParts.size() + 1, tempFile, data.length);
            uploadedParts.add(partETag);
            buffer.reset();
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

    private static class MultiPartUploadCommitter<T, C> implements Committer {

        private final MultiPartUploadStore<T, C> multiPartUploadStore;
        private final org.apache.hadoop.fs.Path hadoopPath;
        private final Path targetPath;
        private final String uploadId;
        private final String objectName;
        private final List<T> uploadedParts;
        private final long byteLength;
        private boolean committed = false;
        private boolean discarded = false;

        public MultiPartUploadCommitter(
                MultiPartUploadStore<T, C> multiPartUploadStore,
                org.apache.hadoop.fs.Path hadoopPath,
                Path targetPath,
                String uploadId,
                List<T> uploadedParts,
                String objectName,
                long byteLength) {
            this.multiPartUploadStore = multiPartUploadStore;
            this.hadoopPath = hadoopPath;
            this.targetPath = targetPath;
            this.uploadId = uploadId;
            this.objectName = objectName;
            this.uploadedParts = new ArrayList<>(uploadedParts);
            this.byteLength = byteLength;
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
                multiPartUploadStore.completeMultipartUpload(
                        objectName, uploadId, uploadedParts, byteLength);
                committed = true;
                LOG.info(
                        "Successfully committed multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);
            } catch (Exception e) {
                throw new IOException("Failed to commit multipart upload with ID: " + uploadId, e);
            }
        }

        @Override
        public void discard() throws IOException {
            if (discarded) {
                return;
            }

            try {
                multiPartUploadStore.abortMultipartUpload(objectName, uploadId);
                discarded = true;
                LOG.info(
                        "Successfully discarded multipart upload with ID: {} for path: {}",
                        uploadId,
                        hadoopPath);
            } catch (Exception e) {
                LOG.warn("Failed to discard multipart upload with ID: {}", uploadId, e);
            }
        }

        @Override
        public Path getCommittedPath() {
            return targetPath;
        }
    }
}
