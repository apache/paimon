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

import org.apache.paimon.fs.CommittablePositionOutputStream;
import org.apache.paimon.fs.Path;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartResult;
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

/** S3 implementation of CommittablePositionOutputStream using multipart upload. */
public class S3CommittablePositionOutputStream extends CommittablePositionOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(S3CommittablePositionOutputStream.class);

    private static final int MIN_PART_SIZE = 5 * 1024 * 1024;

    private final S3Accessor s3Accessor;
    private final org.apache.hadoop.fs.Path hadoopPath;
    private final Path targetPath;
    private final ByteArrayOutputStream buffer;
    private final List<PartETag> uploadedParts;
    private final String uploadId;
    private final String objectName;

    private long position;
    private boolean closed = false;

    public S3CommittablePositionOutputStream(
            S3Accessor s3Accessor,
            org.apache.hadoop.fs.Path hadoopPath,
            Path targetPath,
            boolean overwrite) {
        this.s3Accessor = s3Accessor;
        this.hadoopPath = hadoopPath;
        this.targetPath = targetPath;
        this.buffer = new ByteArrayOutputStream();
        this.uploadedParts = new ArrayList<>();
        this.position = 0;
        this.objectName = s3Accessor.pathToObject(hadoopPath);
        try {
            this.uploadId = s3Accessor.startMultiPartUpload(this.objectName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        // If buffer reaches minimum part size, upload a part
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
        if (buffer.size() > 0) {
            uploadPart();
        }

        return new S3Committer(
                s3Accessor, targetPath, objectName, uploadId, uploadedParts, position);
    }

    private void uploadPart() throws IOException {
        if (buffer.size() == 0) {
            return;
        }
        File tempFile = null;
        try {
            byte[] data = buffer.toByteArray();
            tempFile = Files.createTempFile("s3-part-" + UUID.randomUUID(), ".tmp").toFile();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                fos.write(data);
                fos.flush();
            }
            UploadPartResult result =
                    s3Accessor.uploadPart(
                            s3Accessor.pathToObject(hadoopPath),
                            uploadId,
                            uploadedParts.size() + 1,
                            tempFile,
                            data.length);
            uploadedParts.add(result.getPartETag());
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

    /** S3 Committer implementation that completes or aborts the multipart upload. */
    private static class S3Committer implements Committer {

        private final S3Accessor s3Accessor;
        private final Path targetPath;
        private final String objectName;
        private final String uploadId;
        private final List<PartETag> uploadedParts;
        private boolean committed = false;
        private boolean discarded = false;
        private long numBytesInParts;

        public S3Committer(
                S3Accessor s3Accessor,
                Path targetPath,
                String objectName,
                String uploadId,
                List<PartETag> uploadedParts,
                long numBytesInParts) {
            this.targetPath = targetPath;
            this.s3Accessor = s3Accessor;
            this.objectName = objectName;
            this.uploadId = uploadId;
            this.uploadedParts = new ArrayList<>(uploadedParts);
            this.numBytesInParts = numBytesInParts;
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
                s3Accessor.commitMultiPartUpload(
                        objectName, uploadId, uploadedParts, numBytesInParts, null);
                committed = true;
                LOG.info(
                        "Successfully committed S3 multipart upload with ID: {} for object: {}",
                        uploadId,
                        objectName);
            } catch (Exception e) {
                throw new IOException(
                        "Failed to commit S3 multipart upload with ID: " + uploadId, e);
            }
        }

        @Override
        public void discard() throws IOException {
            if (discarded) {
                return;
            }

            try {
                s3Accessor.abortMultipartUpload(objectName, uploadId);
                discarded = true;

                LOG.info(
                        "Successfully discarded S3 multipart upload with ID: {} for object: {}",
                        uploadId,
                        objectName);
            } catch (Exception e) {
                LOG.warn("Failed to discard S3 multipart upload with ID: " + uploadId, e);
            }
        }

        @Override
        public Path getCommittedPath() {
            return targetPath;
        }
    }
}
