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

/** According to multipart upload to support two phase commit. */
public abstract class MultiPartUploadTwoPhaseOutputStream<T, C> extends TwoPhaseOutputStream {

    private static final Logger LOG =
            LoggerFactory.getLogger(MultiPartUploadTwoPhaseOutputStream.class);

    private final ByteArrayOutputStream buffer;
    private final MultiPartUploadStore<T, C> multiPartUploadStore;

    protected final String objectName;
    protected final Path targetPath;
    protected final String uploadId;

    protected List<T> uploadedParts;
    protected long position;

    private boolean closed = false;
    private Committer committer;

    public MultiPartUploadTwoPhaseOutputStream(
            MultiPartUploadStore<T, C> multiPartUploadStore,
            org.apache.hadoop.fs.Path hadoopPath,
            Path path)
            throws IOException {
        this.multiPartUploadStore = multiPartUploadStore;
        this.buffer = new ByteArrayOutputStream();
        this.uploadedParts = new ArrayList<>();
        this.objectName = multiPartUploadStore.pathToObject(hadoopPath);
        this.targetPath = path;
        this.uploadId = multiPartUploadStore.startMultiPartUpload(objectName);
        this.position = 0;
    }

    // OSS limit:  100KB ~ 5GB
    // S3 limit:  5MiB ~ 5GiB
    // Considering memory usage, and referencing Flink's setting of 10MiB.
    public int partSizeThreshold() {
        return 10 << 20;
    }

    public abstract Committer committer();

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
        uploadPartIfLargerThanThreshold();
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
        int remaining = len;
        int offset = off;
        while (remaining > 0) {
            uploadPartIfLargerThanThreshold();
            int currentSize = buffer.size();
            int space = partSizeThreshold() - currentSize;
            int count = Math.min(remaining, space);
            buffer.write(b, offset, count);
            offset += count;
            remaining -= count;
            position += count;
            uploadPartIfLargerThanThreshold();
        }
    }

    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        uploadPartIfLargerThanThreshold();
    }

    @Override
    public void close() throws IOException {
        if (!closed && this.committer == null) {
            this.committer = closeForCommit();
        }
    }

    @Override
    public Committer closeForCommit() throws IOException {
        if (closed && this.committer != null) {
            return this.committer;
        } else if (closed) {
            throw new IOException("Stream is already closed but committer is null");
        }
        closed = true;
        // Only last upload part can be smaller than part size threshold
        uploadPartUtil();
        return committer();
    }

    private void uploadPartIfLargerThanThreshold() throws IOException {
        if (buffer.size() >= partSizeThreshold()) {
            uploadPartUtil();
        }
    }

    private void uploadPartUtil() throws IOException {
        if (buffer.size() == 0) {
            return;
        }

        File tempFile = null;
        int partNumber = uploadedParts.size() + 1;
        try {
            tempFile = Files.createTempFile("multi-part-" + UUID.randomUUID(), ".tmp").toFile();
            try (FileOutputStream fos = new FileOutputStream(tempFile)) {
                buffer.writeTo(fos);
                fos.flush();
            }
            T partETag =
                    multiPartUploadStore.uploadPart(
                            objectName,
                            uploadId,
                            partNumber,
                            tempFile,
                            checkedDownCast(tempFile.length()));
            uploadedParts.add(partETag);
            buffer.reset();
        } catch (Exception e) {
            throw new IOException(
                    "Failed to upload part " + partNumber + " for upload ID: " + uploadId, e);
        } finally {
            if (tempFile != null && tempFile.exists()) {
                if (!tempFile.delete()) {
                    LOG.warn("Failed to delete temporary file: {}", tempFile.getAbsolutePath());
                }
            }
        }
    }

    private static int checkedDownCast(long value) {
        int downCast = (int) value;
        if (downCast != value) {
            throw new IllegalArgumentException(
                    "Cannot downcast long value " + value + " to integer.");
        }
        return downCast;
    }
}
