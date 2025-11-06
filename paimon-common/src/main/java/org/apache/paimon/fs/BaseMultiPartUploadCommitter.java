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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Base implementation of MultiPartUploadCommitter. */
public abstract class BaseMultiPartUploadCommitter<T, C> implements TwoPhaseOutputStream.Committer {

    private static final Logger LOG = LoggerFactory.getLogger(BaseMultiPartUploadCommitter.class);

    private static final long serialVersionUID = 1L;

    private final String uploadId;
    private final String objectName;
    private final List<T> uploadedParts;
    private final long byteLength;

    public BaseMultiPartUploadCommitter(
            String uploadId, List<T> uploadedParts, String objectName, long byteLength) {
        this.uploadId = uploadId;
        this.objectName = objectName;
        this.uploadedParts = new ArrayList<>(uploadedParts);
        this.byteLength = byteLength;
    }

    protected abstract MultiPartUploadStore<T, C> multiPartUploadStore(
            FileIO fileIO, Path targetPath) throws IOException;

    @Override
    public void commit(FileIO fileIO) throws IOException {
        try {
            MultiPartUploadStore<T, C> multiPartUploadStore =
                    multiPartUploadStore(fileIO, targetFilePath());
            multiPartUploadStore.completeMultipartUpload(
                    objectName, uploadId, uploadedParts, byteLength);
            LOG.info(
                    "Successfully committed multipart upload with ID: {} for objectName: {}",
                    uploadId,
                    objectName);
        } catch (Exception e) {
            throw new IOException("Failed to commit multipart upload with ID: " + uploadId, e);
        }
    }

    @Override
    public void discard(FileIO fileIO) throws IOException {
        try {
            MultiPartUploadStore<T, C> multiPartUploadStore =
                    multiPartUploadStore(fileIO, targetFilePath());
            multiPartUploadStore.abortMultipartUpload(objectName, uploadId);
            LOG.info(
                    "Successfully discarded multipart upload with ID: {} for objectName: {}",
                    uploadId,
                    objectName);
        } catch (Exception e) {
            LOG.warn("Failed to discard multipart upload with ID: {}", uploadId, e);
        }
    }

    @Override
    public Path targetFilePath() {
        return new Path(objectName);
    }

    @Override
    public void clean(FileIO fileIO) throws IOException {}
}
