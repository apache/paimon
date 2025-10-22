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
    private boolean committed = false;
    private boolean discarded = false;

    public BaseMultiPartUploadCommitter(
            String uploadId, List<T> uploadedParts, String objectName, long byteLength) {
        this.uploadId = uploadId;
        this.objectName = objectName;
        this.uploadedParts = new ArrayList<>(uploadedParts);
        this.byteLength = byteLength;
    }

    protected abstract MultiPartUploadStore<T, C> multiPartUploadStore(
            FileIO fileIO, String targetPath) throws IOException;

    @Override
    public void commit(FileIO fileIO) throws IOException {
        if (committed) {
            return;
        }
        if (discarded) {
            throw new IOException("Cannot commit: committer has been discarded");
        }

        try {
            MultiPartUploadStore<T, C> multiPartUploadStore =
                    multiPartUploadStore(fileIO, objectName);
            multiPartUploadStore.completeMultipartUpload(
                    objectName, uploadId, uploadedParts, byteLength);
            committed = true;
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
        if (discarded) {
            return;
        }

        try {
            MultiPartUploadStore<T, C> multiPartUploadStore =
                    multiPartUploadStore(fileIO, objectName);
            multiPartUploadStore.abortMultipartUpload(objectName, uploadId);
            discarded = true;
            LOG.info(
                    "Successfully discarded multipart upload with ID: {} for objectName: {}",
                    uploadId,
                    objectName);
        } catch (Exception e) {
            LOG.warn("Failed to discard multipart upload with ID: {}", uploadId, e);
        }
    }

    @Override
    public String targetFilePath() {
        return objectName;
    }
}
