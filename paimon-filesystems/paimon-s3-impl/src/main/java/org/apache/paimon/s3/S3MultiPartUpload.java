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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.MultiPartUploadStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Provides the multipart upload by Amazon S3 using Hadoop 3.4+ API (AWS SDK v2). */
public class S3MultiPartUpload
        implements MultiPartUploadStore<CompletedPart, CompleteMultipartUploadResponse> {

    private final S3AFileSystem s3a;
    private final WriteOperationHelper s3accessHelper;

    public S3MultiPartUpload(S3AFileSystem s3a) {
        checkNotNull(s3a);
        this.s3accessHelper = s3a.createWriteOperationHelper(s3a.getActiveAuditSpan());
        this.s3a = s3a;
    }

    public S3MultiPartUpload(S3AFileSystem s3a, Configuration conf) {
        this(s3a);
        checkNotNull(conf);
    }

    @Override
    public Path workingDirectory() {
        return s3a.getWorkingDirectory();
    }

    @Override
    public String startMultiPartUpload(String objectName) throws IOException {
        return s3accessHelper.initiateMultiPartUpload(objectName, PutObjectOptions.keepingDirs());
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(
            String objectName, String uploadId, List<CompletedPart> parts, long numBytesInParts)
            throws IOException {
        return s3accessHelper.completeMPUwithRetries(
                objectName,
                uploadId,
                parts,
                numBytesInParts,
                new AtomicInteger(0),
                PutObjectOptions.keepingDirs());
    }

    @Override
    public CompletedPart uploadPart(
            String objectName, String uploadId, int partNumber, File file, int byteLength)
            throws IOException {
        UploadPartRequest request =
                newUploadPartRequest(objectName, uploadId, partNumber, byteLength);
        RequestBody body = RequestBody.fromBytes(Files.readAllBytes(file.toPath()));
        UploadPartResponse response = s3accessHelper.uploadPart(request, body, null);
        return CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build();
    }

    /**
     * Builds the part request through the S3A request factory, the same way the multipart upload is
     * initiated. Hand-assembling the request would drop the SSE-C encryption parameters, which S3
     * requires on every part when the upload was initiated with a customer-provided key.
     *
     * <p>{@code isLastPart} is always {@code false}: the caller does not know in advance which part
     * is the last one, and the factory only uses the flag to set {@code sdkPartType}, which the
     * hand-assembled request never set either.
     */
    @VisibleForTesting
    UploadPartRequest newUploadPartRequest(
            String objectName, String uploadId, int partNumber, int byteLength) throws IOException {
        return s3accessHelper
                .newUploadPartRequestBuilder(objectName, uploadId, partNumber, false, byteLength)
                .build();
    }

    @Override
    public void abortMultipartUpload(String destKey, String uploadId) throws IOException {
        s3accessHelper.abortMultipartUpload(destKey, uploadId, false, null);
    }
}
