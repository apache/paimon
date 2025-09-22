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

import org.apache.paimon.fs.MultiPartUploadStore;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Provides the multipart upload by Amazon S3. */
public class S3MultiPartUpload
        implements MultiPartUploadStore<PartETag, CompleteMultipartUploadResult> {

    private final S3AFileSystem s3a;

    private final InternalWriteOperationHelper s3accessHelper;

    public S3MultiPartUpload(S3AFileSystem s3a, Configuration conf) {
        checkNotNull(s3a);
        this.s3accessHelper =
                new InternalWriteOperationHelper(
                        s3a,
                        checkNotNull(conf),
                        s3a.createStoreContext().getInstrumentation(),
                        s3a.getAuditSpanSource(),
                        s3a.getActiveAuditSpan());
        this.s3a = s3a;
    }

    @Override
    public Path workingDirectory() {
        return s3a.getWorkingDirectory();
    }

    @Override
    public String startMultiPartUpload(String objectName) throws IOException {
        return s3accessHelper.initiateMultiPartUpload(objectName);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(
            String objectName, String uploadId, List<PartETag> partETags, long numBytesInParts)
            throws IOException {
        return s3accessHelper.completeMPUwithRetries(
                objectName, uploadId, partETags, numBytesInParts, new AtomicInteger(0));
    }

    @Override
    public PartETag uploadPart(
            String objectName, String uploadId, int partNumber, File file, long byteLength)
            throws IOException {
        final UploadPartRequest uploadRequest =
                s3accessHelper.newUploadPartRequest(
                        objectName,
                        uploadId,
                        partNumber,
                        checkedDownCast(byteLength),
                        null,
                        file,
                        0L);
        return s3accessHelper.uploadPart(uploadRequest).getPartETag();
    }

    @Override
    public void abortMultipartUpload(String destKey, String uploadId) throws IOException {
        s3accessHelper.abortMultipartUpload(destKey, uploadId, false, null);
    }

    private static final class InternalWriteOperationHelper extends WriteOperationHelper {

        InternalWriteOperationHelper(
                S3AFileSystem owner,
                Configuration conf,
                S3AStatisticsContext statisticsContext,
                AuditSpanSource auditSpanSource,
                AuditSpan auditSpan) {
            super(owner, conf, statisticsContext, auditSpanSource, auditSpan);
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
