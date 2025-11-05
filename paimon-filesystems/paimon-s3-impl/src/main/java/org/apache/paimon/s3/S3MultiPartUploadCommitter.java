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

import org.apache.paimon.fs.BaseMultiPartUploadCommitter;
import org.apache.paimon.fs.Path;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;

import java.util.List;

/** S3 implementation of MultiPartUploadCommitter. */
public class S3MultiPartUploadCommitter
        extends BaseMultiPartUploadCommitter<PartETag, CompleteMultipartUploadResult> {
    public S3MultiPartUploadCommitter(
            String uploadId,
            List<PartETag> uploadedParts,
            String objectName,
            long position,
            Path targetPath) {
        super(uploadId, uploadedParts, objectName, position, targetPath);
    }
}
