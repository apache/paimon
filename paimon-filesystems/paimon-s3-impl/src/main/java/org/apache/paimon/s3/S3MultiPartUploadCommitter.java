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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.Path;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
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

    @Override
    protected MultiPartUploadStore<PartETag, CompleteMultipartUploadResult> multiPartUploadStore(
            FileIO fileIO, Path targetPath) throws IOException {
        S3FileIO s3FileIO = (S3FileIO) fileIO;
        org.apache.hadoop.fs.Path hadoopPath = s3FileIO.path(targetPath);
        S3AFileSystem fs = (S3AFileSystem) s3FileIO.getFileSystem(hadoopPath);
        return new S3MultiPartUpload(fs, fs.getConf());
    }
}
