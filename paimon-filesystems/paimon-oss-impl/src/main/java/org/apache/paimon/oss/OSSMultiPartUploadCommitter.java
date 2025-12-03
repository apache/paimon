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

import org.apache.paimon.fs.BaseMultiPartUploadCommitter;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.Path;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;

/** OSS implementation of MultiPartUploadCommitter. */
public class OSSMultiPartUploadCommitter
        extends BaseMultiPartUploadCommitter<PartETag, CompleteMultipartUploadResult> {
    public OSSMultiPartUploadCommitter(
            String uploadId,
            List<PartETag> uploadedParts,
            String objectName,
            long position,
            Path path) {
        super(uploadId, uploadedParts, objectName, position, path);
    }

    @Override
    protected MultiPartUploadStore<PartETag, CompleteMultipartUploadResult> multiPartUploadStore(
            FileIO fileIO, Path targetPath) throws IOException {
        OSSFileIO ossFileIO = (OSSFileIO) fileIO;
        org.apache.hadoop.fs.Path hadoopPath = ossFileIO.path(targetPath);
        FileSystem fs = ossFileIO.getFileSystem(hadoopPath);
        return new OSSMultiPartUpload((org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem) fs);
    }
}
