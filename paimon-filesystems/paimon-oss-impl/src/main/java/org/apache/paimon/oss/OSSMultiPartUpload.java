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

import org.apache.paimon.fs.MultiPartUploadStore;

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** Provides the multipart upload by Aliyun OSS. */
public class OSSMultiPartUpload
        implements MultiPartUploadStore<PartETag, CompleteMultipartUploadResult> {

    private AliyunOSSFileSystem fs;
    private AliyunOSSFileSystemStore store;

    public OSSMultiPartUpload(AliyunOSSFileSystem fs) {
        this.fs = fs;
        this.store = fs.getStore();
    }

    @Override
    public Path workingDirectory() {
        return fs.getWorkingDirectory();
    }

    @Override
    public String startMultiPartUpload(String objectName) throws IOException {
        return store.getUploadId(objectName);
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(
            String objectName, String uploadId, List<PartETag> partETags, long numBytesInParts) {
        return store.completeMultipartUpload(objectName, uploadId, partETags);
    }

    @Override
    public PartETag uploadPart(
            String objectName, String uploadId, int partNumber, File file, int byteLength)
            throws IOException {
        return store.uploadPart(file, objectName, uploadId, partNumber);
    }

    @Override
    public void abortMultipartUpload(String objectName, String uploadId) {
        store.abortMultipartUpload(objectName, uploadId);
    }
}
