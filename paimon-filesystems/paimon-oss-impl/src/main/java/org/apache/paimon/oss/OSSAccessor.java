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

import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.PartETag;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.List;

/** Provides the bridging logic between Hadoop's abstract filesystem and Aliyun OSS. */
public class OSSAccessor {

    private AliyunOSSFileSystem fs;
    private AliyunOSSFileSystemStore store;

    public OSSAccessor(AliyunOSSFileSystem fs) {
        this.fs = fs;
        this.store = fs.getStore();
    }

    public String pathToObject(org.apache.hadoop.fs.Path hadoopPath) {
        if (!hadoopPath.isAbsolute()) {
            hadoopPath = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), hadoopPath);
        }

        return hadoopPath.toUri().getPath().substring(1);
    }

    public String startMultipartUpload(String objectName) {
        return store.getUploadId(objectName);
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            String objectName, String uploadId, List<PartETag> partETags) {
        return store.completeMultipartUpload(objectName, uploadId, partETags);
    }

    public void abortMultipartUpload(String objectName, String uploadId) {
        store.abortMultipartUpload(objectName, uploadId);
    }

    public PartETag uploadPart(File file, String objectName, String uploadId, int idx)
            throws IOException {
        return store.uploadPart(file, objectName, uploadId, idx);
    }

    @Nonnull
    public String getScheme() {
        return fs.getScheme();
    }
}
