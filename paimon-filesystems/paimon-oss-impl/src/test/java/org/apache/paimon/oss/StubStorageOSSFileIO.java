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

import org.apache.paimon.fs.Path;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link OSSFileIO} that presigns with the SDK and the credentials provider of its real client,
 * but keeps the objects a presign copies in memory.
 */
public class StubStorageOSSFileIO extends OSSFileIO {

    public static final long SOURCE_SIZE = 4096;

    private final transient Runnable whileCopying;
    private transient OSSClient client;

    /** Runs {@code whileCopying} before each part copy, which then asks for credentials. */
    public StubStorageOSSFileIO(Runnable whileCopying) {
        this.whileCopying = whileCopying;
    }

    @Override
    synchronized OSSClient ossClient(Path path) throws Exception {
        if (client == null) {
            client = new StubStorageClient(super.ossClient(path).getCredentialsProvider());
        }
        return client;
    }

    private class StubStorageClient extends OSSClient {

        private final Map<String, ObjectMetadata> objects = new ConcurrentHashMap<>();
        private final Map<String, ObjectMetadata> uploads = new ConcurrentHashMap<>();

        private StubStorageClient(CredentialsProvider provider) {
            super("https://oss-cn-hangzhou.aliyuncs.com", provider, new ClientConfiguration());
        }

        @Override
        public ObjectMetadata headObject(String bucket, String key) {
            ObjectMetadata metadata = objects.get(key);
            if (metadata != null) {
                return metadata;
            }
            if (key.contains("_bloburl_")) {
                throw new OSSException(
                        "missing", "NoSuchKey", "request", "host", null, null, "HEAD");
            }
            ObjectMetadata source = new ObjectMetadata();
            source.setContentLength(SOURCE_SIZE);
            return source;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest request) {
            ObjectMetadata metadata = request.getObjectMetadata();
            metadata.setContentLength(0);
            uploads.put(request.getKey(), metadata);
            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setUploadId(request.getKey());
            return result;
        }

        @Override
        public UploadPartCopyResult uploadPartCopy(UploadPartCopyRequest request) {
            whileCopying.run();
            // like the SDK signing the copy request
            getCredentialsProvider().getCredentials();
            ObjectMetadata metadata = uploads.get(request.getUploadId());
            metadata.setContentLength(metadata.getContentLength() + request.getPartSize());
            UploadPartCopyResult result = new UploadPartCopyResult();
            result.setPartNumber(request.getPartNumber());
            result.setETag("etag");
            return result;
        }

        @Override
        public CompleteMultipartUploadResult completeMultipartUpload(
                CompleteMultipartUploadRequest request) {
            objects.put(request.getKey(), uploads.remove(request.getUploadId()));
            return new CompleteMultipartUploadResult();
        }
    }
}
