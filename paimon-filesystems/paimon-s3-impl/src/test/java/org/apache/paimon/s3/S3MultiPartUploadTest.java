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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link UploadPartRequest} built by {@link S3MultiPartUpload}. No S3 backend is
 * contacted: {@code fs.s3a.bucket.probe=0} makes {@link S3AFileSystem#initialize} skip the bucket
 * existence check, so the request factory can be exercised offline.
 */
class S3MultiPartUploadTest {

    private static final String BUCKET = "paimon-test-bucket";
    private static final String OBJECT_NAME = "path/to/object";
    private static final String UPLOAD_ID = "test-upload-id";

    @Test
    void testUploadPartRequestCarriesSseCustomerParameters() throws Exception {
        Configuration conf = baseConfiguration();
        conf.set("fs.s3a.encryption.algorithm", "SSE-C");
        conf.set("fs.s3a.encryption.key", sseCustomerKey());

        UploadPartRequest request = newUploadPartRequest(conf);

        assertThat(request.sseCustomerAlgorithm()).isEqualTo("AES256");
        assertThat(request.sseCustomerKey()).isNotNull();
        assertThat(request.sseCustomerKeyMD5()).isNotNull();
    }

    @Test
    void testUploadPartRequestWithoutEncryption() throws Exception {
        UploadPartRequest request = newUploadPartRequest(baseConfiguration());

        assertThat(request.sseCustomerAlgorithm()).isNull();
        assertThat(request.sseCustomerKey()).isNull();
        assertThat(request.sseCustomerKeyMD5()).isNull();
    }

    @Test
    void testUploadPartRequestKeepsPartCoordinates() throws Exception {
        UploadPartRequest request = newUploadPartRequest(baseConfiguration());

        assertThat(request.bucket()).isEqualTo(BUCKET);
        assertThat(request.key()).isEqualTo(OBJECT_NAME);
        assertThat(request.uploadId()).isEqualTo(UPLOAD_ID);
        assertThat(request.partNumber()).isEqualTo(3);
        assertThat(request.contentLength()).isEqualTo(1024L);
        assertThat(request.sdkPartType()).isNull();
    }

    private static UploadPartRequest newUploadPartRequest(Configuration conf) throws IOException {
        try (S3AFileSystem fs = new S3AFileSystem()) {
            fs.initialize(URI.create("s3a://" + BUCKET + "/"), conf);
            S3MultiPartUpload upload = new S3MultiPartUpload(fs);
            return upload.newUploadPartRequest(OBJECT_NAME, UPLOAD_ID, 3, 1024);
        }
    }

    private static Configuration baseConfiguration() {
        Configuration conf = new Configuration(false);
        // Never reach the network: no bucket probe, dummy credentials and an unroutable endpoint.
        conf.setInt("fs.s3a.bucket.probe", 0);
        conf.set("fs.s3a.endpoint", "http://localhost:1");
        conf.set("fs.s3a.endpoint.region", "us-east-1");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.access.key", "dummy-access-key");
        conf.set("fs.s3a.secret.key", "dummy-secret-key");
        return conf;
    }

    /** A 256-bit key, base64 encoded, as required by {@code fs.s3a.encryption.key} for SSE-C. */
    private static String sseCustomerKey() {
        byte[] key = new byte[32];
        for (int i = 0; i < key.length; i++) {
            key[i] = (byte) i;
        }
        return Base64.getEncoder().encodeToString(key);
    }
}
