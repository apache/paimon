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

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.ExecutionContext;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.RetryStrategy;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OSSFileIO}. */
public class OSSFileIOTest {

    /** The swap must replace both operations with the SSE-stamping subclasses. */
    @Test
    public void testSseOperationSwapTakesEffect() throws Exception {
        OSSClient ossClient =
                (OSSClient) new OSSClientBuilder().build("http://oss.example.com", "ak", "sk");
        try {
            OSSFileIO.swapSseOperations(ossClient, OSSFileIO.resolveSse("KMS", "my-cmk", "SM4"));

            assertThat(ossClient.getObjectOperation())
                    .isInstanceOf(OSSFileIO.SseObjectOperation.class);
            assertThat(ossClient.getMultipartOperation())
                    .isInstanceOf(OSSFileIO.SseMultipartOperation.class);
        } finally {
            ossClient.shutdown();
        }
    }

    /** applySse must stamp exactly the configured headers onto the metadata. */
    @Test
    public void testApplySseStampsHeaders() {
        ObjectMetadata kms =
                OSSFileIO.applySse(
                        (ObjectMetadata) null, OSSFileIO.resolveSse("KMS", "my-cmk", "SM4"));
        assertThat(kms.getServerSideEncryption()).isEqualTo("KMS");
        assertThat(kms.getServerSideEncryptionKeyId()).isEqualTo("my-cmk");
        assertThat(kms.getServerSideDataEncryption()).isEqualTo("SM4");

        ObjectMetadata aes =
                OSSFileIO.applySse(
                        (ObjectMetadata) null, OSSFileIO.resolveSse("AES256", null, null));
        assertThat(aes.getServerSideEncryption()).isEqualTo("AES256");
        assertThat(aes.getServerSideEncryptionKeyId()).isNull();
        assertThat(aes.getServerSideDataEncryption()).isNull();
    }

    /** applySse must stamp the SSE fields and data-encryption header on a CopyObject request. */
    @Test
    public void testApplySseStampsCopyRequest() {
        CopyObjectRequest kms = new CopyObjectRequest("sb", "sk", "db", "dk");
        OSSFileIO.applySse(kms, OSSFileIO.resolveSse("KMS", "my-cmk", "SM4"));
        assertThat(kms.getServerSideEncryption()).isEqualTo("KMS");
        assertThat(kms.getServerSideEncryptionKeyId()).isEqualTo("my-cmk");
        assertThat(kms.getHeaders()).containsEntry("x-oss-server-side-data-encryption", "SM4");

        CopyObjectRequest aes = new CopyObjectRequest("sb", "sk", "db", "dk");
        OSSFileIO.applySse(aes, OSSFileIO.resolveSse("AES256", null, null));
        assertThat(aes.getServerSideEncryption()).isEqualTo("AES256");
        assertThat(aes.getServerSideEncryptionKeyId()).isNull();
        assertThat(aes.getHeaders()).doesNotContainKey("x-oss-server-side-data-encryption");
    }

    /** copyObject must carry all three SSE headers through to the final signed wire request. */
    @Test
    public void testCopyObjectSendsSseHeadersOnWire() throws Exception {
        AtomicReference<Map<String, String>> wire = new AtomicReference<>();
        ServiceClient capturing =
                new ServiceClient(new ClientConfiguration()) {
                    @Override
                    protected ResponseMessage sendRequestCore(
                            ServiceClient.Request request, ExecutionContext context) {
                        wire.set(new HashMap<>(request.getHeaders()));
                        throw new ClientException("captured");
                    }

                    @Override
                    protected RetryStrategy getDefaultRetryStrategy() {
                        return new RetryStrategy() {
                            @Override
                            public boolean shouldRetry(
                                    Exception e,
                                    RequestMessage request,
                                    ResponseMessage response,
                                    int retries) {
                                return false;
                            }
                        };
                    }

                    @Override
                    public void shutdown() {}
                };

        OSSFileIO.SseObjectOperation operation =
                new OSSFileIO.SseObjectOperation(
                        capturing,
                        new DefaultCredentialProvider("ak", "sk"),
                        OSSFileIO.resolveSse("KMS", "my-cmk", "SM4"));
        operation.setEndpoint(new URI("http://oss.example.com"));

        assertThatThrownBy(
                        () -> operation.copyObject(new CopyObjectRequest("sb", "sk", "db", "dk")))
                .isInstanceOf(ClientException.class);

        assertThat(wire.get())
                .containsEntry("x-oss-server-side-encryption", "KMS")
                .containsEntry("x-oss-server-side-encryption-key-id", "my-cmk")
                .containsEntry("x-oss-server-side-data-encryption", "SM4");
    }

    @Test
    public void testResolveSse() {
        // Nothing set -> no SSE.
        assertThat(OSSFileIO.resolveSse(null, null, null)).isNull();

        // Method only (no key-id / data-encryption).
        OSSFileIO.SseConfig aes = OSSFileIO.resolveSse("AES256", null, null);
        assertThat(aes.method).isEqualTo("AES256");
        assertThat(aes.keyId).isNull();
        assertThat(aes.dataEnc).isNull();

        // A key id / SM4 data-encryption each default the method to KMS.
        assertThat(OSSFileIO.resolveSse(null, "my-cmk", null).method).isEqualTo("KMS");
        OSSFileIO.SseConfig dataOnly = OSSFileIO.resolveSse(null, null, "SM4");
        assertThat(dataOnly.method).isEqualTo("KMS");
        assertThat(dataOnly.dataEnc).isEqualTo("SM4");
        OSSFileIO.SseConfig full = OSSFileIO.resolveSse("KMS", "my-cmk", "SM4");
        assertThat(full.method).isEqualTo("KMS");
        assertThat(full.keyId).isEqualTo("my-cmk");
        assertThat(full.dataEnc).isEqualTo("SM4");

        // The method is canonicalized to the exact OSS value.
        assertThat(OSSFileIO.resolveSse("kms", "my-cmk", "sm4").method).isEqualTo("KMS");
        assertThat(OSSFileIO.resolveSse("kms", "my-cmk", "sm4").dataEnc).isEqualTo("SM4");

        // Invalid / conflicting config is rejected fail-fast.
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES-256", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AES256/KMS/SM4");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES256", "my-cmk", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("AES256", null, "SM4"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("KMS", "my-cmk", "AES256"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SM4");
        assertThatThrownBy(() -> OSSFileIO.resolveSse("KMS", "bad key", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whitespace");

        // A present-but-blank value is a misconfiguration, not "unset".
        assertThatThrownBy(() -> OSSFileIO.resolveSse(null, "  ", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
        assertThatThrownBy(() -> OSSFileIO.resolveSse(" ", "", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }
}
