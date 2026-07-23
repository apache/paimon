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

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.ExecutionContext;
import com.aliyun.oss.common.comm.RequestMessage;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.common.comm.RetryStrategy;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link OSSFileIO}. */
public class OSSFileIOTest {

    @Test
    public void testCreateBlobPresignedUrlMaterializesSinglePart() throws Exception {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/source.blob", 10, 20);
        String fingerprint = sha256Hex(descriptor.serialize());
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(100);
        ObjectMetadata target = new ObjectMetadata();
        target.setContentLength(20);
        target.setContentType("image/png");
        target.addUserMetadata("paimon-blob-descriptor-sha256", fingerprint);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenThrow(missingObject())
                .thenReturn(target);
        when(client.headObject("bucket", "table/bucket-0/source.blob")).thenReturn(source);
        InitiateMultipartUploadResult initiate = new InitiateMultipartUploadResult();
        initiate.setUploadId("upload-id");
        when(client.initiateMultipartUpload(any())).thenReturn(initiate);
        UploadPartCopyResult copied = new UploadPartCopyResult();
        copied.setPartNumber(1);
        copied.setETag("etag");
        when(client.uploadPartCopy(any())).thenReturn(copied);
        when(client.getEndpoint()).thenReturn(URI.create("https://oss-cn-hangzhou.aliyuncs.com"));
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenAnswer(
                        invocation ->
                                presignedUrl(
                                        "https://bucket.oss-cn-hangzhou.aliyuncs.com/"
                                                + invocation.getArgument(1)
                                                + "?Signature=test"));

        String url =
                new TestOSSFileIO(client)
                        .createBlobPresignedUrl(
                                new Path("oss://bucket/table"),
                                descriptor,
                                "PNG",
                                Duration.ofMinutes(5));

        assertThat(url)
                .matches(
                        "https://bucket\\.oss-cn-hangzhou\\.aliyuncs\\.com/"
                                + "table/bucket-0/_bloburl_[0-9a-f]{64}\\.png\\?Signature=test");
        ArgumentCaptor<InitiateMultipartUploadRequest> initiateCaptor =
                ArgumentCaptor.forClass(InitiateMultipartUploadRequest.class);
        verify(client).initiateMultipartUpload(initiateCaptor.capture());
        assertThat(initiateCaptor.getValue().getObjectMetadata().getContentType())
                .isEqualTo("image/png");
        assertThat(initiateCaptor.getValue().getObjectMetadata().getUserMetadata())
                .containsEntry("paimon-blob-descriptor-sha256", fingerprint);

        ArgumentCaptor<UploadPartCopyRequest> copyCaptor =
                ArgumentCaptor.forClass(UploadPartCopyRequest.class);
        verify(client).uploadPartCopy(copyCaptor.capture());
        UploadPartCopyRequest copy = copyCaptor.getValue();
        assertThat(copy.getSourceBucketName()).isEqualTo("bucket");
        assertThat(copy.getSourceKey()).isEqualTo("table/bucket-0/source.blob");
        assertThat(copy.getBeginIndex()).isEqualTo(10);
        assertThat(copy.getPartSize()).isEqualTo(20);
        assertThat(copy.getPartNumber()).isEqualTo(1);
        verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    public void testCreateBlobPresignedUrlCacheHitOnlyPresigns() throws Exception {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/bucket-0/source.blob", 10, 20);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenReturn(matchingMetadata(descriptor, "image/jpeg"));
        stubPresigning(client);

        String url =
                new TestOSSFileIO(client)
                        .createBlobPresignedUrl(
                                new Path("oss://bucket/table"),
                                descriptor,
                                "jpg",
                                Duration.ofMinutes(5));

        assertThat(url).startsWith("https://bucket.oss-cn-hangzhou.aliyuncs.com/");
        verify(client, never()).headObject("bucket", "table/bucket-0/source.blob");
        verify(client, never()).initiateMultipartUpload(any());
        verify(client, never()).uploadPartCopy(any());
    }

    @Test
    public void testCreateBlobPresignedUrlControlsMultipartCount() throws Exception {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/source.blob", 0, Long.MAX_VALUE);
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(Long.MAX_VALUE);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenThrow(missingObject())
                .thenReturn(matchingMetadata(descriptor, "image/png"));
        when(client.headObject("bucket", "table/source.blob")).thenReturn(source);
        stubMultipartUpload(client);
        stubPresigning(client);

        new TestOSSFileIO(client)
                .createBlobPresignedUrl(
                        new Path("oss://bucket/table"), descriptor, "png", Duration.ofMinutes(5));

        ArgumentCaptor<UploadPartCopyRequest> captor =
                ArgumentCaptor.forClass(UploadPartCopyRequest.class);
        verify(client, times(10_000)).uploadPartCopy(captor.capture());
        assertThat(captor.getAllValues().get(0).getBeginIndex()).isZero();
        assertThat(captor.getAllValues().get(9_999).getPartNumber()).isEqualTo(10_000);
    }

    @Test
    public void testCreateBlobPresignedUrlHandlesZeroLength() throws Exception {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/source.blob", 10, 0);
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(10);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenThrow(missingObject())
                .thenReturn(matchingMetadata(descriptor, "image/png"));
        when(client.headObject("bucket", "table/source.blob")).thenReturn(source);
        stubPresigning(client);

        new TestOSSFileIO(client)
                .createBlobPresignedUrl(
                        new Path("oss://bucket/table"), descriptor, "png", Duration.ofMinutes(5));

        verify(client)
                .putObject(
                        eq("bucket"),
                        contains("_bloburl_"),
                        any(java.io.InputStream.class),
                        any(ObjectMetadata.class));
        verify(client, never()).initiateMultipartUpload(any());
    }

    @Test
    public void testCreateBlobPresignedUrlAbortsFailedUpload() {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/source.blob", 0, 20);
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(20);
        when(client.headObject(eq("bucket"), contains("_bloburl_"))).thenThrow(missingObject());
        when(client.headObject("bucket", "table/source.blob")).thenReturn(source);
        InitiateMultipartUploadResult initiate = new InitiateMultipartUploadResult();
        initiate.setUploadId("upload-id");
        when(client.initiateMultipartUpload(any())).thenReturn(initiate);
        when(client.uploadPartCopy(any())).thenThrow(new ClientException("failed"));

        assertThatThrownBy(
                        () ->
                                new TestOSSFileIO(client)
                                        .createBlobPresignedUrl(
                                                new Path("oss://bucket/table"),
                                                descriptor,
                                                "png",
                                                Duration.ofMinutes(5)))
                .isInstanceOf(java.io.IOException.class);
        ArgumentCaptor<AbortMultipartUploadRequest> captor =
                ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(client).abortMultipartUpload(captor.capture());
        assertThat(captor.getValue().getUploadId()).isEqualTo("upload-id");
    }

    @Test
    public void testCreateBlobPresignedUrlRejectsFinalHeadMismatch() {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/source.blob", 0, 20);
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(20);
        ObjectMetadata wrongTarget = matchingMetadata(descriptor, "image/png");
        wrongTarget.setContentLength(19);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenThrow(missingObject())
                .thenReturn(wrongTarget);
        when(client.headObject("bucket", "table/source.blob")).thenReturn(source);
        stubMultipartUpload(client);

        assertThatThrownBy(
                        () ->
                                new TestOSSFileIO(client)
                                        .createBlobPresignedUrl(
                                                new Path("oss://bucket/table"),
                                                descriptor,
                                                "png",
                                                Duration.ofMinutes(5)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("metadata does not match");
        verify(client, never())
                .generatePresignedUrl(anyString(), anyString(), any(), any(HttpMethod.class));
    }

    @Test
    public void testCreateBlobPresignedUrlRejectsInvalidArguments() {
        OSSClient client = mock(OSSClient.class);
        TestOSSFileIO fileIO = new TestOSSFileIO(client);
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/source.blob", 0, 1);
        Path root = new Path("oss://bucket/table");

        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        root, descriptor, ".png", Duration.ofSeconds(1)))
                .isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        root, descriptor, "unknown", Duration.ofSeconds(1)))
                .isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        root, descriptor, "png", Duration.ofMillis(1)))
                .isInstanceOf(java.io.IOException.class);
        assertThatThrownBy(
                        () ->
                                fileIO.createBlobPresignedUrl(
                                        new Path("oss://bucket/other"),
                                        descriptor,
                                        "png",
                                        Duration.ofSeconds(1)))
                .isInstanceOf(java.io.IOException.class);
    }

    @Test
    public void testCreateBlobPresignedUrlRejectsInvalidRange() {
        assertInvalidRange(-1, 1, 10);
        assertInvalidRange(0, -1, 10);
        assertInvalidRange(9, 2, 10);
        assertInvalidRange(Long.MAX_VALUE, 1, Long.MAX_VALUE);
    }

    @Test
    public void testCreateBlobPresignedUrlRejectsInvalidTarget() {
        assertInvalidPresignedUrl(
                "https://oss-cn-hangzhou.aliyuncs.com",
                "http://bucket.oss-cn-hangzhou.aliyuncs.com/table/_bloburl_hash.png");
        assertInvalidPresignedUrl(
                "https://oss-cn-hangzhou.aliyuncs.com",
                "https://other.oss-cn-hangzhou.aliyuncs.com/table/_bloburl_hash.png");
        assertInvalidPresignedUrl(
                "https://oss-cn-hangzhou.aliyuncs.com",
                "https://bucket.oss-cn-hangzhou.aliyuncs.com/table/other.png");
        assertInvalidPresignedUrl(
                "http://oss-cn-hangzhou.aliyuncs.com",
                "https://bucket.oss-cn-hangzhou.aliyuncs.com/table/_bloburl_hash.png");
    }

    private static void assertInvalidPresignedUrl(String endpoint, String generatedUrl) {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/source.blob", 0, 1);
        when(client.headObject(eq("bucket"), contains("_bloburl_")))
                .thenReturn(matchingMetadata(descriptor, "image/png"));
        when(client.getEndpoint()).thenReturn(URI.create(endpoint));
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenReturn(presignedUrl(generatedUrl));

        assertThatThrownBy(
                        () ->
                                new TestOSSFileIO(client)
                                        .createBlobPresignedUrl(
                                                new Path("oss://bucket/table"),
                                                descriptor,
                                                "png",
                                                Duration.ofMinutes(5)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("invalid target");
    }

    private static void assertInvalidRange(long offset, long length, long sourceLength) {
        OSSClient client = mock(OSSClient.class);
        BlobDescriptor descriptor =
                new BlobDescriptor("oss://bucket/table/source.blob", offset, length);
        ObjectMetadata source = new ObjectMetadata();
        source.setContentLength(sourceLength);
        when(client.headObject(eq("bucket"), contains("_bloburl_"))).thenThrow(missingObject());
        when(client.headObject("bucket", "table/source.blob")).thenReturn(source);

        assertThatThrownBy(
                        () ->
                                new TestOSSFileIO(client)
                                        .createBlobPresignedUrl(
                                                new Path("oss://bucket/table"),
                                                descriptor,
                                                "png",
                                                Duration.ofMinutes(5)))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("range");
        verify(client, never()).initiateMultipartUpload(any());
    }

    private static ObjectMetadata matchingMetadata(BlobDescriptor descriptor, String contentType) {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(descriptor.length());
            metadata.setContentType(contentType);
            metadata.addUserMetadata(
                    "paimon-blob-descriptor-sha256", sha256Hex(descriptor.serialize()));
            return metadata;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static void stubMultipartUpload(OSSClient client) {
        InitiateMultipartUploadResult initiate = new InitiateMultipartUploadResult();
        initiate.setUploadId("upload-id");
        when(client.initiateMultipartUpload(any())).thenReturn(initiate);
        when(client.uploadPartCopy(any()))
                .thenAnswer(
                        invocation -> {
                            UploadPartCopyRequest request = invocation.getArgument(0);
                            UploadPartCopyResult copied = new UploadPartCopyResult();
                            copied.setPartNumber(request.getPartNumber());
                            copied.setETag("etag-" + request.getPartNumber());
                            return copied;
                        });
    }

    private static void stubPresigning(OSSClient client) {
        when(client.getEndpoint()).thenReturn(URI.create("https://oss-cn-hangzhou.aliyuncs.com"));
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenAnswer(
                        invocation ->
                                presignedUrl(
                                        "https://bucket.oss-cn-hangzhou.aliyuncs.com/"
                                                + invocation.getArgument(1)
                                                + "?Signature=test"));
    }

    private static OSSException missingObject() {
        return new OSSException("missing", "NoSuchKey", "request", "host", null, null, "HEAD");
    }

    private static URL presignedUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String sha256Hex(byte[] bytes) throws NoSuchAlgorithmException {
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
        StringBuilder result = new StringBuilder(hash.length * 2);
        for (byte value : hash) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static class TestOSSFileIO extends OSSFileIO {

        private final OSSClient client;

        private TestOSSFileIO(OSSClient client) {
            this.client = client;
        }

        OSSClient ossClient(Path path) {
            return client;
        }
    }

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
