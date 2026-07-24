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
import org.apache.paimon.utils.BlobDescriptorUtils;

import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.UploadPartCopyRequest;
import com.aliyun.oss.model.UploadPartCopyResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/** Materializes blob ranges and creates OSS presigned URLs. */
public final class OSSBlobPresigner {

    private static final String BLOB_FINGERPRINT_METADATA = "paimon-blob-descriptor-sha256";
    private static final long BLOB_COPY_MIN_PART_SIZE = 100L * 1024 * 1024;
    private static final long MAX_MULTIPART_UPLOAD_PARTS = 10_000;
    private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

    private OSSBlobPresigner() {}

    public static String create(
            OSSClient client,
            Path tableRoot,
            BlobDescriptor descriptor,
            String extension,
            Duration validity)
            throws IOException {
        BlobDescriptorUtils.validateTableRoot(tableRoot, descriptor);
        if (validity == null
                || validity.isZero()
                || validity.isNegative()
                || validity.getNano() != 0) {
            throw new IOException("Blob presigned URL validity must be positive whole seconds.");
        }

        String normalizedExtension = normalizeExtension(extension);
        String contentType = contentType(normalizedExtension);
        try {
            URI source = new Path(descriptor.uri()).toUri();
            String bucket = source.getAuthority();
            String sourceKey = objectKey(source);
            String fingerprint = sha256Hex(descriptor.serialize());
            int parentEnd = sourceKey.lastIndexOf('/') + 1;
            String targetKey =
                    sourceKey.substring(0, parentEnd)
                            + "_bloburl_"
                            + fingerprint
                            + "."
                            + normalizedExtension;

            ObjectMetadata target = headObjectIfExists(client, bucket, targetKey);
            if (!matches(target, descriptor.length(), contentType, fingerprint)) {
                ObjectMetadata sourceMetadata = client.headObject(bucket, sourceKey);
                validateRange(descriptor, sourceMetadata.getContentLength());
                materialize(
                        client, bucket, sourceKey, targetKey, descriptor, contentType, fingerprint);
                target = client.headObject(bucket, targetKey);
                if (!matches(target, descriptor.length(), contentType, fingerprint)) {
                    throw new IOException(
                            "Materialized blob object metadata does not match descriptor.");
                }
            }

            URL url =
                    client.generatePresignedUrl(
                            bucket,
                            targetKey,
                            Date.from(Instant.now().plus(validity)),
                            HttpMethod.GET);
            validatePresignedUrl(client, url, bucket, targetKey);
            return url.toString();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to create blob presigned URL.", e);
        }
    }

    private static void materialize(
            OSSClient client,
            String bucket,
            String sourceKey,
            String targetKey,
            BlobDescriptor descriptor,
            String contentType,
            String fingerprint)
            throws Exception {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(contentType);
        metadata.addUserMetadata(BLOB_FINGERPRINT_METADATA, fingerprint);
        if (descriptor.length() == 0) {
            metadata.setContentLength(0);
            client.putObject(bucket, targetKey, new ByteArrayInputStream(new byte[0]), metadata);
            return;
        }

        String uploadId = null;
        try {
            InitiateMultipartUploadResult initiated =
                    client.initiateMultipartUpload(
                            new InitiateMultipartUploadRequest(bucket, targetKey, metadata));
            uploadId = initiated.getUploadId();
            long partSize =
                    Math.max(
                            BLOB_COPY_MIN_PART_SIZE,
                            descriptor.length() / MAX_MULTIPART_UPLOAD_PARTS + 1);
            List<PartETag> parts = new ArrayList<>();
            long copied = 0;
            int partNumber = 1;
            while (copied < descriptor.length()) {
                long size = Math.min(partSize, descriptor.length() - copied);
                UploadPartCopyResult result =
                        client.uploadPartCopy(
                                new UploadPartCopyRequest(
                                        bucket,
                                        sourceKey,
                                        bucket,
                                        targetKey,
                                        uploadId,
                                        partNumber,
                                        descriptor.offset() + copied,
                                        size));
                parts.add(new PartETag(partNumber, result.getETag()));
                copied += size;
                partNumber++;
            }
            client.completeMultipartUpload(
                    new CompleteMultipartUploadRequest(bucket, targetKey, uploadId, parts));
            uploadId = null;
        } catch (Exception e) {
            if (uploadId != null) {
                try {
                    client.abortMultipartUpload(
                            new AbortMultipartUploadRequest(bucket, targetKey, uploadId));
                } catch (Exception abortException) {
                    e.addSuppressed(abortException);
                }
            }
            throw e;
        }
    }

    private static ObjectMetadata headObjectIfExists(OSSClient client, String bucket, String key) {
        try {
            return client.headObject(bucket, key);
        } catch (OSSException e) {
            if ("NoSuchKey".equals(e.getErrorCode()) || "NoSuchObject".equals(e.getErrorCode())) {
                return null;
            }
            throw e;
        }
    }

    private static boolean matches(
            ObjectMetadata metadata, long length, String contentType, String fingerprint) {
        return metadata != null
                && metadata.getContentLength() == length
                && contentType.equals(metadata.getContentType())
                && fingerprint.equals(metadata.getUserMetadata().get(BLOB_FINGERPRINT_METADATA));
    }

    private static void validateRange(BlobDescriptor descriptor, long sourceLength)
            throws IOException {
        if (descriptor.offset() < 0
                || descriptor.length() < 0
                || descriptor.offset() > sourceLength
                || descriptor.length() > sourceLength - descriptor.offset()) {
            throw new IOException("Blob descriptor range is outside the source object.");
        }
    }

    private static String normalizeExtension(String extension) throws IOException {
        if (extension == null || extension.isEmpty()) {
            throw new IOException("Blob file extension must contain only ASCII letters or digits.");
        }
        for (int i = 0; i < extension.length(); i++) {
            char c = extension.charAt(i);
            if (!(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9')) {
                throw new IOException(
                        "Blob file extension must contain only ASCII letters or digits.");
            }
        }
        return extension.toLowerCase(Locale.ROOT);
    }

    private static String contentType(String extension) throws IOException {
        String contentType = URLConnection.guessContentTypeFromName("file." + extension);
        if (contentType == null || "application/octet-stream".equals(contentType)) {
            throw new IOException("Unknown MIME type for blob file extension: " + extension);
        }
        return contentType;
    }

    private static String objectKey(URI uri) throws IOException {
        String path = uri.getPath();
        if (path == null || !path.startsWith("/") || path.length() == 1) {
            throw new IOException("Blob descriptor URI must contain an OSS object key.");
        }
        return path.substring(1);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            char[] chars = new char[hash.length * 2];
            for (int i = 0; i < hash.length; i++) {
                chars[i * 2] = HEX_CHARS[(hash[i] >> 4) & 0x0f];
                chars[i * 2 + 1] = HEX_CHARS[hash[i] & 0x0f];
            }
            return new String(chars);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available.", e);
        }
    }

    private static void validatePresignedUrl(
            OSSClient client, URL url, String bucket, String targetKey) throws Exception {
        URI endpoint = client.getEndpoint();
        String expectedHost = bucket + "." + endpoint.getHost();
        if (!"https".equalsIgnoreCase(endpoint.getScheme())
                || !"https".equalsIgnoreCase(url.getProtocol())
                || !expectedHost.equalsIgnoreCase(url.getHost())
                || !("/" + targetKey).equals(url.toURI().getPath())) {
            throw new IOException("OSS client generated a presigned URL for an invalid target.");
        }
    }
}
