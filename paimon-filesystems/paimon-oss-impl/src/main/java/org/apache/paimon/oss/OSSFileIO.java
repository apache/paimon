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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ReflectionUtils;
import org.apache.paimon.utils.StringUtils;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.CredentialsProvider;
import com.aliyun.oss.common.comm.ServiceClient;
import com.aliyun.oss.internal.OSSMultipartOperation;
import com.aliyun.oss.internal.OSSObjectOperation;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.CopyObjectResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystemStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;

/** OSS {@link FileIO}. */
public class OSSFileIO extends HadoopCompliantFileIO implements HadoopOptionsProvider {

    private static final long serialVersionUID = 2L;

    private static final Logger LOG = LoggerFactory.getLogger(OSSFileIO.class);

    /**
     * In order to simplify, we make paimon oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.oss` in paimon conf to hadoop conf.
     */
    private static final String[] CONFIG_PREFIXES = {"fs.oss."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_SECOND_LEVEL_DOMAIN_ENABLED = "fs.oss.sld.enabled";
    private static final String OSS_SSE_KMS_KEY_ID = "fs.oss.server-side-encryption-key-id";

    /** OSS-native value for the {@code x-oss-server-side-encryption} header when using a CMK. */
    private static final String SSE_KMS_ALGORITHM = "KMS";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(), OSS_SECURITY_TOKEN);
                }
            };

    /**
     * Cache AliyunOSSFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, AliyunOSSFileSystem> CACHE = new ConcurrentHashMap<>();

    // create a shared config to avoid load properties everytime
    private static final Configuration SHARED_CONFIG = new Configuration();

    private Options hadoopOptions;
    private boolean allowCache = true;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        allowCache = context.options().get(FILE_IO_ALLOW_CACHE);
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = context.options().get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase())) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase());
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            hadoopOptions.get(key));
                }
            }
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        if (!overwrite && this.exists(path)) {
            throw new IOException("File " + path + " already exists.");
        }
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileSystem fs = getFileSystem(hadoopPath);
        return new OssTwoPhaseOutputStream(
                new OSSMultiPartUpload((org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem) fs),
                hadoopPath,
                path);
    }

    public Options hadoopOptions() {
        return hadoopOptions;
    }

    @Override
    public Options hadoopOptions(Path path, String opType) {
        return hadoopOptions;
    }

    @Override
    protected AliyunOSSFileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        Supplier<AliyunOSSFileSystem> supplier =
                () -> {
                    // create config from base config, if initializing a new config, it will
                    // retrieve props from the file, which comes at a high cost
                    Configuration hadoopConf = new Configuration(SHARED_CONFIG);
                    hadoopOptions.toMap().forEach(hadoopConf::set);
                    URI fsUri = path.toUri();
                    if (scheme == null && authority == null) {
                        fsUri = FileSystem.getDefaultUri(hadoopConf);
                    } else if (scheme != null && authority == null) {
                        URI defaultUri = FileSystem.getDefaultUri(hadoopConf);
                        if (scheme.equals(defaultUri.getScheme())
                                && defaultUri.getAuthority() != null) {
                            fsUri = defaultUri;
                        }
                    }

                    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }

                    if (hadoopOptions.getBoolean(OSS_SECOND_LEVEL_DOMAIN_ENABLED, false)) {
                        enableSecondLevelDomain(fs);
                    }

                    String sseKmsKeyId = hadoopOptions.get(OSS_SSE_KMS_KEY_ID);
                    if (sseKmsKeyId != null) {
                        String trimmed = sseKmsKeyId.trim();
                        // Reject what can never be a key id, so config errors fail fast at
                        // init instead of mid-job.
                        if (trimmed.isEmpty()
                                || trimmed.chars().anyMatch(Character::isWhitespace)) {
                            throw new IllegalArgumentException(
                                    "Invalid value for '"
                                            + OSS_SSE_KMS_KEY_ID
                                            + "': the CMK key id must not be blank or contain"
                                            + " whitespace/newlines.");
                        }
                        enableSseKms(fs, trimmed);
                    }

                    return fs;
                };

        if (allowCache) {
            return CACHE.computeIfAbsent(
                    new CacheKey(hadoopOptions, scheme, authority), key -> supplier.get());
        } else {
            return supplier.get();
        }
    }

    @Override
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        URI uri = path.toUri();
        String bucket = uri.getHost();
        String objectKey = uri.getPath().substring(1);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setHeader("x-oss-forbid-overwrite", "true");

        String sseAlgorithm =
                hadoopOptions.getString("fs.oss.server-side-encryption-algorithm", "");
        if (StringUtils.isNotEmpty(sseAlgorithm)) {
            metadata.setServerSideEncryption(sseAlgorithm);
        }

        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(path(path));
        try {
            OSSClient ossClient = getOssClient(fs);
            ossClient.putObject(bucket, objectKey, new ByteArrayInputStream(bytes), metadata);
            return true;
        } catch (OSSException e) {
            if ("FileAlreadyExists".equals(e.getErrorCode())) {
                LOG.warn("Failed to atomic write {}: object already exists", path);
                return false;
            }
            throw new IOException("Failed to atomic write " + path, e);
        } catch (Exception e) {
            throw new IOException("Failed to atomic write " + path, e);
        }
    }

    @Override
    public void close() {
        if (!allowCache) {
            fsMap.values().forEach(IOUtils::closeQuietly);
            fsMap.clear();
        }
    }

    public void enableSecondLevelDomain(AliyunOSSFileSystem fs) {
        try {
            OSSClient ossClient = getOssClient(fs);
            ServiceClient serviceClient =
                    ReflectionUtils.getPrivateFieldValue(ossClient, "serviceClient");
            serviceClient.getClientConfiguration().setSLDEnabled(true);
        } catch (Exception e) {
            LOG.error("Failed to enable second level domain.", e);
            throw new RuntimeException("Failed to enable second level domain.", e);
        }
    }

    /** Reflectively extract the underlying {@link OSSClient} that hadoop-aliyun keeps private. */
    private static OSSClient getOssClient(AliyunOSSFileSystem fs) throws Exception {
        AliyunOSSFileSystemStore store = fs.getStore();
        return ReflectionUtils.getPrivateFieldValue(store, "ossClient");
    }

    /**
     * Enable SSE-KMS with a customer CMK. hadoop-aliyun exposes no CMK key-id option, so swap the
     * OSS client's object/multipart operations for subclasses that stamp the OSS-native SSE-KMS
     * headers on the write paths: PutObject, server-side CopyObject (rename/commit) and
     * InitiateMultipartUpload. UploadPart/HeadObject/GetObject are left untouched (stamping SSE
     * headers on those makes OSS reject the request with 400).
     */
    private void enableSseKms(AliyunOSSFileSystem fs, String kmsKeyId) {
        try {
            swapSseKmsOperations(getOssClient(fs), kmsKeyId);
        } catch (Exception e) {
            LOG.error("Failed to enable SSE-KMS BYOK.", e);
            throw new RuntimeException("Failed to enable SSE-KMS BYOK.", e);
        }
    }

    /**
     * Swap the OSS client's object/multipart operations for the CMK-stamping subclasses. Package
     * private so a unit test can pin the reflected field names and getters against the bundled
     * aliyun-oss-sdk version.
     */
    static void swapSseKmsOperations(OSSClient ossClient, String kmsKeyId) throws Exception {
        ServiceClient serviceClient =
                ReflectionUtils.getPrivateFieldValue(ossClient, "serviceClient");
        CredentialsProvider credsProvider =
                ReflectionUtils.getPrivateFieldValue(ossClient, "credsProvider");
        // Replacement operations must inherit the endpoint or requests NPE.
        URI endpoint = ossClient.getEndpoint();
        SseKmsObjectOperation objectOperation =
                new SseKmsObjectOperation(serviceClient, credsProvider, kmsKeyId);
        objectOperation.setEndpoint(endpoint);
        SseKmsMultipartOperation multipartOperation =
                new SseKmsMultipartOperation(serviceClient, credsProvider, kmsKeyId);
        multipartOperation.setEndpoint(endpoint);
        ReflectionUtils.setPrivateFieldValue(ossClient, "objectOperation", objectOperation);
        ReflectionUtils.setPrivateFieldValue(ossClient, "multipartOperation", multipartOperation);
        // Fail closed: if a future SDK change makes the swap a no-op, fail loudly
        // rather than silently writing objects without the customer CMK.
        if (ossClient.getObjectOperation() != objectOperation
                || ossClient.getMultipartOperation() != multipartOperation) {
            throw new IllegalStateException(
                    "SSE-KMS BYOK operation swap did not take effect; refusing to write "
                            + "without server-side encryption. The aliyun-oss-sdk internals "
                            + "may have changed.");
        }
    }

    /** Stamps the customer CMK on the given metadata, allocating one if absent. */
    private static ObjectMetadata applySseKms(ObjectMetadata metadata, String kmsKeyId) {
        if (metadata == null) {
            metadata = new ObjectMetadata();
        }
        String existing = metadata.getServerSideEncryption();
        if (existing != null && !SSE_KMS_ALGORITHM.equals(existing)) {
            LOG.warn(
                    "Customer CMK SSE-KMS is overriding the previously-set server-side "
                            + "encryption algorithm '{}' with 'KMS'.",
                    existing);
        }
        metadata.setServerSideEncryption(SSE_KMS_ALGORITHM);
        metadata.setServerSideEncryptionKeyId(kmsKeyId);
        return metadata;
    }

    /** Stamps the customer CMK on every simple PutObject. */
    static class SseKmsObjectOperation extends OSSObjectOperation {
        private final String kmsKeyId;

        SseKmsObjectOperation(
                ServiceClient serviceClient, CredentialsProvider credsProvider, String kmsKeyId) {
            super(serviceClient, credsProvider);
            this.kmsKeyId = kmsKeyId;
        }

        @Override
        public PutObjectResult putObject(PutObjectRequest request) {
            request.setMetadata(applySseKms(request.getMetadata(), kmsKeyId));
            return super.putObject(request);
        }

        @Override
        public CopyObjectResult copyObject(CopyObjectRequest request) {
            // Rename/commit uses server-side copy. populateCopyObjectHeaders reads the SSE
            // headers straight from these request fields, so setting them here stamps the CMK
            // without touching newObjectMetadata. Setting newObjectMetadata would force
            // metadata-directive REPLACE and drop the source object's Content-Type/user metadata.
            request.setServerSideEncryption(SSE_KMS_ALGORITHM);
            request.setServerSideEncryptionKeyId(kmsKeyId);
            return super.copyObject(request);
        }
    }

    /** Stamps the customer CMK on the multipart-upload init; parts inherit it. */
    static class SseKmsMultipartOperation extends OSSMultipartOperation {
        private final String kmsKeyId;

        SseKmsMultipartOperation(
                ServiceClient serviceClient, CredentialsProvider credsProvider, String kmsKeyId) {
            super(serviceClient, credsProvider);
            this.kmsKeyId = kmsKeyId;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest request) {
            request.setObjectMetadata(applySseKms(request.getObjectMetadata(), kmsKeyId));
            return super.initiateMultipartUpload(request);
        }
    }

    private static class CacheKey {

        private final Options options;
        private final String scheme;
        private final String authority;

        private CacheKey(Options options, String scheme, String authority) {
            this.options = options;
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(options, cacheKey.options)
                    && Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(options, scheme, authority);
        }
    }
}
