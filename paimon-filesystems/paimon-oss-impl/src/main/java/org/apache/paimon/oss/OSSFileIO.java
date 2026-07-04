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
import com.aliyun.oss.internal.OSSHeaders;
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
import static org.apache.paimon.utils.Preconditions.checkArgument;

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
    // Paimon OSS SSE keys, mapping 1:1 to the OSS headers; they take precedence over hadoop's
    // server-side-encryption-algorithm.
    /** SSE method -> x-oss-server-side-encryption (AES256 / KMS / SM4). */
    private static final String OSS_SSE_METHOD = "fs.oss.server-side-encryption";

    /** CMK key id -> x-oss-server-side-encryption-key-id (valid only when the method is KMS). */
    private static final String OSS_SSE_KMS_KEY_ID = "fs.oss.server-side-encryption-key-id";

    /**
     * Data encryption -> x-oss-server-side-data-encryption (SM4; valid only when method is KMS).
     */
    private static final String OSS_SSE_DATA_ENCRYPTION = "fs.oss.server-side-data-encryption";

    /**
     * hadoop-aliyun's native SSE method key; only used as the {@code tryToWriteAtomic} fallback.
     */
    private static final String OSS_SSE_ALGORITHM = "fs.oss.server-side-encryption-algorithm";

    private static final String SSE_METHOD_AES256 = "AES256";
    private static final String SSE_METHOD_KMS = "KMS";
    private static final String SSE_DATA_SM4 = "SM4";

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

                    SseConfig sse = configuredSse();
                    if (sse != null) {
                        enableSse(fs, sse);
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
        // Fall back to native SSE only when Paimon SSE is unset (the swap stamps it otherwise).
        String sseAlgorithm = hadoopOptions.getString(OSS_SSE_ALGORITHM, "");
        if (StringUtils.isNotEmpty(sseAlgorithm) && configuredSse() == null) {
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

    /** The Paimon SSE config from the three keys, or null when none is set. */
    private SseConfig configuredSse() {
        return resolveSse(
                hadoopOptions.get(OSS_SSE_METHOD),
                hadoopOptions.get(OSS_SSE_KMS_KEY_ID),
                hadoopOptions.get(OSS_SSE_DATA_ENCRYPTION));
    }

    /** Parse the three SSE keys into an {@link SseConfig}, or null. Throws on bad input. */
    static SseConfig resolveSse(String method, String keyId, String dataEncryption) {
        // Reject present-but-blank values fail-fast instead of silently writing plaintext.
        checkNotBlankIfSet(method, OSS_SSE_METHOD);
        checkNotBlankIfSet(keyId, OSS_SSE_KMS_KEY_ID);
        checkNotBlankIfSet(dataEncryption, OSS_SSE_DATA_ENCRYPTION);
        method = method == null ? null : method.trim();
        keyId = keyId == null ? null : keyId.trim();
        dataEncryption = dataEncryption == null ? null : dataEncryption.trim();
        if (method == null && keyId == null && dataEncryption == null) {
            return null;
        }
        // A key id / data-encryption implies KMS; default the method to KMS when unset.
        method = canonicalSseMethod(method == null ? SSE_METHOD_KMS : method);
        if (keyId != null) {
            checkArgument(
                    keyId.chars().noneMatch(Character::isWhitespace),
                    "Invalid value for '%s': the CMK key id must not contain whitespace/newlines.",
                    OSS_SSE_KMS_KEY_ID);
            checkArgument(
                    SSE_METHOD_KMS.equals(method),
                    "'%s' requires '%s=KMS', but got '%s'.",
                    OSS_SSE_KMS_KEY_ID,
                    OSS_SSE_METHOD,
                    method);
        }
        if (dataEncryption != null) {
            checkArgument(
                    SSE_METHOD_KMS.equals(method),
                    "'%s' requires '%s=KMS', but got '%s'.",
                    OSS_SSE_DATA_ENCRYPTION,
                    OSS_SSE_METHOD,
                    method);
            checkArgument(
                    SSE_DATA_SM4.equalsIgnoreCase(dataEncryption),
                    "'%s' only supports 'SM4', but got '%s'.",
                    OSS_SSE_DATA_ENCRYPTION,
                    dataEncryption);
            dataEncryption = SSE_DATA_SM4;
        }
        return new SseConfig(method, keyId, dataEncryption);
    }

    /** Reject a config value that is present but blank (encryption requested yet misconfigured). */
    private static void checkNotBlankIfSet(String value, String key) {
        checkArgument(
                value == null || !StringUtils.isNullOrWhitespaceOnly(value),
                "'%s' is set but blank.",
                key);
    }

    /** Canonicalize the SSE method to the exact OSS header value; reject unknown values. */
    private static String canonicalSseMethod(String method) {
        if (SSE_METHOD_AES256.equalsIgnoreCase(method)) {
            return SSE_METHOD_AES256;
        }
        if (SSE_METHOD_KMS.equalsIgnoreCase(method)) {
            return SSE_METHOD_KMS;
        }
        if (SSE_DATA_SM4.equalsIgnoreCase(method)) {
            return SSE_DATA_SM4;
        }
        throw new IllegalArgumentException(
                "'"
                        + OSS_SSE_METHOD
                        + "' must be one of AES256/KMS/SM4, but got '"
                        + method
                        + "'.");
    }

    /** Resolved OSS server-side-encryption settings. */
    static final class SseConfig {
        final String method;
        final String keyId;
        final String dataEnc;

        SseConfig(String method, String keyId, String dataEnc) {
            this.method = method;
            this.keyId = keyId;
            this.dataEnc = dataEnc;
        }
    }

    /** Swap in the SSE-stamping object/multipart operations (write paths only). */
    private void enableSse(AliyunOSSFileSystem fs, SseConfig sse) {
        try {
            swapSseOperations(getOssClient(fs), sse);
        } catch (Exception e) {
            LOG.error("Failed to enable OSS server-side encryption.", e);
            throw new RuntimeException("Failed to enable OSS server-side encryption.", e);
        }
    }

    /** Swap in the SSE-stamping operations; package-private so a test pins reflected names. */
    static void swapSseOperations(OSSClient ossClient, SseConfig sse) throws Exception {
        ServiceClient serviceClient =
                ReflectionUtils.getPrivateFieldValue(ossClient, "serviceClient");
        CredentialsProvider credsProvider =
                ReflectionUtils.getPrivateFieldValue(ossClient, "credsProvider");
        // Replacement operations must inherit the endpoint or requests NPE.
        URI endpoint = ossClient.getEndpoint();
        SseObjectOperation objectOperation =
                new SseObjectOperation(serviceClient, credsProvider, sse);
        objectOperation.setEndpoint(endpoint);
        SseMultipartOperation multipartOperation =
                new SseMultipartOperation(serviceClient, credsProvider, sse);
        multipartOperation.setEndpoint(endpoint);
        ReflectionUtils.setPrivateFieldValue(ossClient, "objectOperation", objectOperation);
        ReflectionUtils.setPrivateFieldValue(ossClient, "multipartOperation", multipartOperation);
        // Fail closed: if the swap didn't take effect, refuse to write unencrypted.
        if (ossClient.getObjectOperation() != objectOperation
                || ossClient.getMultipartOperation() != multipartOperation) {
            throw new IllegalStateException(
                    "OSS SSE operation swap did not take effect; refusing to write without "
                            + "server-side encryption. The aliyun-oss-sdk internals may have changed.");
        }
    }

    /** Stamp the SSE headers on object metadata (PutObject / multipart init). */
    static ObjectMetadata applySse(ObjectMetadata metadata, SseConfig sse) {
        if (metadata == null) {
            metadata = new ObjectMetadata();
        }
        String existing = metadata.getServerSideEncryption();
        if (existing != null && !sse.method.equals(existing)) {
            LOG.warn(
                    "Paimon SSE is overriding the previously-set server-side encryption "
                            + "method '{}' with '{}'.",
                    existing,
                    sse.method);
        }
        metadata.setServerSideEncryption(sse.method);
        if (sse.keyId != null) {
            metadata.setServerSideEncryptionKeyId(sse.keyId);
        }
        if (sse.dataEnc != null) {
            metadata.setServerSideDataEncryption(sse.dataEnc);
        }
        return metadata;
    }

    /** Stamp the SSE headers on a server-side CopyObject request (rename/commit). */
    static CopyObjectRequest applySse(CopyObjectRequest request, SseConfig sse) {
        // Use request fields, not newObjectMetadata (forces REPLACE, drops source metadata).
        // No data-encryption field; addHeader works since doOperation merges headers pre-sign.
        request.setServerSideEncryption(sse.method);
        if (sse.keyId != null) {
            request.setServerSideEncryptionKeyId(sse.keyId);
        }
        if (sse.dataEnc != null) {
            request.addHeader(OSSHeaders.OSS_SERVER_SIDE_DATA_ENCRYPTION, sse.dataEnc);
        }
        return request;
    }

    /** Stamps the SSE headers on every simple PutObject and server-side CopyObject. */
    static class SseObjectOperation extends OSSObjectOperation {
        private final SseConfig sse;

        SseObjectOperation(
                ServiceClient serviceClient, CredentialsProvider credsProvider, SseConfig sse) {
            super(serviceClient, credsProvider);
            this.sse = sse;
        }

        @Override
        public PutObjectResult putObject(PutObjectRequest request) {
            request.setMetadata(applySse(request.getMetadata(), sse));
            return super.putObject(request);
        }

        @Override
        public CopyObjectResult copyObject(CopyObjectRequest request) {
            return super.copyObject(applySse(request, sse));
        }
    }

    /** Stamps the SSE headers on the multipart-upload init; parts inherit them. */
    static class SseMultipartOperation extends OSSMultipartOperation {
        private final SseConfig sse;

        SseMultipartOperation(
                ServiceClient serviceClient, CredentialsProvider credsProvider, SseConfig sse) {
            super(serviceClient, credsProvider);
            this.sse = sse;
        }

        @Override
        public InitiateMultipartUploadResult initiateMultipartUpload(
                InitiateMultipartUploadRequest request) {
            request.setObjectMetadata(applySse(request.getObjectMetadata(), sse));
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
