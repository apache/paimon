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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.jindo.CacheEndpoints.CacheEndpoint;
import org.apache.paimon.options.Options;
import org.apache.paimon.plugin.PluginLoader;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SensitiveConfigUtils;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.dls.JindoDlsFileSystem;
import com.aliyun.jindodata.oss.JindoOssFileSystem;
import com.aliyun.jindodata.oss.auth.SimpleCredentialsProvider;
import com.aliyun.jindodata.store.JindoMpuStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;

/** Jindo {@link FileIO}. */
public class JindoFileIO extends HadoopCompliantFileIO implements HadoopOptionsProvider {

    private static final long serialVersionUID = 2L;

    private static final Logger LOG = LoggerFactory.getLogger(JindoFileIO.class);

    /**
     * In order to simplify, we make paimon oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.` in paimon conf to hadoop conf.
     *
     * <p>Not use fs.oss because this FileIO also access dlf/dls scheme.
     */
    private static final String[] CONFIG_PREFIXES = {"fs."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_SHOW_DIR_TIMESTAMP = "fs.oss.show-dir-timestamp";
    private static final String OSS_ENDPOINT = "fs.oss.endpoint";
    private static final String OSS_HTTPS_ENABLE = "fs.oss.https.enable";
    private static final String OSS_SECOND_LEVEL_DOMAIN_ENABLE =
            "fs.oss.second.level.domain.enable";
    private static final String OSS_REGION = "fs.oss.region";
    // JindoSDK options of a DLF cache cluster, which the OSS endpoint must not use
    private static final String OSS_DLF_CACHE_PREFIX = "fs.oss.dlf-cache.";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(Locale.ROOT), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(Locale.ROOT), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(Locale.ROOT), OSS_SECURITY_TOKEN);
                }
            };

    /**
     * Cache JindoOssFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, Pair<JindoHadoopSystem, String>> CACHE =
            new ConcurrentHashMap<>();

    private Options hadoopOptions;
    private Options hadoopOptionsWithCache;
    private Map<String, Options> cacheEndpointOptions;
    private boolean allowCache = true;
    private transient BlobPresigner blobPresigner;

    public JindoFileIO() {}

    JindoFileIO(BlobPresigner blobPresigner) {
        this.blobPresigner = blobPresigner;
    }

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        super.configure(context);
        allowCache = context.options().get(FILE_IO_ALLOW_CACHE);
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = context.options().get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase(Locale.ROOT))) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase(Locale.ROOT));
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            SensitiveConfigUtils.redactValue(key, hadoopOptions.get(key)));
                }
            }
        }
        // as in rest catalog use could define ak for table so we need first use ak.
        if (hadoopOptions.containsKey(OSS_ACCESS_KEY_ID)
                && hadoopOptions.containsKey(OSS_ACCESS_KEY_SECRET)) {
            LOG.info("Using Ak init Jindo.");
            // https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.1/oss/hadoop/jindosdk_ide_hadoop.md
            hadoopOptions.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
            hadoopOptions.set("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");

            // Misalignment can greatly affect performance, so the maximum buffer is set here
            hadoopOptions.set("fs.oss.read.position.buffer.size", "8388608");
            hadoopOptions.set("fs.oss.credentials.provider", SimpleCredentialsProvider.NAME);
        } else {
            LOG.info("Using hadoop conf init Jindo.");
            context.hadoopConf()
                    .iterator()
                    .forEachRemaining(entry -> hadoopOptions.set(entry.getKey(), entry.getValue()));
        }

        // Resolving a timestamp for every listed directory is expensive in Jindo.
        if (!hadoopOptions.containsKey(OSS_SHOW_DIR_TIMESTAMP)) {
            hadoopOptions.set(OSS_SHOW_DIR_TIMESTAMP, "false");
        }

        JindoUserAgent.apply(context.options(), hadoopOptions);

        // another config when enable cache
        hadoopOptionsWithCache = new Options(hadoopOptions.toMap());
        hadoopOptionsWithCache.set("fs.xengine", "jindocache");
        if (!hadoopOptionsWithCache.containsKey("fs.jindocache.client.metrics.enable")) {
            // enable metrics report by default
            hadoopOptionsWithCache.set("fs.jindocache.client.metrics.enable", "true");
        }
        // Workaround: following configurations to avoid bug in some JindoSDK versions
        hadoopOptionsWithCache.set("fs.oss.read.profile.columnar.use-pread", "false");
        hadoopOptionsWithCache.set(
                "fs.jindocache.read.profile.columnar.readahead.pread.enable", "false");

        if (cacheEndpoints != null) {
            cacheEndpointOptions = new HashMap<>();
            for (CacheEndpoint endpoint : cacheEndpoints.endpoints()) {
                Options options = withEndpoint(hadoopOptions, endpoint.url);
                options.set(
                        OSS_SECOND_LEVEL_DOMAIN_ENABLE, String.valueOf(endpoint.pathStyleAccess));
                if (endpoint.region != null) {
                    options.set(OSS_REGION, endpoint.region);
                }
                cacheEndpointOptions.put(endpoint.name, options);
            }
            // fs.oss.endpoint may name a cache for older clients, so set the OSS endpoint again
            hadoopOptions = withEndpoint(hadoopOptions, cacheEndpoints.ossEndpoint());
            hadoopOptions.keySet().removeIf(key -> key.startsWith(OSS_DLF_CACHE_PREFIX));
        }
    }

    /** Copies the options with an endpoint: its host[:port], and https from its scheme if any. */
    static Options withEndpoint(Options base, @Nullable String endpoint) {
        Options options = new Options(base.toMap());
        if (endpoint == null) {
            return options;
        }
        int schemeEnd = endpoint.indexOf("://");
        if (schemeEnd >= 0) {
            String scheme = endpoint.substring(0, schemeEnd);
            options.set(OSS_HTTPS_ENABLE, String.valueOf("https".equalsIgnoreCase(scheme)));
            endpoint = endpoint.substring(schemeEnd + 3);
        }
        int pathStart = endpoint.indexOf('/');
        options.set(OSS_ENDPOINT, pathStart >= 0 ? endpoint.substring(0, pathStart) : endpoint);
        return options;
    }

    /**
     * This method is used to initialize some thirdparty connector, such as Lance reader/writer.
     *
     * @param path file path
     * @param opType read/write/meta
     * @return
     */
    @Override
    public Options hadoopOptions(Path path, String opType) {
        boolean shouldCache = false;
        if (opType.equalsIgnoreCase("read")) {
            shouldCache = readCacheEnabled && shouldCache(path);
        } else if (opType.equalsIgnoreCase("write")) {
            shouldCache = writeCacheEnabled && shouldCache(path);
        } else if (opType.equalsIgnoreCase("meta")) {
            shouldCache = metaCacheEnabled && shouldCache(path);
        }
        if (!shouldCache) {
            return hadoopOptions;
        } else if (cacheEndpoints != null) {
            return cacheEndpointOptions.get(cacheEndpoints.endpointOf(path).name);
        } else {
            return hadoopOptionsWithCache;
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        if (!overwrite && this.exists(path)) {
            throw new IOException("File " + path + " already exists.");
        }
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        Pair<JindoHadoopSystem, String> pair = getFileSystemPair(hadoopPath, false);
        JindoHadoopSystem fs = pair.getKey();
        JindoMpuStore mpuStore = fs.getMpuStore(hadoopPath);
        if (mpuStore == null) {
            LOG.debug(
                    "Jindo multipart upload is unavailable for {}, falling back to rename commit.",
                    path);
            return super.newTwoPhaseOutputStream(path, overwrite);
        }
        return new JindoTwoPhaseOutputStream(
                new JindoMultiPartUpload(mpuStore, fs.getWorkingDirectory()), hadoopPath, path);
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        BlobPresigner presigner = blobPresigner();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(presigner.getClass().getClassLoader());
            return presigner.create(tableRoot, descriptor, validity);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private synchronized BlobPresigner blobPresigner() {
        if (blobPresigner == null) {
            PluginLoader loader = BlobPlugin.getLoader();
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(loader.submoduleClassLoader());
                BlobPresigner presigner =
                        loader.newInstance("org.apache.paimon.jindo.JindoBlobPresigner");
                presigner.configure(hadoopOptions);
                blobPresigner = presigner;
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
        return blobPresigner;
    }

    private static class BlobPlugin {

        private static PluginLoader loader;

        private static synchronized PluginLoader getLoader() {
            if (loader == null) {
                loader = new PluginLoader("paimon-plugin-jindo-oss");
            }
            return loader;
        }
    }

    @Override
    protected Pair<JindoHadoopSystem, String> createFileSystem(
            org.apache.hadoop.fs.Path path, boolean enableCache) {
        return createFileSystem(path, enableCache ? hadoopOptionsWithCache : hadoopOptions);
    }

    @Override
    Pair<JindoHadoopSystem, String> createFileSystem(
            org.apache.hadoop.fs.Path path, CacheEndpoint endpoint) {
        return createFileSystem(path, cacheEndpointOptions.get(endpoint.name));
    }

    private Pair<JindoHadoopSystem, String> createFileSystem(
            org.apache.hadoop.fs.Path path, Options options) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        Supplier<Pair<JindoHadoopSystem, String>> supplier =
                () -> {
                    Configuration hadoopConf = new Configuration(false);
                    options.toMap().forEach(hadoopConf::set);
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

                    JindoHadoopSystem fs;
                    if ("oss".equals(scheme)) {
                        fs = new JindoOssFileSystem();
                    } else if ("dls".equals(scheme)) {
                        fs = new JindoDlsFileSystem();
                    } else {
                        throw new RuntimeException(
                                "Unsupported scheme for Jindo FileSystem: " + scheme);
                    }

                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return Pair.of(fs, fs.getSysType(path).getSysType());
                };

        if (allowCache) {
            return CACHE.computeIfAbsent(
                    new CacheKey(options, scheme, authority), key -> supplier.get());
        } else {
            return supplier.get();
        }
    }

    @Override
    public synchronized void close() {
        if (blobPresigner != null) {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(blobPresigner.getClass().getClassLoader());
                blobPresigner.close();
                blobPresigner = null;
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
        if (!allowCache) {
            fsMap.values().stream().map(Pair::getKey).forEach(IOUtils::closeQuietly);
            fsMap.clear();
            if (cacheEndpointFsMap != null) {
                cacheEndpointFsMap.values().stream()
                        .map(Pair::getKey)
                        .forEach(IOUtils::closeQuietly);
                cacheEndpointFsMap.clear();
            }
        }
    }

    /** Contract shared with the isolated OSS implementation. */
    public interface BlobPresigner extends AutoCloseable {

        void configure(Options options);

        String create(Path tableRoot, BlobDescriptor descriptor, Duration validity)
                throws IOException;

        @Override
        void close();
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
