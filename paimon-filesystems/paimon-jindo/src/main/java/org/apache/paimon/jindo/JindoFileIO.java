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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.dls.JindoDlsFileSystem;
import com.aliyun.jindodata.oss.JindoOssFileSystem;
import com.aliyun.jindodata.oss.auth.SimpleCredentialsProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;

/** Jindo {@link FileIO}. */
public class JindoFileIO extends HadoopCompliantFileIO {

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

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(), OSS_SECURITY_TOKEN);
                }
            };

    /**
     * Cache JindoOssFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, Pair<JindoHadoopSystem, String>> CACHE =
            new ConcurrentHashMap<>();

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
    }

    public Options hadoopOptions() {
        return hadoopOptions;
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        if (!overwrite && this.exists(path)) {
            throw new IOException("File " + path + " already exists.");
        }
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        Pair<JindoHadoopSystem, String> pair = getFileSystemPair(hadoopPath);
        JindoHadoopSystem fs = pair.getKey();
        return new JindoTwoPhaseOutputStream(
                new JindoMultiPartUpload(fs, hadoopPath), hadoopPath, path);
    }

    @Override
    protected Pair<JindoHadoopSystem, String> createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        Supplier<Pair<JindoHadoopSystem, String>> supplier =
                () -> {
                    Configuration hadoopConf = new Configuration(false);
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
                    new CacheKey(hadoopOptions, scheme, authority), key -> supplier.get());
        } else {
            return supplier.get();
        }
    }

    @Override
    public void close() {
        if (!allowCache) {
            fsMap.values().stream().map(Pair::getKey).forEach(IOUtils::closeQuietly);
            fsMap.clear();
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
