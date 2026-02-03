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

package org.apache.paimon.azure;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Azure Blob Storage {@link FileIO}. */
public class AzureFileIO extends HadoopCompliantFileIO {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AzureFileIO.class);

    private static final String[] CONFIG_PREFIXES = {"azure.", "fs.azure.", "fs.wasb."};

    private static final String HADOOP_CONFIG_PREFIX = "fs.azure.";

    private static final String ABFS_SCHEME = "abfs";

    private static final String[][] MIRRORED_CONFIG_KEYS = {
        {"fs.azure.account.key", "fs.azure.account.key.ACCOUNT.blob.core.windows.net"},
        {"fs.azure.account.auth.type", "fs.azure.account.oauth2.client.endpoint"}
    };

    private static final Map<CacheKey, FileSystem> CACHE = new ConcurrentHashMap<>();

    private Options hadoopOptions;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public boolean supportsConditionalWrite() {
        return true;
    }

    @Override
    public boolean tryToWriteAtomicIfAbsent(Path path, String content) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileSystem fs = getFileSystem(hadoopPath);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream out = fs.createFile(hadoopPath).create().overwrite(false).build()) {
            out.write(bytes);
            return true;
        } catch (FileAlreadyExistsException e) {
            LOG.debug("Conditional write failed, file already exists: {}", path);
            return false;
        }
    }

    @Override
    public void configure(CatalogContext context) {
        this.hadoopOptions = mirrorCertainHadoopConfig(loadHadoopConfigFromContext(context));
    }

    // add additional config entries from the IO config to the Hadoop config
    private Options loadHadoopConfigFromContext(CatalogContext context) {
        Options hadoopConfig = new Options();
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = HADOOP_CONFIG_PREFIX + key.substring(prefix.length());
                    String value = context.options().get(key);
                    hadoopConfig.set(newKey, value);

                    LOG.debug("Adding config entry for {} as {} to Hadoop config", key, newKey);
                }
            }
        }
        return hadoopConfig;
    }

    // mirror certain keys to make use more uniform across implementations
    // with different keys
    private Options mirrorCertainHadoopConfig(Options hadoopConfig) {
        for (String[] mirrored : MIRRORED_CONFIG_KEYS) {
            String value = hadoopConfig.get(mirrored[0]);
            if (value != null) {
                hadoopConfig.set(mirrored[1], value);
            }
        }
        return hadoopConfig;
    }

    @Override
    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) {
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        return CACHE.computeIfAbsent(
                new CacheKey(hadoopOptions, scheme, authority),
                key -> {
                    Configuration hadoopConf = new Configuration();
                    key.options.toMap().forEach(hadoopConf::set);
                    URI fsUri = path.toUri();
                    FileSystem fs;

                    if (ABFS_SCHEME.equalsIgnoreCase(scheme)) {
                        fs = new AzureBlobFileSystem();
                        LOG.debug("Creating ABFS filesystem for {}", fsUri);
                    } else {
                        fs = new NativeAzureFileSystem();
                        LOG.debug("Creating WASB filesystem for {}", fsUri);
                    }
                    try {
                        fs.initialize(fsUri, hadoopConf);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return fs;
                });
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
