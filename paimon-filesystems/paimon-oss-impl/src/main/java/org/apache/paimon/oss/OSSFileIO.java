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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.IOUtils;

import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.options.CatalogOptions.FILE_IO_POPULATE_META;

/** OSS {@link FileIO}. */
public class OSSFileIO extends HadoopCompliantFileIO {

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
    private boolean populateMeta = false;

    @Override
    public boolean isObjectStore() {
        return true;
    }

    @Override
    public void configure(CatalogContext context) {
        allowCache = context.options().get(FILE_IO_ALLOW_CACHE);
        populateMeta = context.options().get(FILE_IO_POPULATE_META);
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

    public Options hadoopOptions() {
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
    public void close() {
        if (!allowCache) {
            fsMap.values().forEach(IOUtils::closeQuietly);
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

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        FileStatus basic = super.getFileStatus(path);
        if (!populateMeta) {
            return basic;
        }
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(path(path));
        return getExtendedFileStatus(fs, basic);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        FileStatus[] basic = super.listStatus(path);
        if (!populateMeta) {
            return basic;
        }
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(path(path));
        FileStatus[] extended = new FileStatus[basic.length];
        for (int i = 0; i < basic.length; i++) {
            extended[i] = getExtendedFileStatus(fs, basic[i]);
        }
        return extended;
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        RemoteIterator<FileStatus> basicIter = super.listFilesIterative(path, recursive);
        if (!populateMeta) {
            return basicIter;
        }
        AliyunOSSFileSystem fs = (AliyunOSSFileSystem) getFileSystem(path(path));
        return new RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return basicIter.hasNext();
            }

            @Override
            public FileStatus next() throws IOException {
                FileStatus basic = basicIter.next();
                return getExtendedFileStatus(fs, basic);
            }

            @Override
            public void close() throws IOException {
                basicIter.close();
            }
        };
    }

    private ExtendedFileStatus getExtendedFileStatus(AliyunOSSFileSystem fs, FileStatus status) {
        org.apache.hadoop.fs.Path path = path(status.getPath());
        if (!path.isAbsolute()) {
            path = new org.apache.hadoop.fs.Path(fs.getWorkingDirectory(), path);
        }
        String objKey = path.toUri().getPath().substring(1);
        ObjectMetadata meta = fs.getStore().getObjectMetadata(objKey);
        return new ExtendedFileStatus(status, meta);
    }

    private static class ExtendedFileStatus implements FileStatus {

        private final FileStatus basic;
        @Nullable private final com.aliyun.oss.model.ObjectMetadata meta;

        private ExtendedFileStatus(
                FileStatus basic, @Nullable com.aliyun.oss.model.ObjectMetadata meta) {
            this.basic = basic;
            this.meta = meta;
        }

        @Override
        public long getLen() {
            return basic.getLen();
        }

        @Override
        public boolean isDir() {
            return basic.isDir();
        }

        @Override
        public Path getPath() {
            return basic.getPath();
        }

        @Override
        public long getModificationTime() {
            return basic.getModificationTime();
        }

        @Override
        public long getAccessTime() {
            return basic.getAccessTime();
        }

        @Nullable
        @Override
        public String getOwner() {
            return basic.getOwner();
        }

        @Nullable
        @Override
        public Integer getGeneration() {
            return basic.getGeneration();
        }

        @Nullable
        @Override
        public String getContentType() {
            if (meta == null) {
                return basic.getContentType();
            }
            return meta.getContentType();
        }

        @Nullable
        @Override
        public String getStorageClass() {
            return basic.getStorageClass();
        }

        @Nullable
        @Override
        public String getMd5Hash() {
            if (meta == null) {
                return basic.getMd5Hash();
            }
            return meta.getContentMD5();
        }

        @Nullable
        @Override
        public Long getMetadataModificationTime() {
            if (meta == null) {
                return basic.getMetadataModificationTime();
            }
            return meta.getLastModified().getTime();
        }

        @Nullable
        @Override
        public Map<String, String> getMetadata() {
            if (meta == null) {
                return basic.getMetadata();
            }
            return meta.getUserMetadata();
        }
    }
}
