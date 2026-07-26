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
import org.apache.paimon.options.Options;
import org.apache.paimon.plugin.PluginLoader;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.jindodata.dls.JindoDlsFileSystem;
import com.aliyun.jindodata.oss.JindoOssFileSystem;
import com.aliyun.jindodata.store.JindoMpuStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Duration;
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
     * Cache JindoOssFileSystem, at present, there is no good mechanism to ensure that the file
     * system will be shut down, so here the fs cache is used to avoid resource leakage.
     */
    private static final Map<CacheKey, Pair<JindoHadoopSystem, String>> CACHE =
            new ConcurrentHashMap<>();

    private JindoHadoopOptions jindoHadoopOptions;
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
        jindoHadoopOptions = new JindoHadoopOptions(context);
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
        return jindoHadoopOptions.get(shouldCache);
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
                presigner.configure(jindoHadoopOptions.get(false));
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
        final String scheme = path.toUri().getScheme();
        final String authority = path.toUri().getAuthority();
        Options options = jindoHadoopOptions.get(enableCache);
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
            closeFileSystems(fsMap);
            closeFileSystems(jindoCacheFsMap);
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

    private static void closeFileSystems(Map<String, Pair<JindoHadoopSystem, String>> fileSystems) {
        if (fileSystems == null) {
            return;
        }
        fileSystems.values().stream().map(Pair::getKey).forEach(IOUtils::closeQuietly);
        fileSystems.clear();
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
