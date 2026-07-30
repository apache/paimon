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

package org.apache.paimon.fs.cache;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link FileIO} wrapper that caches reads at block granularity.
 *
 * <p>Only file types in the whitelist are cached. Others are read directly from the delegate.
 *
 * <p>After deserialization, catalog-created wrappers lazily reuse a JVM-local cache manager.
 */
public class CachingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    // Sharing keeps the configured size limit JVM-wide instead of allocating one cache per task.
    private static final Map<LocalCacheConfiguration, LocalCacheManager>
            DESERIALIZED_CACHE_MANAGERS = new ConcurrentHashMap<>();

    private final FileIO delegate;
    private final Set<FileType> whitelist;
    @Nullable private final LocalCacheConfiguration cacheConfiguration;

    private transient volatile LocalCacheManager cache;

    public CachingFileIO(FileIO delegate, LocalCacheManager cache, Set<FileType> whitelist) {
        this(delegate, cache, whitelist, null);
    }

    private CachingFileIO(
            FileIO delegate,
            LocalCacheManager cache,
            Set<FileType> whitelist,
            @Nullable LocalCacheConfiguration cacheConfiguration) {
        this.delegate = delegate;
        this.cache = cache;
        this.whitelist = EnumSet.copyOf(whitelist);
        this.cacheConfiguration = cacheConfiguration;
    }

    /**
     * Wraps the given {@link FileIO} with caching if local cache is enabled in the catalog context.
     *
     * @param fileIO the FileIO to potentially wrap
     * @param context the catalog context containing cache configuration
     * @param cache the cache manager instance (managed by the Catalog)
     * @return a CachingFileIO if caching is enabled and configured, otherwise the original FileIO
     */
    public static FileIO wrapWithCachingIfNeeded(
            FileIO fileIO, CatalogContext context, @Nullable LocalCacheManager cache) {
        if (fileIO instanceof CachingFileIO) {
            return fileIO;
        }
        if (cache == null) {
            return fileIO;
        }
        Options options = context.options();
        Set<FileType> whitelist =
                FileType.parseWhitelist(options.get(CatalogOptions.LOCAL_CACHE_WHITELIST));
        if (whitelist.isEmpty()) {
            return fileIO;
        }
        return new CachingFileIO(fileIO, cache, whitelist, LocalCacheConfiguration.from(context));
    }

    /**
     * Creates a {@link LocalCacheManager} from the catalog context options, or returns null if
     * caching is not enabled.
     */
    @Nullable
    public static LocalCacheManager createCacheManager(CatalogContext context) {
        LocalCacheConfiguration configuration = LocalCacheConfiguration.from(context);
        return configuration == null ? null : configuration.createCacheManager();
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        FileType fileType = FileType.classify(path);
        if (!whitelist.contains(fileType) || FileType.isMutable(path)) {
            return delegate.newInputStream(path);
        }
        LocalCacheManager c = getOrCreateCacheManager();
        if (c == null) {
            return delegate.newInputStream(path);
        }
        return new CachingSeekableInputStream(delegate, path, c);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return delegate.newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return delegate.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return delegate.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return delegate.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return delegate.delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return delegate.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return delegate.rename(src, dst);
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        return delegate.createBlobPresignedUrl(tableRoot, descriptor, validity);
    }

    @Override
    public boolean isObjectStore() {
        return delegate.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        delegate.configure(context);
    }

    @Override
    public void setRuntimeContext(Map<String, String> options) {
        delegate.setRuntimeContext(options);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Nullable
    private LocalCacheManager getOrCreateCacheManager() {
        LocalCacheManager current = cache;
        if (current == null && cacheConfiguration != null) {
            synchronized (this) {
                current = cache;
                if (current == null) {
                    current =
                            DESERIALIZED_CACHE_MANAGERS.computeIfAbsent(
                                    cacheConfiguration,
                                    LocalCacheConfiguration::createCacheManager);
                    cache = current;
                }
            }
        }
        return current;
    }

    private static class LocalCacheConfiguration implements Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable private final String cacheDir;
        private final long maxSize;
        private final int blockSize;

        private LocalCacheConfiguration(@Nullable String cacheDir, long maxSize, int blockSize) {
            this.cacheDir = cacheDir;
            this.maxSize = maxSize;
            this.blockSize = blockSize;
        }

        @Nullable
        private static LocalCacheConfiguration from(CatalogContext context) {
            Options options = context.options();
            if (!options.get(CatalogOptions.LOCAL_CACHE_ENABLED)) {
                return null;
            }

            MemorySize maxSizeOption = options.get(CatalogOptions.LOCAL_CACHE_MAX_SIZE);
            long maxSize = maxSizeOption == null ? Long.MAX_VALUE : maxSizeOption.getBytes();
            int blockSize = (int) options.get(CatalogOptions.LOCAL_CACHE_BLOCK_SIZE).getBytes();
            return new LocalCacheConfiguration(
                    options.get(CatalogOptions.LOCAL_CACHE_DIR), maxSize, blockSize);
        }

        private LocalCacheManager createCacheManager() {
            if (cacheDir == null) {
                return new LocalMemoryCacheManager(maxSize, blockSize);
            }
            return new LocalDiskCacheManager(cacheDir, maxSize, blockSize);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof LocalCacheConfiguration)) {
                return false;
            }
            LocalCacheConfiguration that = (LocalCacheConfiguration) object;
            return maxSize == that.maxSize
                    && blockSize == that.blockSize
                    && Objects.equals(cacheDir, that.cacheDir);
        }

        @Override
        public int hashCode() {
            return Objects.hash(cacheDir, maxSize, blockSize);
        }
    }
}
