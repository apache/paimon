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
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link FileIO} wrapper that caches reads at block granularity.
 *
 * <p>Only file types in the whitelist are cached. Others are read directly from the delegate.
 *
 * <p>After deserialization, copies of the same catalog-created wrapper lazily reuse a JVM-local
 * cache manager.
 */
public class CachingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    // Copies of one serialized wrapper share a JVM-local limit instead of allocating one per task.
    private static final Map<LocalCacheConfiguration, SharedCacheManager>
            DESERIALIZED_CACHE_MANAGERS = new ConcurrentHashMap<>();

    private final FileIO delegate;
    private final Set<FileType> whitelist;
    @Nullable private final LocalCacheConfiguration cacheConfiguration;
    private String cacheNamespace;

    private transient volatile LocalCacheManager cache;
    private transient volatile SharedCacheManager sharedCacheManager;
    private transient volatile boolean closed;

    public CachingFileIO(FileIO delegate, LocalCacheManager cache, Set<FileType> whitelist) {
        this(delegate, cache, whitelist, null, UUID.randomUUID().toString());
    }

    private CachingFileIO(
            FileIO delegate,
            LocalCacheManager cache,
            Set<FileType> whitelist,
            @Nullable LocalCacheConfiguration cacheConfiguration,
            String cacheNamespace) {
        this.delegate = delegate;
        this.cache = cache;
        this.whitelist = EnumSet.copyOf(whitelist);
        this.cacheConfiguration = cacheConfiguration;
        this.cacheNamespace = cacheNamespace;
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
        String cacheNamespace = UUID.randomUUID().toString();
        return new CachingFileIO(
                fileIO,
                cache,
                whitelist,
                LocalCacheConfiguration.from(context, cacheNamespace),
                cacheNamespace);
    }

    /**
     * Creates a {@link LocalCacheManager} from the catalog context options, or returns null if
     * caching is not enabled.
     */
    @Nullable
    public static LocalCacheManager createCacheManager(CatalogContext context) {
        LocalCacheConfiguration configuration = LocalCacheConfiguration.from(context, "");
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
        if (c instanceof LocalDiskCacheManager) {
            FileStatus status = delegate.getFileStatus(path);
            return new CachingSeekableInputStream(
                    delegate, path, c, diskCacheKey(path, status), status.getLen());
        }
        return new CachingSeekableInputStream(delegate, path, c, cacheNamespace + ":" + path, -1);
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
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        // the interface default (temp file + rename) would bypass the delegate's atomic override
        return delegate.tryToWriteAtomic(path, content);
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
        SharedCacheManager shared;
        synchronized (this) {
            closed = true;
            shared = sharedCacheManager;
            sharedCacheManager = null;
            if (shared != null) {
                cache = null;
            }
        }

        try {
            delegate.close();
        } finally {
            if (shared != null) {
                releaseCacheManager(cacheConfiguration, shared);
            }
        }
    }

    @Nullable
    private LocalCacheManager getOrCreateCacheManager() {
        if (closed) {
            return null;
        }
        LocalCacheManager current = cache;
        if (current == null && cacheConfiguration != null) {
            synchronized (this) {
                if (closed) {
                    return null;
                }
                current = cache;
                if (current == null) {
                    SharedCacheManager shared = sharedCacheManager;
                    if (shared == null) {
                        shared = acquireCacheManager(cacheConfiguration);
                        sharedCacheManager = shared;
                    }
                    current = shared.getOrCreate(cacheConfiguration);
                    cache = current;
                }
            }
        }
        return current;
    }

    private static SharedCacheManager acquireCacheManager(
            LocalCacheConfiguration cacheConfiguration) {
        return DESERIALIZED_CACHE_MANAGERS.compute(
                cacheConfiguration,
                (ignored, existing) -> {
                    SharedCacheManager shared =
                            existing == null ? new SharedCacheManager() : existing;
                    shared.retain(cacheConfiguration.cacheNamespace);
                    return shared;
                });
    }

    private static void releaseCacheManager(
            @Nullable LocalCacheConfiguration cacheConfiguration, SharedCacheManager shared) {
        if (cacheConfiguration == null) {
            return;
        }
        DESERIALIZED_CACHE_MANAGERS.computeIfPresent(
                cacheConfiguration,
                (ignored, existing) -> {
                    if (existing != shared) {
                        return existing;
                    }
                    return existing.release(cacheConfiguration.cacheNamespace) ? null : existing;
                });
    }

    private static String diskCacheKey(Path path, FileStatus status) {
        return path + "\0" + status.getLen() + "\0" + status.getModificationTime();
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        if (cacheConfiguration != null) {
            sharedCacheManager = acquireCacheManager(cacheConfiguration);
        }
    }

    private static class SharedCacheManager {

        @Nullable private LocalCacheManager cacheManager;
        private final Map<String, Integer> namespaceReferences = new HashMap<>();
        private int references;

        private synchronized void retain(String cacheNamespace) {
            references++;
            namespaceReferences.merge(cacheNamespace, 1, Integer::sum);
        }

        private synchronized LocalCacheManager getOrCreate(
                LocalCacheConfiguration cacheConfiguration) {
            if (cacheManager == null) {
                cacheManager = cacheConfiguration.createCacheManager();
            }
            return cacheManager;
        }

        private synchronized boolean release(String cacheNamespace) {
            if (references <= 0) {
                throw new IllegalStateException("Cache manager has already been released.");
            }
            Integer namespaceCount = namespaceReferences.get(cacheNamespace);
            if (namespaceCount == null) {
                throw new IllegalStateException("Cache namespace has already been released.");
            }
            if (namespaceCount == 1) {
                namespaceReferences.remove(cacheNamespace);
                if (cacheManager != null) {
                    cacheManager.invalidate(cacheNamespace + ":");
                }
            } else {
                namespaceReferences.put(cacheNamespace, namespaceCount - 1);
            }
            return --references == 0;
        }
    }

    private static class LocalCacheConfiguration implements Serializable {

        private static final long serialVersionUID = 1L;

        @Nullable private final String cacheDir;
        private final long maxSize;
        private final int blockSize;
        private final String cacheNamespace;

        private LocalCacheConfiguration(
                @Nullable String cacheDir, long maxSize, int blockSize, String cacheNamespace) {
            this.cacheDir = cacheDir;
            this.maxSize = maxSize;
            this.blockSize = blockSize;
            this.cacheNamespace = cacheNamespace;
        }

        @Nullable
        private static LocalCacheConfiguration from(CatalogContext context, String cacheNamespace) {
            Options options = context.options();
            if (!options.get(CatalogOptions.LOCAL_CACHE_ENABLED)) {
                return null;
            }

            MemorySize maxSizeOption = options.get(CatalogOptions.LOCAL_CACHE_MAX_SIZE);
            long maxSize = maxSizeOption == null ? Long.MAX_VALUE : maxSizeOption.getBytes();
            int blockSize = (int) options.get(CatalogOptions.LOCAL_CACHE_BLOCK_SIZE).getBytes();
            return new LocalCacheConfiguration(
                    options.get(CatalogOptions.LOCAL_CACHE_DIR),
                    maxSize,
                    blockSize,
                    cacheNamespace);
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
