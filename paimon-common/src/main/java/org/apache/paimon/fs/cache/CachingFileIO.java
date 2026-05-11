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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FileIO} wrapper that caches reads at block granularity.
 *
 * <p>Only file types in the whitelist are cached. Others are read directly from the delegate.
 *
 * <p>After deserialization, the cache is null and reads fall through to the delegate directly.
 */
public class CachingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final FileIO delegate;
    private final Set<FileType> whitelist;

    private transient volatile LocalCacheManager cache;

    public CachingFileIO(FileIO delegate, LocalCacheManager cache, Set<FileType> whitelist) {
        this.delegate = delegate;
        this.cache = cache;
        this.whitelist = EnumSet.copyOf(whitelist);
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
        return new CachingFileIO(fileIO, cache, whitelist);
    }

    /**
     * Creates a {@link LocalCacheManager} from the catalog context options, or returns null if
     * caching is not enabled.
     */
    @Nullable
    public static LocalCacheManager createCacheManager(CatalogContext context) {
        Options options = context.options();
        if (!options.get(CatalogOptions.LOCAL_CACHE_ENABLED)) {
            return null;
        }

        MemorySize maxSizeOpt = options.get(CatalogOptions.LOCAL_CACHE_MAX_SIZE);
        long maxSize = maxSizeOpt == null ? Long.MAX_VALUE : maxSizeOpt.getBytes();
        int blockSize = (int) options.get(CatalogOptions.LOCAL_CACHE_BLOCK_SIZE).getBytes();

        String cacheDir = options.get(CatalogOptions.LOCAL_CACHE_DIR);
        if (cacheDir != null) {
            return new LocalDiskCacheManager(cacheDir, maxSize, blockSize);
        } else {
            return new LocalMemoryCacheManager(maxSize, blockSize);
        }
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        LocalCacheManager c = cache;
        FileType fileType = FileType.classify(path);
        if (c == null || !whitelist.contains(fileType) || FileType.isMutable(path)) {
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
}
