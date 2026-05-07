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
import org.apache.paimon.utils.FileType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FileIO} wrapper that caches reads at block granularity on local disk.
 *
 * <p>Only file types in the whitelist are cached. Others are read directly from the delegate.
 */
public class CachingFileIO implements FileIO {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CachingFileIO.class);

    private final FileIO delegate;
    private final transient BlockDiskCache cache;
    private final Set<FileType> whitelist;
    private transient volatile boolean cacheNullWarned;

    public CachingFileIO(FileIO delegate, BlockDiskCache cache, Set<FileType> whitelist) {
        this.delegate = delegate;
        this.cache = cache;
        this.whitelist = EnumSet.copyOf(whitelist);
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        if (cache == null) {
            if (!cacheNullWarned) {
                cacheNullWarned = true;
                LOG.warn(
                        "BlockDiskCache is null (likely after deserialization), "
                                + "file cache is disabled for this CachingFileIO instance.");
            }
            return delegate.newInputStream(path);
        }
        FileType fileType = FileType.classify(path);
        if (!whitelist.contains(fileType) || FileType.isMutable(path)) {
            return delegate.newInputStream(path);
        }
        return new CachingSeekableInputStream(delegate, path, cache);
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
        if (cache != null) {
            cache.close();
        }
        delegate.close();
    }
}
