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

package org.apache.paimon.io;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Cache data file to local. */
public class DataFileLocalCachingFileIO implements FileIO {

    private static final LRUCache CACHE = new LRUCache(LocalFileIO.create());

    private final LocalFileIO cacheFileIO = LocalFileIO.create();

    private final FileIO targetFileIO;
    private final String localTempDirs;
    private final MemorySize localCacheSize;
    private final double localCacheEvictionRatio;

    private transient IOManager ioManager;

    public DataFileLocalCachingFileIO(
            FileIO targetFileIO,
            String localTempDirs,
            MemorySize localCacheSize,
            double localCacheEvictionRatio) {
        this.targetFileIO = targetFileIO;
        this.localTempDirs = localTempDirs;
        this.localCacheSize = localCacheSize;
        this.localCacheEvictionRatio = localCacheEvictionRatio;
    }

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        // won't be called currently
    }

    @Override
    public void setRuntimeContext(Map<String, String> options) {
        targetFileIO.setRuntimeContext(options);
    }

    // TODO: what if the input is very large ?
    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        if (notDataFilePath(path) || cacheFull()) {
            return targetFileIO.newInputStream(path);
        }

        Optional<Path> cachePath = getCachePath(path, true);
        if (cachePath.isPresent()) {
            return cacheFileIO.newInputStream(cachePath.get());
        }

        Path newCachePath = newLocalCachePath(path);
        IOUtils.copyBytes(
                targetFileIO.newInputStream(path), cacheFileIO.newOutputStream(newCachePath, true));
        CACHE.put(path, newCachePath, true);
        return cacheFileIO.newInputStream(newCachePath);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        if (notDataFilePath(path)) {
            return targetFileIO.newOutputStream(path, overwrite);
        }

        checkState(
                !getCachePath(path, false).isPresent() && !overwrite,
                "Data file won't be written after closed.");

        Path cachePath = newLocalCachePath(path);
        CACHE.put(path, cachePath, false);
        return cacheFileIO.newOutputStream(cachePath, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        // For non-data files, directly get status from target file system
        if (notDataFilePath(path)) {
            return targetFileIO.getFileStatus(path);
        }

        // If file is in cache but not on remote, flush it to remote
        LRUCache.Node node = CACHE.get(path, false);
        if (node != null && !node.onRemote()) {
            CACHE.flushFile(path, targetFileIO);
        }

        return targetFileIO.getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return targetFileIO.listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return getCachePath(path, true).isPresent() || targetFileIO.exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LRUCache.Node node = CACHE.invalidate(path);
        if (node == null || node.onRemote()) {
            return targetFileIO.delete(path, recursive);
        }

        // true if invalidating cache successfully
        return true;
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return targetFileIO.mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        checkState(notDataFilePath(src), "Data file won't be renamed.");
        return targetFileIO.rename(src, dst);
    }

    @Override
    public void close() throws IOException {
        flush();
        CACHE.expire(0);
        cacheFileIO.close();
        targetFileIO.close();
    }

    public void flush() throws IOException {
        CACHE.flush(targetFileIO);
    }

    public void expire() throws IOException {
        CACHE.expire((long) (localCacheSize.getBytes() * localCacheEvictionRatio));
    }

    private boolean cacheFull() {
        return CACHE.size() >= localCacheSize.getBytes();
    }

    private boolean notDataFilePath(Path path) {
        return !path.toString().contains(FileStorePathFactory.BUCKET_PATH_PREFIX);
    }

    private Path newLocalCachePath(Path origin) {
        synchronized (this) {
            if (ioManager == null) {
                ioManager = IOManager.create(localTempDirs);
            }
        }

        FileIOChannel.ID id = ioManager.createChannel(origin.getName() + "-local-cache-");
        return new Path(id.getPath());
    }

    private Optional<Path> getCachePath(Path origin, boolean refreshLru) {
        LRUCache.Node node = CACHE.get(origin, refreshLru);
        return node == null ? Optional.empty() : Optional.of(node.cachePath());
    }
}
