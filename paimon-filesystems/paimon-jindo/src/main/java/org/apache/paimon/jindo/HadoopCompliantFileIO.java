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
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_FILE_IO_CACHE_WHITELIST_PATH;

/**
 * Hadoop {@link FileIO}.
 *
 * <p>Important: copy this class from HadoopFileIO here to avoid class loader conflicts.
 */
public abstract class HadoopCompliantFileIO implements FileIO {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopCompliantFileIO.class);

    private static final long serialVersionUID = 1L;

    /// Detailed cache strategies are retrieved from REST server.
    private static final String META_CACHE_ENABLED_TAG = "meta";
    private static final String READ_CACHE_ENABLED_TAG = "read";
    private static final String WRITE_CACHE_ENABLED_TAG = "write";
    private static final String DISABLE_CACHE_TAG = "none";

    protected boolean metaCacheEnabled = false;
    protected boolean readCacheEnabled = false;
    protected boolean writeCacheEnabled = false;

    protected transient volatile Map<String, Pair<JindoHadoopSystem, String>> fsMap;
    protected transient volatile Map<String, Pair<JindoHadoopSystem, String>> jindoCacheFsMap;

    // Only enable cache for path which is generated with uuid
    private List<String> cacheWhitelistPaths = new ArrayList<>();

    boolean shouldCache(Path path) {
        if (cacheWhitelistPaths.isEmpty()) {
            return true;
        }
        String pathStr = path.toUri().getPath();
        for (String pattern : cacheWhitelistPaths) {
            if (pathStr.contains(pattern)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void configure(CatalogContext context) {
        // Process file io cache configuration
        if (!context.options().get(DLF_FILE_IO_CACHE_ENABLED)
                || context.options().get(DLF_FILE_IO_CACHE_POLICY) == null
                || context.options().get(DLF_FILE_IO_CACHE_POLICY).contains(DISABLE_CACHE_TAG)) {
            return;
        }
        // Enable file io cache
        if (context.options().get("fs.jindocache.namespace.rpc.address") == null) {
            LOG.info(
                    "FileIO cache is enabled but JindoCache RPC address is not set, fallback to no-cache");
        } else {
            metaCacheEnabled =
                    context.options()
                            .get(DLF_FILE_IO_CACHE_POLICY)
                            .contains(META_CACHE_ENABLED_TAG);
            readCacheEnabled =
                    context.options()
                            .get(DLF_FILE_IO_CACHE_POLICY)
                            .contains(READ_CACHE_ENABLED_TAG);
            writeCacheEnabled =
                    context.options()
                            .get(DLF_FILE_IO_CACHE_POLICY)
                            .contains(WRITE_CACHE_ENABLED_TAG);
            String whitelist = context.options().get(DLF_FILE_IO_CACHE_WHITELIST_PATH);
            if (!whitelist.equals("*")) {
                cacheWhitelistPaths = Lists.newArrayList(whitelist.split(","));
            }
            LOG.info(
                    "Cache enabled with cache policy: meta cache enabled {}, read cache enabled {}, write cache enabled {}, whitelist path: {}",
                    metaCacheEnabled,
                    readCacheEnabled,
                    writeCacheEnabled,
                    whitelist);
        }
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        boolean shouldCache = readCacheEnabled && shouldCache(path);
        LOG.debug("InputStream should cache {} for path {}", shouldCache, path);
        Pair<JindoHadoopSystem, String> pair = getFileSystemPair(hadoopPath, shouldCache);
        JindoHadoopSystem fs = pair.getKey();
        String sysType = pair.getValue();
        FSDataInputStream fsInput = fs.open(hadoopPath);
        return "jobj".equalsIgnoreCase(sysType)
                ? new VectoredReadableInputStream(fsInput)
                : new HadoopSeekableInputStream(fsInput);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        boolean shouldCache = writeCacheEnabled && shouldCache(path);
        LOG.debug("OutputStream should cache {} for path {}", shouldCache, path);
        return new HadoopPositionOutputStream(
                getFileSystem(hadoopPath, shouldCache).create(hadoopPath, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        boolean shouldCache = metaCacheEnabled && shouldCache(path);
        LOG.debug("GetFileStatus should cache {} for path {}", shouldCache, path);
        return new HadoopFileStatus(
                getFileSystem(hadoopPath, shouldCache).getFileStatus(hadoopPath));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileStatus[] statuses = new FileStatus[0];
        org.apache.hadoop.fs.FileStatus[] hadoopStatuses =
                getFileSystem(hadoopPath, false).listStatus(hadoopPath);
        if (hadoopStatuses != null) {
            statuses = new FileStatus[hadoopStatuses.length];
            for (int i = 0; i < hadoopStatuses.length; i++) {
                statuses[i] = new HadoopFileStatus(hadoopStatuses[i]);
            }
        }
        return statuses;
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> hadoopIter =
                getFileSystem(hadoopPath, false).listFiles(hadoopPath, recursive);
        return new RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return hadoopIter.hasNext();
            }

            @Override
            public FileStatus next() throws IOException {
                org.apache.hadoop.fs.FileStatus hadoopStatus = hadoopIter.next();
                return new HadoopFileStatus(hadoopStatus);
            }
        };
    }

    @Override
    public boolean exists(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        boolean shouldCache = metaCacheEnabled && shouldCache(path);
        LOG.debug("Exists should cache {} for path {}", shouldCache, path);
        return getFileSystem(hadoopPath, shouldCache).exists(hadoopPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath, false).delete(hadoopPath, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath, false).mkdirs(hadoopPath);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        org.apache.hadoop.fs.Path hadoopSrc = path(src);
        org.apache.hadoop.fs.Path hadoopDst = path(dst);
        return getFileSystem(hadoopSrc, false).rename(hadoopSrc, hadoopDst);
    }

    protected org.apache.hadoop.fs.Path path(Path path) {
        URI uri = path.toUri();
        if ("oss".equals(uri.getScheme()) && uri.getUserInfo() != null) {
            path = new Path("oss:/" + uri.getPath());
        }
        return new org.apache.hadoop.fs.Path(path.toUri());
    }

    protected JindoHadoopSystem getFileSystem(org.apache.hadoop.fs.Path path, boolean enableCache)
            throws IOException {
        return getFileSystemPair(path, enableCache).getKey();
    }

    protected Pair<JindoHadoopSystem, String> getFileSystemPair(
            org.apache.hadoop.fs.Path path, boolean enableCache) throws IOException {
        Map<String, Pair<JindoHadoopSystem, String>> map;
        if (enableCache) {
            if (jindoCacheFsMap == null) {
                synchronized (this) {
                    if (jindoCacheFsMap == null) {
                        jindoCacheFsMap = new ConcurrentHashMap<>();
                    }
                }
            }
            map = jindoCacheFsMap;
        } else {
            if (fsMap == null) {
                synchronized (this) {
                    if (fsMap == null) {
                        fsMap = new ConcurrentHashMap<>();
                    }
                }
            }
            map = fsMap;
        }

        String authority = path.toUri().getAuthority();
        if (authority == null) {
            authority = "DEFAULT";
        }
        try {
            return map.computeIfAbsent(
                    authority,
                    k -> {
                        try {
                            return createFileSystem(path, enableCache);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    protected abstract Pair<JindoHadoopSystem, String> createFileSystem(
            org.apache.hadoop.fs.Path path, boolean enableCache) throws IOException;

    private static class HadoopSeekableInputStream extends SeekableInputStream {

        private static final int MIN_SKIP_BYTES = 1024 * 1024;

        protected final FSDataInputStream in;

        private HadoopSeekableInputStream(FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public void seek(long seekPos) throws IOException {
            // We do some optimizations to avoid that some implementations of distributed FS perform
            // expensive seeks when they are actually not needed.
            long delta = seekPos - getPos();

            if (delta > 0L && delta <= MIN_SKIP_BYTES) {
                // Instead of a small forward seek, we skip over the gap
                skipFully(delta);
            } else if (delta != 0L) {
                // For larger gaps and backward seeks, we do a real seek
                forceSeek(seekPos);
            } // Do nothing if delta is zero.
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public int read() throws IOException {
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        /**
         * Positions the stream to the given location. In contrast to {@link #seek(long)}, this
         * method will always issue a "seek" command to the dfs and may not replace it by {@link
         * #skip(long)} for small seeks.
         *
         * <p>Notice that the underlying DFS implementation can still decide to do skip instead of
         * seek.
         *
         * @param seekPos the position to seek to.
         */
        public void forceSeek(long seekPos) throws IOException {
            in.seek(seekPos);
        }

        /**
         * Skips over a given amount of bytes in the stream.
         *
         * @param bytes the number of bytes to skip.
         */
        public void skipFully(long bytes) throws IOException {
            while (bytes > 0) {
                bytes -= in.skip(bytes);
            }
        }
    }

    private static class VectoredReadableInputStream extends HadoopSeekableInputStream
            implements VectoredReadable {

        private VectoredReadableInputStream(FSDataInputStream in) {
            super(in);
        }

        @Override
        public int pread(long position, byte[] bytes, int off, int len) throws IOException {
            return in.read(position, bytes, off, len);
        }
    }

    private static class HadoopPositionOutputStream extends PositionOutputStream {

        private final FSDataOutputStream out;

        private HadoopPositionOutputStream(FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.hflush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class HadoopFileStatus implements FileStatus {

        private final org.apache.hadoop.fs.FileStatus status;

        private HadoopFileStatus(org.apache.hadoop.fs.FileStatus status) {
            this.status = status;
        }

        @Override
        public long getLen() {
            return status.getLen();
        }

        @Override
        public boolean isDir() {
            return status.isDirectory();
        }

        @Override
        public Path getPath() {
            return new Path(status.getPath().toUri());
        }

        @Override
        public long getModificationTime() {
            return status.getModificationTime();
        }
    }
}
