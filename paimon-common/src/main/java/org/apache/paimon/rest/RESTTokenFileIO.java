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

package org.apache.paimon.rest;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileRange;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.SeekableInputStreamWrapper;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.fs.VectoredReadable;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;

/** A {@link FileIO} to support getting token from REST Server. */
public class RESTTokenFileIO implements FileIO {

    private static final long serialVersionUID = 2L;

    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");

    private static final Cache<RESTToken, CachedFileIO> FILE_IO_CACHE =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .expireAfterAccess(10, TimeUnit.HOURS)
                    .removalListener((ignored, value, cause) -> ((CachedFileIO) value).release())
                    .scheduler(
                            Scheduler.forScheduledExecutorService(
                                    Executors.newSingleThreadScheduledExecutor(
                                            ThreadUtils.newDaemonThreadFactory(
                                                    "rest-token-file-io-scheduler"))))
                    .build();

    private static final Logger LOG = LoggerFactory.getLogger(RESTTokenFileIO.class);

    private final CatalogContext catalogContext;
    private final Identifier identifier;
    private final Path path;

    // Api instance before serialization, it will become null after serialization, then we should
    // create RESTApi from catalogContext
    private transient volatile RESTApi apiInstance;

    // the latest token from REST Server, serializable in order to avoid loading token from the REST
    // Server again after serialization
    private volatile RESTToken token;

    public RESTTokenFileIO(
            CatalogContext catalogContext, RESTApi apiInstance, Identifier identifier, Path path) {
        this.catalogContext = catalogContext;
        this.apiInstance = apiInstance;
        this.identifier = identifier;
        this.path = path;
    }

    @Override
    public void configure(CatalogContext context) {
        throw new UnsupportedOperationException("RESTTokenFileIO does not support configuration.");
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        Lease lease = acquire();
        boolean opened = false;
        try {
            SeekableInputStream delegate = lease.fileIO().newInputStream(path);
            SeekableInputStream in =
                    delegate instanceof VectoredReadable
                            ? new LeasedVectoredInputStream(delegate, lease)
                            : new LeasedSeekableInputStream(delegate, lease);
            opened = true;
            return in;
        } finally {
            if (!opened) {
                lease.close();
            }
        }
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        Lease lease = acquire();
        boolean opened = false;
        try {
            PositionOutputStream out =
                    new LeasedPositionOutputStream(
                            lease.fileIO().newOutputStream(path, overwrite), lease);
            opened = true;
            return out;
        } finally {
            if (!opened) {
                lease.close();
            }
        }
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        Lease lease = acquire();
        boolean opened = false;
        try {
            TwoPhaseOutputStream out =
                    new LeasedTwoPhaseOutputStream(
                            lease.fileIO().newTwoPhaseOutputStream(path, overwrite), lease);
            opened = true;
            return out;
        } finally {
            if (!opened) {
                lease.close();
            }
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().getFileStatus(path);
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().listStatus(path);
        }
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        // the interface default would hide the inner FileIO's iterative listing override
        Lease lease = acquire();
        boolean listing = false;
        try {
            RemoteIterator<FileStatus> iterator =
                    new LeasedRemoteIterator(
                            lease.fileIO().listFilesIterative(path, recursive), lease);
            listing = true;
            return iterator;
        } finally {
            if (!listing) {
                lease.close();
            }
        }
    }

    @Override
    public boolean exists(Path path) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().exists(path);
        }
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().delete(path, recursive);
        }
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().mkdirs(path);
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        try (Lease lease = acquire()) {
            return lease.fileIO().rename(src, dst);
        }
    }

    @Override
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        // the interface default (temp file + rename) would bypass the inner FileIO's atomic
        // override
        try (Lease lease = acquire()) {
            return lease.fileIO().tryToWriteAtomic(path, content);
        }
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        if (!path.equals(tableRoot)) {
            throw new IOException("Table root does not match RESTTokenFileIO bound table root.");
        }
        try (Lease lease = acquire()) {
            return lease.fileIO().createBlobPresignedUrl(tableRoot, descriptor, validity);
        }
    }

    @Override
    public boolean isObjectStore() {
        try (Lease lease = acquire()) {
            return lease.fileIO().isObjectStore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Lease acquire() throws IOException {
        tryToRefreshToken();

        while (true) {
            RESTToken currentToken = token;
            CachedFileIO cached = FILE_IO_CACHE.getIfPresent(currentToken);
            if (cached != null) {
                Lease lease = cached.acquire();
                if (lease != null) {
                    return lease;
                }
                FILE_IO_CACHE.asMap().remove(currentToken, cached);
            }

            synchronized (FILE_IO_CACHE) {
                cached = FILE_IO_CACHE.getIfPresent(currentToken);
                if (cached != null) {
                    Lease lease = cached.acquire();
                    if (lease != null) {
                        return lease;
                    }
                    FILE_IO_CACHE.asMap().remove(currentToken, cached);
                    continue;
                }

                Options options = catalogContext.options();
                options = new Options(RESTUtil.merge(options.toMap(), currentToken.token()));
                options.set(FILE_IO_ALLOW_CACHE, false);
                CatalogContext context =
                        CatalogContext.create(
                                options,
                                catalogContext.hadoopConf(),
                                catalogContext.preferIO(),
                                catalogContext.fallbackIO());
                FileIO fileIO;
                try {
                    fileIO = FileIO.get(path, context);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                cached = new CachedFileIO(fileIO);
                Lease lease = cached.acquire();
                FILE_IO_CACHE.put(currentToken, cached);
                return lease;
            }
        }
    }

    public static Lease lease(FileIO fileIO) throws IOException {
        if (fileIO instanceof RESTTokenFileIO) {
            return ((RESTTokenFileIO) fileIO).acquire();
        }
        return new Lease(fileIO, null);
    }

    /**
     * Pins the cache entry for the life of the process.
     *
     * @deprecated use {@link #acquire()} and work inside the lease.
     */
    @Deprecated
    public FileIO fileIO() throws IOException {
        return acquire().fileIO();
    }

    @VisibleForTesting
    static void invalidateFileIOCache() {
        FILE_IO_CACHE.invalidateAll();
        FILE_IO_CACHE.cleanUp();
    }

    private void tryToRefreshToken() {
        if (shouldRefresh()) {
            synchronized (this) {
                if (shouldRefresh()) {
                    refreshToken();
                }
            }
        }
    }

    private boolean shouldRefresh() {
        return token == null
                || token.expireAtMillis() - System.currentTimeMillis()
                        < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }

    private void refreshToken() {
        LOG.info("begin refresh data token for identifier [{}]", identifier);
        if (apiInstance == null) {
            apiInstance = new RESTApi(catalogContext.options(), false);
        }
        Identifier tableIdentifier = identifier;
        if (identifier.isSystemTable()) {
            tableIdentifier =
                    new Identifier(
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            identifier.getBranchName());
        }
        GetTableTokenResponse response = apiInstance.loadTableToken(tableIdentifier);
        LOG.info(
                "end refresh data token for identifier [{}] expiresAtMillis [{}]",
                identifier,
                response.getExpiresAtMillis());

        token =
                new RESTToken(
                        mergeTokenWithCatalogOptions(response.getToken()),
                        response.getExpiresAtMillis());
    }

    private Map<String, String> mergeTokenWithCatalogOptions(Map<String, String> token) {
        Map<String, String> newToken = Maps.newLinkedHashMap(token);
        Options catalogOptions = catalogContext.options();
        // DLF OSS endpoint should override the standard OSS endpoint.
        String dlfOssEndpoint = catalogOptions.get(DLF_OSS_ENDPOINT.key());
        if (dlfOssEndpoint != null && !dlfOssEndpoint.isEmpty()) {
            newToken.put("fs.oss.endpoint", dlfOssEndpoint);
        }
        if (catalogOptions.contains(IO_CACHE_ENABLED)) {
            newToken.put(
                    IO_CACHE_ENABLED.key(), String.valueOf(catalogOptions.get(IO_CACHE_ENABLED)));
        }
        return ImmutableMap.copyOf(newToken);
    }

    /**
     * Public interface to get valid token, this can be invoked by native engines to get the token
     * and use own File System.
     */
    public RESTToken validToken() {
        tryToRefreshToken();
        return token;
    }

    /** A cached {@link FileIO} and the number of references still outstanding on it. */
    @VisibleForTesting
    static class CachedFileIO {

        private final FileIO fileIO;
        private final AtomicInteger references = new AtomicInteger(1);

        CachedFileIO(FileIO fileIO) {
            this.fileIO = fileIO;
        }

        @Nullable
        Lease acquire() {
            while (true) {
                int current = references.get();
                if (current <= 0) {
                    return null;
                }
                if (references.compareAndSet(current, current + 1)) {
                    return new Lease(fileIO, this);
                }
            }
        }

        void release() {
            if (references.decrementAndGet() == 0) {
                IOUtils.closeQuietly(fileIO);
            }
        }
    }

    /** The right to use a {@link FileIO} until this lease is closed. */
    public static class Lease implements Closeable {

        private final FileIO fileIO;
        @Nullable private final CachedFileIO cached;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(FileIO fileIO, @Nullable CachedFileIO cached) {
            this.fileIO = fileIO;
            this.cached = cached;
        }

        public FileIO fileIO() {
            return fileIO;
        }

        @Override
        public void close() {
            if (cached != null && released.compareAndSet(false, true)) {
                cached.release();
            }
        }
    }

    private static class LeasedSeekableInputStream extends SeekableInputStreamWrapper {

        private final Lease lease;

        private LeasedSeekableInputStream(SeekableInputStream in, Lease lease) {
            super(in);
            this.lease = lease;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                lease.close();
            }
        }
    }

    private static class LeasedVectoredInputStream extends LeasedSeekableInputStream
            implements VectoredReadable {

        private LeasedVectoredInputStream(SeekableInputStream in, Lease lease) {
            super(in, lease);
        }

        private VectoredReadable vectored() {
            return (VectoredReadable) in;
        }

        @Override
        public int pread(long position, byte[] buffer, int offset, int length) throws IOException {
            return vectored().pread(position, buffer, offset, length);
        }

        @Override
        public void preadFully(long position, byte[] buffer, int offset, int length)
                throws IOException {
            vectored().preadFully(position, buffer, offset, length);
        }

        @Override
        public int minSeekForVectorReads() {
            return vectored().minSeekForVectorReads();
        }

        @Override
        public int batchSizeForVectorReads() {
            return vectored().batchSizeForVectorReads();
        }

        @Override
        public int parallelismForVectorReads() {
            return vectored().parallelismForVectorReads();
        }

        @Override
        public void readVectored(List<? extends FileRange> ranges) throws IOException {
            vectored().readVectored(ranges);
        }
    }

    private static class LeasedPositionOutputStream extends PositionOutputStreamWrapper {

        private final Lease lease;

        private LeasedPositionOutputStream(PositionOutputStream out, Lease lease) {
            super(out);
            this.lease = lease;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                lease.close();
            }
        }
    }

    private static class LeasedTwoPhaseOutputStream extends TwoPhaseOutputStream {

        private final TwoPhaseOutputStream out;
        private final Lease lease;

        private LeasedTwoPhaseOutputStream(TwoPhaseOutputStream out, Lease lease) {
            this.out = out;
            this.lease = lease;
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
            out.flush();
        }

        @Override
        public Committer closeForCommit() throws IOException {
            try {
                return out.closeForCommit();
            } finally {
                lease.close();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } finally {
                lease.close();
            }
        }
    }

    private static class LeasedRemoteIterator implements RemoteIterator<FileStatus> {

        private final RemoteIterator<FileStatus> iterator;
        private final Lease lease;

        private LeasedRemoteIterator(RemoteIterator<FileStatus> iterator, Lease lease) {
            this.iterator = iterator;
            this.lease = lease;
        }

        @Override
        public boolean hasNext() throws IOException {
            boolean hasNext;
            try {
                hasNext = iterator.hasNext();
            } catch (Throwable t) {
                lease.close();
                throw t;
            }
            if (!hasNext) {
                lease.close();
            }
            return hasNext;
        }

        @Override
        public FileStatus next() throws IOException {
            try {
                return iterator.next();
            } catch (Throwable t) {
                lease.close();
                throw t;
            }
        }
    }
}
