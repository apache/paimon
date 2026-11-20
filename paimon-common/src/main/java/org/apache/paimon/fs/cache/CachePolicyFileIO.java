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
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;
import org.apache.paimon.utils.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ORIGIN_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;

/**
 * Routes operations between origin and cache FileIOs without depending on a storage backend.
 *
 * <p>The cache uses {@code fs.oss.endpoint}. Operations bypassing cache use {@code
 * io-cache.origin.endpoint} when provided.
 */
public class CachePolicyFileIO implements FileIO, HadoopOptionsProvider {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CachePolicyFileIO.class);

    private final Path tablePath;
    private final CachePolicy policy;
    private final FileIO origin;
    private final CatalogContext originContext;
    private final CatalogContext cacheContext;
    private final Map<String, String> runtimeOptions = new HashMap<>();

    private transient volatile FileIO cache;
    private volatile boolean cacheUnavailable;
    private volatile boolean closed;

    private CachePolicyFileIO(
            Path tablePath,
            CachePolicy policy,
            FileIO origin,
            CatalogContext originContext,
            CatalogContext cacheContext) {
        this.tablePath = tablePath;
        this.policy = policy;
        this.origin = origin;
        this.originContext = originContext;
        this.cacheContext = cacheContext;
    }

    /** Creates the origin FileIO, wrapping it only when the REST cache policy is enabled. */
    public static FileIO create(Path path, CatalogContext context) throws IOException {
        CachePolicy policy = new CachePolicy(context.options());
        CatalogContext originContext =
                withEndpoint(context, context.options().get(IO_CACHE_ORIGIN_ENDPOINT));
        FileIO origin = FileIO.get(path, originContext);
        if (!policy.enabled()) {
            return origin;
        }
        return new CachePolicyFileIO(
                path,
                policy,
                origin,
                originContext,
                withEndpoint(context, context.options().get("fs.oss.endpoint")));
    }

    private static CatalogContext withEndpoint(CatalogContext context, String endpoint) {
        Options options = new Options(context.options().toMap());
        Configuration hadoopConf = new Configuration(context.hadoopConf());
        if (!StringUtils.isNullOrWhitespaceOnly(endpoint)) {
            options.set("fs.oss.endpoint", endpoint);
            hadoopConf.set("fs.oss.endpoint", endpoint);
        }
        // Delegates receive one endpoint. Retain the legacy JindoCache options so that its
        // deprecated, opt-in implementation keeps working independently of endpoint routing.
        for (String key : context.options().keySet()) {
            if (key.startsWith("io-cache.")
                    && !key.equals(IO_CACHE_ENABLED.key())
                    && !key.equals(IO_CACHE_POLICY.key())
                    && !key.equals(IO_CACHE_WHITELIST_PATH.key())) {
                options.remove(key);
            }
        }
        return CatalogContext.create(options, hadoopConf, context.preferIO(), context.fallbackIO());
    }

    /** Returns the origin delegate for committing uploads started by this wrapper. */
    public FileIO originFileIO() throws IOException {
        ensureOpen();
        return origin;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("CachePolicyFileIO is closed.");
        }
    }

    private FileIO fileIO(Path path, String operation) throws IOException {
        ensureOpen();
        boolean useCache = !cacheUnavailable && policy.shouldCache(path, operation);
        LOG.debug(
                "CACHE_POLICY_ROUTE operation={} route={} endpoint={} file_type={} path={}",
                operation,
                useCache ? "cache" : "origin",
                (useCache ? cacheContext : originContext).options().get("fs.oss.endpoint"),
                FileType.classify(path),
                path);
        if (!useCache) {
            return origin;
        }
        FileIO current = cache;
        if (current == null) {
            synchronized (this) {
                ensureOpen();
                if (cacheUnavailable) {
                    return origin;
                }
                current = cache;
                if (current == null) {
                    try {
                        current = FileIO.get(tablePath, cacheContext);
                        current.setRuntimeContext(new HashMap<>(runtimeOptions));
                        // Some SDKs defer all network I/O until an output stream is closed.
                        // Confirm connectivity before returning such a stream or native options.
                        // A false result is valid: a newly written file need not exist yet.
                        current.exists(path);
                    } catch (IOException | RuntimeException | Error e) {
                        if (current != null) {
                            try {
                                current.close();
                            } catch (Exception closeFailure) {
                                e.addSuppressed(closeFailure);
                            }
                        }
                        if (CacheConnectionFailure.isConnectionFailure(e)) {
                            disableCache(path, operation, e);
                            return origin;
                        }
                        throw e;
                    }
                    cache = current;
                }
            }
        }
        return current;
    }

    private synchronized void disableCache(Path path, String operation, Throwable failure) {
        if (!cacheUnavailable) {
            cacheUnavailable = true;
            LOG.warn(
                    "CACHE_POLICY_FALLBACK operation={} cache_endpoint={} origin_endpoint={} path={} failure_type={}; "
                            + "using origin for subsequent operations on this FileIO",
                    operation,
                    cacheContext.options().get("fs.oss.endpoint"),
                    originContext.options().get("fs.oss.endpoint"),
                    path,
                    failure.getClass().getName(),
                    failure);
        }
    }

    private <T> T execute(Path path, String operation, IOAction<T> action) throws IOException {
        FileIO delegate = fileIO(path, operation);
        try {
            return action.apply(delegate);
        } catch (IOException | RuntimeException failure) {
            if (delegate == origin || !CacheConnectionFailure.isConnectionFailure(failure)) {
                throw failure;
            }
            disableCache(path, operation, failure);
            try {
                return action.apply(origin);
            } catch (IOException | RuntimeException originFailure) {
                if (originFailure != failure) {
                    originFailure.addSuppressed(failure);
                }
                throw originFailure;
            }
        }
    }

    @FunctionalInterface
    private interface IOAction<T> {
        T apply(FileIO fileIO) throws IOException;
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return execute(path, "read", io -> io.newInputStream(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return execute(path, "write", io -> io.newOutputStream(path, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return execute(path, "meta", io -> io.getFileStatus(path));
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return execute(path, "meta", io -> io.exists(path));
    }

    @Override
    public long getFileSize(Path path) throws IOException {
        return execute(path, "meta", io -> io.getFileSize(path));
    }

    @Override
    public boolean isDir(Path path) throws IOException {
        return execute(path, "meta", io -> io.isDir(path));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fileIO(path, "list").listStatus(path);
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        return fileIO(path, "list").listFilesIterative(path, recursive);
    }

    @Override
    public FileStatus[] listDirectories(Path path) throws IOException {
        return fileIO(path, "list").listDirectories(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileIO(path, "delete").delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fileIO(path, "mkdirs").mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileIO(src, "rename").rename(src, dst);
    }

    @Override
    public boolean tryToWriteAtomic(Path path, String content) throws IOException {
        return fileIO(path, "atomic-write").tryToWriteAtomic(path, content);
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return fileIO(path, "two-phase-write").newTwoPhaseOutputStream(path, overwrite);
    }

    @Override
    public String createBlobPresignedUrl(
            Path tableRoot, BlobDescriptor descriptor, Duration validity) throws IOException {
        return fileIO(tableRoot, "presign").createBlobPresignedUrl(tableRoot, descriptor, validity);
    }

    @Override
    public Optional<Path> archive(Path path, StorageType type) throws IOException {
        return fileIO(path, "archive").archive(path, type);
    }

    @Override
    public void restoreArchive(Path path, Duration duration) throws IOException {
        fileIO(path, "restore").restoreArchive(path, duration);
    }

    @Override
    public Optional<Path> unarchive(Path path, StorageType type) throws IOException {
        return fileIO(path, "unarchive").unarchive(path, type);
    }

    @Override
    public Options hadoopOptions(Path path, String opType) {
        try {
            FileIO delegate = fileIO(path, opType);
            boolean useCache = delegate != origin;
            while (delegate instanceof ResolvingFileIO) {
                delegate = ((ResolvingFileIO) delegate).fileIO(path);
            }
            if (delegate instanceof HadoopOptionsProvider) {
                return ((HadoopOptionsProvider) delegate).hadoopOptions(path, opType);
            }
            Options options =
                    new Options((useCache ? cacheContext : originContext).options().toMap());
            synchronized (this) {
                runtimeOptions.forEach(options::set);
            }
            return options;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isObjectStore() {
        return origin.isObjectStore();
    }

    @Override
    public void configure(CatalogContext context) {
        throw new UnsupportedOperationException("Use CachePolicyFileIO.create to configure.");
    }

    @Override
    public synchronized void setRuntimeContext(Map<String, String> options) {
        if (closed) {
            throw new IllegalStateException("CachePolicyFileIO is closed.");
        }
        runtimeOptions.putAll(options);
        origin.setRuntimeContext(options);
        if (cache != null) {
            cache.setRuntimeContext(options);
        }
    }

    @Override
    public void close() throws IOException {
        FileIO current;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            current = cache;
            cache = null;
        }
        try (FileIO cacheToClose = current) {
            origin.close();
        }
    }
}
