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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;

/** A {@link FileIO} to support getting token from REST Server. */
public class RESTTokenFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");

    private static final Cache<RESTToken, FileIO> FILE_IO_CACHE =
            Caffeine.newBuilder()
                    .expireAfterAccess(30, TimeUnit.MINUTES)
                    .maximumSize(100)
                    .removalListener(
                            (ignored, value, cause) -> IOUtils.closeQuietly((FileIO) value))
                    .scheduler(
                            Scheduler.forScheduledExecutorService(
                                    Executors.newSingleThreadScheduledExecutor(
                                            ThreadUtils.newDaemonThreadFactory(
                                                    "rest-token-file-io-scheduler"))))
                    .build();

    private final RESTCatalogLoader catalogLoader;
    private final Identifier identifier;
    private final Path path;

    // catalog instance before serialization, it will become null after serialization, then we
    // should create catalog from catalog loader
    private final transient RESTCatalog catalogInstance;

    // the latest token from REST Server, serializable in order to avoid loading token from the REST
    // Server again after serialization
    private volatile RESTToken token;

    public RESTTokenFileIO(
            RESTCatalogLoader catalogLoader,
            RESTCatalog catalogInstance,
            Identifier identifier,
            Path path) {
        this.catalogLoader = catalogLoader;
        this.catalogInstance = catalogInstance;
        this.identifier = identifier;
        this.path = path;
    }

    @Override
    public void configure(CatalogContext context) {
        throw new UnsupportedOperationException("RESTTokenFileIO does not support configuration.");
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return fileIO().newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return fileIO().newOutputStream(path, overwrite);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return fileIO().getFileStatus(path);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fileIO().listStatus(path);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return fileIO().exists(path);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileIO().delete(path, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fileIO().mkdirs(path);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileIO().rename(src, dst);
    }

    @Override
    public boolean isObjectStore() {
        try {
            return fileIO().isObjectStore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FileIO fileIO() throws IOException {
        tryToRefreshToken();

        FileIO fileIO = FILE_IO_CACHE.getIfPresent(token);
        if (fileIO != null) {
            return fileIO;
        }

        synchronized (FILE_IO_CACHE) {
            fileIO = FILE_IO_CACHE.getIfPresent(token);
            if (fileIO != null) {
                return fileIO;
            }

            CatalogContext context = catalogLoader.context();
            Options options = context.options();
            // the original options are not overwritten
            options = new Options(RESTUtil.merge(token.token(), options.toMap()));
            options.set(FILE_IO_ALLOW_CACHE, false);
            context = CatalogContext.create(options, context.preferIO(), context.fallbackIO());
            try {
                fileIO = FileIO.get(path, context);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            FILE_IO_CACHE.put(token, fileIO);
            return fileIO;
        }
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
        return token == null || System.currentTimeMillis() > token.expireAtMillis();
    }

    private void refreshToken() {
        GetTableTokenResponse response;
        if (catalogInstance != null) {
            try {
                response = catalogInstance.loadTableToken(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException(e);
            }
        } else {
            try (RESTCatalog catalog = catalogLoader.load()) {
                response = catalog.loadTableToken(identifier);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        token = new RESTToken(response.getToken(), response.getExpiresAtMillis());
    }

    /**
     * Public interface to get valid token, this can be invoked by native engines to get the token
     * and use own File System.
     */
    public RESTToken validToken() {
        tryToRefreshToken();
        return token;
    }
}
