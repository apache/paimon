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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;

/** A {@link FileIO} to support getting token from REST Server. */
public class RESTTokenFileIO implements FileIO {

    private static final long serialVersionUID = 2L;

    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");

    private static final Cache<RESTToken, FileIO> FILE_IO_CACHE =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .expireAfterAccess(10, TimeUnit.HOURS)
                    .removalListener(
                            (ignored, value, cause) -> IOUtils.closeQuietly((FileIO) value))
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
        return fileIO().newInputStream(path);
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return fileIO().newOutputStream(path, overwrite);
    }

    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return fileIO().newTwoPhaseOutputStream(path, overwrite);
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

    public FileIO fileIO() throws IOException {
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

            Options options = catalogContext.options();
            options = new Options(RESTUtil.merge(options.toMap(), token.token()));
            options.set(FILE_IO_ALLOW_CACHE, false);
            CatalogContext context =
                    CatalogContext.create(
                            options,
                            catalogContext.hadoopConf(),
                            catalogContext.preferIO(),
                            catalogContext.fallbackIO());
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
        // DLF OSS endpoint should override the standard OSS endpoint.
        String dlfOssEndpoint = catalogContext.options().get(DLF_OSS_ENDPOINT.key());
        if (dlfOssEndpoint != null && !dlfOssEndpoint.isEmpty()) {
            newToken.put("fs.oss.endpoint", dlfOssEndpoint);
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
}
