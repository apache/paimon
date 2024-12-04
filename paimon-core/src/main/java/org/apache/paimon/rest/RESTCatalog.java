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
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthOptions;
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.auth.BearTokenCredentialsProvider;
import org.apache.paimon.rest.auth.BearTokenFileCredentialsProvider;
import org.apache.paimon.rest.auth.CredentialsProvider;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.guava30.com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** A catalog implementation for REST. */
public class RESTCatalog implements Catalog {
    private RESTClient client;
    private ResourcePaths resourcePaths;
    private Map<String, String> options;
    private Map<String, String> baseHeader;
    // a lazy thread pool for token refresh
    private AuthSession catalogAuth = null;
    private volatile ScheduledExecutorService refreshExecutor = null;
    private boolean keepTokenRefreshed;

    private static final ObjectMapper objectMapper = RESTObjectMapper.create();

    public RESTCatalog(Options options) {
        if (options.getOptional(CatalogOptions.WAREHOUSE).isPresent()) {
            throw new IllegalArgumentException("Can not config warehouse in RESTCatalog.");
        }
        String uri = options.get(RESTCatalogOptions.URI);
        Optional<Duration> connectTimeout =
                options.getOptional(RESTCatalogOptions.CONNECTION_TIMEOUT);
        Optional<Duration> readTimeout = options.getOptional(RESTCatalogOptions.READ_TIMEOUT);
        Integer threadPoolSize = options.get(RESTCatalogOptions.THREAD_POOL_SIZE);
        HttpClientOptions httpClientOptions =
                new HttpClientOptions(
                        uri,
                        connectTimeout,
                        readTimeout,
                        objectMapper,
                        threadPoolSize,
                        DefaultErrorHandler.getInstance());
        this.client = new HttpClient(httpClientOptions);
        this.baseHeader = configHeaders(options.toMap());
        // todo: support create CredentialsProvider by conf
        if (options.getOptional(RESTCatalogOptions.TOKEN).isPresent()) {
            CredentialsProvider credentialsProvider =
                    new BearTokenCredentialsProvider(options.get(RESTCatalogOptions.TOKEN));
            this.catalogAuth = new AuthSession(this.baseHeader, credentialsProvider);
        } else if (options.getOptional(AuthOptions.TOKEN_FILE_PATH).isPresent()) {
            this.keepTokenRefreshed = options.get(AuthOptions.TOKEN_REFRESH_ENABLED);
            long tokenExpireInMills = options.get(AuthOptions.TOKEN_EXPIRES_IN).toMillis();
            String tokenFilePath = options.getOptional(AuthOptions.TOKEN_FILE_PATH).orElse(null);
            long tokenExpireAtMills = System.currentTimeMillis() + tokenExpireInMills;
            CredentialsProvider credentialsProvider =
                    new BearTokenFileCredentialsProvider(
                            tokenFilePath,
                            keepTokenRefreshed,
                            tokenExpireAtMills,
                            tokenExpireInMills);
            this.catalogAuth =
                    AuthSession.fromTokenPath(
                            tokenRefreshExecutor(),
                            this.baseHeader,
                            credentialsProvider,
                            tokenExpireAtMills);
        }
        if (this.catalogAuth != null) {
            Map<String, String> initHeaders =
                    RESTUtil.merge(configHeaders(options.toMap()), this.catalogAuth.getHeaders());
            this.options = fetchOptionsFromServer(initHeaders, options.toMap());
            this.resourcePaths =
                    ResourcePaths.forCatalogProperties(
                            this.options.get(RESTCatalogInternalOptions.PREFIX));
        } else {
            throw new IllegalArgumentException("No auth provider provided.");
        }
    }

    @Override
    public String warehouse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> options() {
        return this.options;
    }

    @Override
    public FileIO fileIO() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listDatabases() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getTableLocation(Identifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {}

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allowUpperCase() {
        return false;
    }

    @Override
    public void close() throws Exception {}

    @VisibleForTesting
    Map<String, String> fetchOptionsFromServer(
            Map<String, String> headers, Map<String, String> clientProperties) {
        ConfigResponse response =
                client.get(ResourcePaths.V1_CONFIG, ConfigResponse.class, headers);
        return response.merge(clientProperties);
    }

    private static Map<String, String> configHeaders(Map<String, String> properties) {
        return RESTUtil.extractPrefixMap(properties, "header.");
    }

    private Map<String, String> headers() {
        catalogAuth.refresh();
        return catalogAuth.getHeaders();
    }

    private ScheduledExecutorService tokenRefreshExecutor() {
        if (!keepTokenRefreshed) {
            return null;
        }

        if (refreshExecutor == null) {
            synchronized (this) {
                if (refreshExecutor == null) {
                    this.refreshExecutor =
                            // todo: move to ThreadPoolUtil
                            new ScheduledThreadPoolExecutor(
                                    1,
                                    new ThreadFactoryBuilder()
                                            .setDaemon(true)
                                            .setNameFormat("token-refresh-thread")
                                            .build());
                }
            }
        }

        return refreshExecutor;
    }
}
