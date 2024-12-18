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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.auth.CredentialsProvider;
import org.apache.paimon.rest.auth.CredentialsProviderFactory;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;
import static org.apache.paimon.utils.ThreadPoolUtils.createScheduledThreadPool;

/** A catalog implementation for REST. */
public class RESTCatalog extends AbstractCatalog {

    private static final ObjectMapper OBJECT_MAPPER = RESTObjectMapper.create();

    private final RESTClient client;
    private final ResourcePaths resourcePaths;
    private final Options options;
    private final AuthSession catalogAuth;

    private volatile ScheduledExecutorService refreshExecutor = null;

    public RESTCatalog(CatalogContext context) {
        this(context, getClient(context.options()), getCredentialsProvider(context.options()));
    }

    public RESTCatalog(
            CatalogContext context, RESTClient client, CredentialsProvider credentialsProvider) {
        this(
                context,
                client,
                credentialsProvider,
                new Options(
                        fetchOptionsFromServer(
                                client,
                                RESTUtil.merge(
                                        configHeaders(context.options().toMap()),
                                        credentialsProvider.authHeader()),
                                context.options().toMap())));
    }

    public RESTCatalog(
            CatalogContext context,
            RESTClient client,
            CredentialsProvider credentialsProvider,
            Options optionsWithServer) {
        super(getFileIOFromOptions(context, optionsWithServer), optionsWithServer);
        this.client = client;
        Map<String, String> baseHeader = configHeaders(optionsWithServer.toMap());
        if (credentialsProvider.keepRefreshed()) {
            this.catalogAuth =
                    AuthSession.fromRefreshCredentialsProvider(
                            tokenRefreshExecutor(), baseHeader, credentialsProvider);

        } else {
            this.catalogAuth = new AuthSession(baseHeader, credentialsProvider);
        }
        this.options = optionsWithServer;
        this.resourcePaths =
                ResourcePaths.forCatalogProperties(
                        this.options.get(RESTCatalogInternalOptions.PREFIX));
    }

    @Override
    public String warehouse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listDatabases() {
        ListDatabasesResponse response =
                client.get(resourcePaths.databases(), ListDatabasesResponse.class, headers());
        if (response.getDatabases() != null) {
            return response.getDatabases();
        }
        return ImmutableList.of();
    }

    @Override
    protected void createDatabaseImpl(String name, Map<String, String> properties) {
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        client.post(resourcePaths.databases(), request, CreateDatabaseResponse.class, headers());
    }

    @Override
    protected Database getDatabaseImpl(String name) throws DatabaseNotExistException {
        try {
            GetDatabaseResponse response =
                    client.get(resourcePaths.database(name), GetDatabaseResponse.class, headers());
            return new Database.DatabaseImpl(
                    name, response.options(), response.comment().orElseGet(() -> null));
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(name);
        }
    }

    @Override
    protected void dropDatabaseImpl(String name) {
        client.delete(resourcePaths.database(name), headers());
    }

    @Override
    protected void alterDatabaseImpl(String name, List<PropertyChange> changes)
            throws DatabaseNotExistException {
        try {
            Pair<Map<String, String>, Set<String>> setPropertiesToRemoveKeys =
                    PropertyChange.getSetPropertiesToRemoveKeys(changes);
            Map<String, String> updateProperties = setPropertiesToRemoveKeys.getLeft();
            Set<String> removeKeys = setPropertiesToRemoveKeys.getRight();
            AlterDatabaseRequest request =
                    new AlterDatabaseRequest(new ArrayList<>(removeKeys), updateProperties);
            AlterDatabaseResponse response =
                    client.post(
                            resourcePaths.databaseProperties(name),
                            request,
                            AlterDatabaseResponse.class,
                            headers());
            if (response.getUpdated().isEmpty()) {
                throw new IllegalStateException("Failed to update properties");
            }
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(name);
        }
    }

    @Override
    protected List<String> listTablesImpl(String databaseName) {
        ListTablesResponse response =
                client.get(resourcePaths.tables(databaseName), ListTablesResponse.class, headers());
        if (response.getTables() != null) {
            return response.getTables();
        }
        return ImmutableList.of();
    }

    @Override
    protected TableSchema getDataTableSchema(Identifier identifier) throws TableNotExistException {
        try {
            GetTableResponse response =
                    client.get(
                            resourcePaths.table(
                                    identifier.getDatabaseName(), identifier.getTableName()),
                            GetTableResponse.class,
                            headers());
            if (response.getSchema() != null) {
                return response.getSchema();
            }
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        }
        throw new TableNotExistException(identifier);
    }

    @Override
    protected void createTableImpl(Identifier identifier, Schema schema) {}

    @Override
    protected void renameTableImpl(Identifier fromTable, Identifier toTable) {}

    @Override
    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {}

    @Override
    protected void dropTableImpl(Identifier identifier) {}

    @Override
    public boolean caseSensitive() {
        return options.getOptional(CASE_SENSITIVE).orElse(true);
    }

    @Override
    public void close() throws Exception {
        if (refreshExecutor != null) {
            refreshExecutor.shutdownNow();
        }
        if (client != null) {
            client.close();
        }
    }

    @VisibleForTesting
    static Map<String, String> fetchOptionsFromServer(
            RESTClient client, Map<String, String> headers, Map<String, String> clientProperties) {
        ConfigResponse response =
                client.get(ResourcePaths.V1_CONFIG, ConfigResponse.class, headers);
        return response.merge(clientProperties);
    }

    @VisibleForTesting
    static RESTClient getClient(Options options) {
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
                        OBJECT_MAPPER,
                        threadPoolSize,
                        DefaultErrorHandler.getInstance());
        return new HttpClient(httpClientOptions);
    }

    // todo: whether it's ok
    private static FileIO getFileIOFromOptions(CatalogContext context, Options options) {
        String warehouseStr = options.get(CatalogOptions.WAREHOUSE);
        Path warehousePath = new Path(warehouseStr);
        FileIO fileIO;
        CatalogContext contextWithNewOptions =
                CatalogContext.create(options, context.preferIO(), context.fallbackIO());
        try {
            fileIO = FileIO.get(warehousePath, contextWithNewOptions);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fileIO;
    }

    private static CredentialsProvider getCredentialsProvider(Options options) {
        return CredentialsProviderFactory.createCredentialsProvider(
                options, RESTCatalog.class.getClassLoader());
    }

    private static Map<String, String> configHeaders(Map<String, String> properties) {
        return RESTUtil.extractPrefixMap(properties, "header.");
    }

    private Map<String, String> headers() {
        return catalogAuth.getHeaders();
    }

    private ScheduledExecutorService tokenRefreshExecutor() {
        if (refreshExecutor == null) {
            synchronized (this) {
                if (refreshExecutor == null) {
                    this.refreshExecutor = createScheduledThreadPool(1, "token-refresh-thread");
                }
            }
        }

        return refreshExecutor;
    }
}
