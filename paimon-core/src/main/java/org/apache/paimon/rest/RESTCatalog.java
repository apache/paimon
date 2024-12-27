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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.auth.CredentialsProvider;
import org.apache.paimon.rest.auth.CredentialsProviderFactory;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreatePartitionRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.DropPartitionRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.SuccessResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.annotations.VisibleForTesting;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;
import static org.apache.paimon.rest.RESTCatalogOptions.METASTORE_PARTITIONED;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.ThreadPoolUtils.createScheduledThreadPool;

/** A catalog implementation for REST. */
public class RESTCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(RESTCatalog.class);
    private static final ObjectMapper OBJECT_MAPPER = RESTObjectMapper.create();

    private final RESTClient client;
    private final ResourcePaths resourcePaths;
    private final AuthSession catalogAuth;
    private final CatalogContext context;
    private final FileIO fileIO;

    private volatile ScheduledExecutorService refreshExecutor = null;

    public RESTCatalog(CatalogContext catalogContext) {
        Options catalogOptions = catalogContext.options();
        if (catalogOptions.getOptional(CatalogOptions.WAREHOUSE).isPresent()) {
            throw new IllegalArgumentException("Can not config warehouse in RESTCatalog.");
        }
        String uri = catalogOptions.get(RESTCatalogOptions.URI);
        Optional<Duration> connectTimeout =
                catalogOptions.getOptional(RESTCatalogOptions.CONNECTION_TIMEOUT);
        Optional<Duration> readTimeout =
                catalogOptions.getOptional(RESTCatalogOptions.READ_TIMEOUT);
        Integer threadPoolSize = catalogOptions.get(RESTCatalogOptions.THREAD_POOL_SIZE);
        HttpClientOptions httpClientOptions =
                new HttpClientOptions(
                        uri,
                        connectTimeout,
                        readTimeout,
                        OBJECT_MAPPER,
                        threadPoolSize,
                        DefaultErrorHandler.getInstance());
        this.client = new HttpClient(httpClientOptions);
        Map<String, String> baseHeader = configHeaders(catalogOptions.toMap());
        CredentialsProvider credentialsProvider =
                CredentialsProviderFactory.createCredentialsProvider(
                        catalogOptions, RESTCatalog.class.getClassLoader());
        if (credentialsProvider.keepRefreshed()) {
            this.catalogAuth =
                    AuthSession.fromRefreshCredentialsProvider(
                            tokenRefreshExecutor(), baseHeader, credentialsProvider);

        } else {
            this.catalogAuth = new AuthSession(baseHeader, credentialsProvider);
        }
        Map<String, String> initHeaders =
                RESTUtil.merge(
                        configHeaders(catalogOptions.toMap()), this.catalogAuth.getHeaders());
        Options options =
                new Options(fetchOptionsFromServer(initHeaders, catalogContext.options().toMap()));
        this.context =
                CatalogContext.create(
                        options, catalogContext.preferIO(), catalogContext.fallbackIO());
        this.resourcePaths =
                ResourcePaths.forCatalogProperties(options.get(RESTCatalogInternalOptions.PREFIX));
        this.fileIO = getFileIOFromOptions(context);
    }

    private static FileIO getFileIOFromOptions(CatalogContext context) {
        try {
            Options options = context.options();
            String warehouseStr = options.get(CatalogOptions.WAREHOUSE);
            Path warehousePath = new Path(warehouseStr);
            CatalogContext contextWithNewOptions =
                    CatalogContext.create(options, context.preferIO(), context.fallbackIO());
            return FileIO.get(warehousePath, contextWithNewOptions);
        } catch (IOException e) {
            LOG.warn("Can not get FileIO from options.");
            throw new RuntimeException(e);
        }
    }

    @Override
    public String warehouse() {
        return context.options().get(CatalogOptions.WAREHOUSE);
    }

    @Override
    public Map<String, String> options() {
        return context.options().toMap();
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
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
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        try {
            client.post(
                    resourcePaths.databases(), request, CreateDatabaseResponse.class, headers());
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name);
            }
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        }
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        if (isSystemDatabase(name)) {
            return Database.of(name);
        }
        try {
            GetDatabaseResponse response =
                    client.get(resourcePaths.database(name), GetDatabaseResponse.class, headers());
            return new Database.DatabaseImpl(
                    name, response.options(), response.comment().orElseGet(() -> null));
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(name);
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        checkNotSystemDatabase(name);
        try {
            if (!cascade && !this.listTables(name).isEmpty()) {
                throw new DatabaseNotEmptyException(name);
            }
            client.delete(resourcePaths.database(name), headers());
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name);
            }
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        }
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        checkNotSystemDatabase(name);
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
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name);
            }
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        ListTablesResponse response =
                client.get(resourcePaths.tables(databaseName), ListTablesResponse.class, headers());
        if (response.getTables() != null) {
            return response.getTables();
        }
        return ImmutableList.of();
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        if (SYSTEM_DATABASE_NAME.equals(identifier.getDatabaseName())) {
            throw new UnsupportedOperationException("TODO support global system tables.");
        } else if (identifier.isSystemTable()) {
            return getSystemTable(identifier);
        } else {
            return getDataOrFormatTable(identifier);
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        try {
            CreateTableRequest request = new CreateTableRequest(identifier, schema);
            client.post(
                    resourcePaths.tables(identifier.getDatabaseName()),
                    request,
                    GetTableResponse.class,
                    headers());
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(identifier);
            }
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        try {
            RenameTableRequest request = new RenameTableRequest(toTable);
            client.post(
                    resourcePaths.renameTable(
                            fromTable.getDatabaseName(), fromTable.getTableName()),
                    request,
                    GetTableResponse.class,
                    headers());
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(fromTable);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(fromTable, e);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException(toTable);
        }
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        try {
            AlterTableRequest request = new AlterTableRequest(changes);
            client.post(
                    resourcePaths.table(identifier.getDatabaseName(), identifier.getTableName()),
                    request,
                    GetTableResponse.class,
                    headers());
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(identifier);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        try {
            client.delete(
                    resourcePaths.table(identifier.getDatabaseName(), identifier.getTableName()),
                    headers());
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(identifier);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void createPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        try {
            CreatePartitionRequest request = new CreatePartitionRequest(identifier, partitionSpec);
            client.post(
                    resourcePaths.partitions(
                            identifier.getDatabaseName(), identifier.getTableName()),
                    request,
                    SuccessResponse.class,
                    headers());
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    // todo: if table not exist how to drop partition
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {
        checkNotSystemTable(identifier, "dropPartition");
        dropPartitionMetadata(identifier, partitions);
        Table table = getTable(identifier);
        if (table != null) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            try (FileStoreCommit commit =
                    fileStoreTable
                            .store()
                            .newCommit(
                                    createCommitUser(
                                            fileStoreTable.coreOptions().toConfiguration()))) {
                commit.dropPartitions(
                        Collections.singletonList(partitions), BatchWriteBuilder.COMMIT_IDENTIFIER);
            }
        }
    }

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        boolean whetherSupportListPartitions = context.options().get(METASTORE_PARTITIONED);
        if (whetherSupportListPartitions) {
            return listPartitionsFromServer(identifier);
        } else {
            return getTable(identifier).newReadBuilder().newScan().listPartitionEntries();
        }
    }

    @Override
    public boolean caseSensitive() {
        return context.options().getOptional(CASE_SENSITIVE).orElse(true);
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
    Map<String, String> fetchOptionsFromServer(
            Map<String, String> headers, Map<String, String> clientProperties) {
        ConfigResponse response =
                client.get(ResourcePaths.V1_CONFIG, ConfigResponse.class, headers);
        return response.merge(clientProperties);
    }

    @VisibleForTesting
    Table getDataOrFormatTable(Identifier identifier) throws TableNotExistException {
        Preconditions.checkArgument(identifier.getSystemTableName() == null);
        GetTableResponse response = getTableResponse(identifier);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        fileIO(),
                        new Path(response.getPath()),
                        TableSchema.create(response.getSchemaId(), response.getSchema()),
                        new CatalogEnvironment(identifier, null, Lock.emptyFactory(), null));
        CoreOptions options = table.coreOptions();
        if (options.type() == TableType.OBJECT_TABLE) {
            String objectLocation = options.objectLocation();
            checkNotNull(objectLocation, "Object location should not be null for object table.");
            table =
                    ObjectTable.builder()
                            .underlyingTable(table)
                            .objectLocation(objectLocation)
                            .objectFileIO(this.fileIO())
                            .build();
        }
        return table;
    }

    @VisibleForTesting
    public List<PartitionEntry> listPartitionsFromServer(Identifier identifier)
            throws TableNotExistException {
        try {
            ListPartitionsResponse response =
                    client.get(
                            resourcePaths.partitions(
                                    identifier.getDatabaseName(), identifier.getTableName()),
                            ListPartitionsResponse.class,
                            headers());
            List<PartitionEntry> partitionEntries = new ArrayList<>();
            for (ListPartitionsResponse.Partition partition : response.getPartitions()) {
                InternalRowSerializer serializer =
                        new InternalRowSerializer(partition.getPartitionType());
                GenericRow row =
                        convertSpecToInternalRow(
                                partition.getSpec(), partition.getPartitionType(), null);
                PartitionEntry partitionEntry =
                        new PartitionEntry(
                                serializer.toBinaryRow(row).copy(),
                                partition.getRecordCount(),
                                partition.getFileSizeInBytes(),
                                partition.getFileCount(),
                                partition.getLastFileCreationTime());
                partitionEntries.add(partitionEntry);
            }
            return partitionEntries;
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    protected GetTableResponse getTableResponse(Identifier identifier)
            throws TableNotExistException {
        try {
            return client.get(
                    resourcePaths.table(identifier.getDatabaseName(), identifier.getTableName()),
                    GetTableResponse.class,
                    headers());
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    protected SuccessResponse dropPartitionMetadata(
            Identifier identifier, Map<String, String> partitions) throws TableNotExistException {
        try {
            DropPartitionRequest request = new DropPartitionRequest(partitions);
            return client.delete(
                    resourcePaths.partitions(
                            identifier.getDatabaseName(), identifier.getTableName()),
                    request,
                    headers());
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    private static Map<String, String> configHeaders(Map<String, String> properties) {
        return RESTUtil.extractPrefixMap(properties, "header.");
    }

    private Map<String, String> headers() {
        return catalogAuth.getHeaders();
    }

    private Table getSystemTable(Identifier identifier) throws TableNotExistException {
        Table originTable =
                getDataOrFormatTable(
                        new Identifier(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                identifier.getBranchName(),
                                null));
        return CatalogUtils.createSystemTable(identifier, originTable);
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
