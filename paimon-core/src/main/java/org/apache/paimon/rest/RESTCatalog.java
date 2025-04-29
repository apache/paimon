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

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.auth.AuthSession;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.ForwardBranchRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.rest.responses.PagedResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.listPartitionsFromFileSystem;
import static org.apache.paimon.catalog.CatalogUtils.validateAutoCreateClose;
import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTUtil.extractPrefixMap;
import static org.apache.paimon.rest.auth.AuthSession.createAuthSession;
import static org.apache.paimon.utils.ThreadPoolUtils.createScheduledThreadPool;

/** A catalog implementation for REST. */
public class RESTCatalog implements Catalog {

    public static final String HEADER_PREFIX = "header.";
    public static final String MAX_RESULTS = "maxResults";
    public static final String PAGE_TOKEN = "pageToken";
    public static final String QUERY_PARAMETER_WAREHOUSE_KEY = "warehouse";

    public static final String TABLE_NAME_PATTERN = "tableNamePattern";
    public static final String VIEW_NAME_PATTERN = "viewNamePattern";
    public static final String PARTITION_NAME_PATTERN = "partitionNamePattern";

    private final RESTClient client;
    private final ResourcePaths resourcePaths;
    private final CatalogContext context;
    private final boolean dataTokenEnabled;
    private final RESTAuthFunction restAuthFunction;

    private volatile ScheduledExecutorService refreshExecutor = null;

    public RESTCatalog(CatalogContext context) {
        this(context, true);
    }

    public RESTCatalog(CatalogContext context, boolean configRequired) {
        this.client = new HttpClient(context.options().get(RESTCatalogOptions.URI));
        AuthSession catalogAuth = createAuthSession(context.options(), tokenRefreshExecutor());
        Options options = context.options();
        Map<String, String> baseHeaders = Collections.emptyMap();
        if (configRequired) {
            String warehouse = options.get(WAREHOUSE);
            Map<String, String> queryParams =
                    StringUtils.isNotEmpty(warehouse)
                            ? ImmutableMap.of(
                                    QUERY_PARAMETER_WAREHOUSE_KEY, RESTUtil.encodeString(warehouse))
                            : ImmutableMap.of();
            baseHeaders = extractPrefixMap(context.options(), HEADER_PREFIX);
            options =
                    new Options(
                            client.get(
                                            ResourcePaths.config(),
                                            queryParams,
                                            ConfigResponse.class,
                                            new RESTAuthFunction(
                                                    Collections.emptyMap(), catalogAuth))
                                    .merge(context.options().toMap()));
            baseHeaders.putAll(extractPrefixMap(options, HEADER_PREFIX));
        }
        this.restAuthFunction = new RESTAuthFunction(baseHeaders, catalogAuth);
        context = CatalogContext.create(options, context.preferIO(), context.fallbackIO());
        this.context = context;
        this.resourcePaths = ResourcePaths.forCatalogProperties(options);
        this.dataTokenEnabled = options.get(RESTTokenFileIO.DATA_TOKEN_ENABLED);
    }

    @Override
    public Map<String, String> options() {
        return context.options().toMap();
    }

    @Override
    public RESTCatalogLoader catalogLoader() {
        return new RESTCatalogLoader(context);
    }

    @Override
    public List<String> listDatabases() {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.databases(),
                                queryParams,
                                ListDatabasesResponse.class,
                                restAuthFunction));
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults, @Nullable String pageToken) {
        ListDatabasesResponse response =
                client.get(
                        resourcePaths.databases(),
                        buildPagedQueryParams(maxResults, pageToken),
                        ListDatabasesResponse.class,
                        restAuthFunction);
        List<String> databases = response.getDatabases();
        if (databases == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(databases, response.getNextPageToken());
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        try {
            client.post(resourcePaths.databases(), request, restAuthFunction);
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(name);
            }
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        if (isSystemDatabase(name)) {
            return Database.of(name);
        }
        try {
            GetDatabaseResponse response =
                    client.get(
                            resourcePaths.database(name),
                            GetDatabaseResponse.class,
                            restAuthFunction);
            Map<String, String> options = new HashMap<>(response.getOptions());
            options.put(DB_LOCATION_PROP, response.getLocation());
            response.putAuditOptionsTo(options);
            return new Database.DatabaseImpl(name, options, options.get(COMMENT_PROP));
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
            client.delete(resourcePaths.database(name), restAuthFunction);
        } catch (NoSuchResourceException | DatabaseNotExistException e) {
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
            client.post(
                    resourcePaths.database(name),
                    request,
                    AlterDatabaseResponse.class,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(name);
            }
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(name, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        try {
            if (isSystemDatabase(databaseName)) {
                return SystemTableLoader.loadGlobalTableNames();
            }
            return listDataFromPageApi(
                    queryParams ->
                            client.get(
                                    resourcePaths.tables(databaseName),
                                    queryParams,
                                    ListTablesResponse.class,
                                    restAuthFunction));
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern)
            throws DatabaseNotExistException {
        try {
            ListTablesResponse response =
                    client.get(
                            resourcePaths.tables(databaseName),
                            buildPagedQueryParams(
                                    maxResults, pageToken, TABLE_NAME_PATTERN, tableNamePattern),
                            ListTablesResponse.class,
                            restAuthFunction);
            List<String> tables = response.getTables();
            if (tables == null) {
                return new PagedList<>(emptyList(), null);
            }
            return new PagedList<>(tables, response.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String db,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern)
            throws DatabaseNotExistException {
        try {
            ListTableDetailsResponse response =
                    client.get(
                            resourcePaths.tableDetails(db),
                            buildPagedQueryParams(
                                    maxResults, pageToken, TABLE_NAME_PATTERN, tableNamePattern),
                            ListTableDetailsResponse.class,
                            restAuthFunction);
            List<GetTableResponse> tables = response.getTableDetails();
            if (tables == null) {
                return new PagedList<>(emptyList(), null);
            }
            return new PagedList<>(
                    tables.stream().map(t -> toTable(db, t)).collect(Collectors.toList()),
                    response.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(db);
        }
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        return CatalogUtils.loadTable(
                this,
                identifier,
                path -> fileIOForData(path, identifier),
                this::fileIOFromOptions,
                this::loadTableMetadata,
                null,
                null);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        GetTableSnapshotResponse response;
        try {
            response =
                    client.get(
                            resourcePaths.tableSnapshot(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            GetTableSnapshotResponse.class,
                            restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_SNAPSHOT)) {
                return Optional.empty();
            }
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }

        return Optional.of(response.getSnapshot());
    }

    @Override
    public boolean supportsListObjectsPaged() {
        return true;
    }

    @Override
    public boolean supportsListByPattern() {
        return true;
    }

    @Override
    public boolean supportsVersionManagement() {
        return true;
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier, Snapshot snapshot, List<PartitionStatistics> statistics)
            throws TableNotExistException {
        CommitTableRequest request = new CommitTableRequest(snapshot, statistics);
        CommitTableResponse response;

        try {
            response =
                    client.post(
                            resourcePaths.commitTable(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            request,
                            CommitTableResponse.class,
                            restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        return response.isSuccess();
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant)
            throws Catalog.TableNotExistException {
        RollbackTableRequest request = new RollbackTableRequest(instant);
        try {
            client.post(
                    resourcePaths.rollbackTable(
                            identifier.getDatabaseName(), identifier.getObjectName()),
                    request,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_SNAPSHOT)) {
                throw new IllegalArgumentException(
                        String.format("Rollback snapshot '%s' doesn't exist.", e.resourceName()));
            } else if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_TAG)) {
                throw new IllegalArgumentException(
                        String.format("Rollback tag '%s' doesn't exist.", e.resourceName()));
            }
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    private TableMetadata loadTableMetadata(Identifier identifier) throws TableNotExistException {
        GetTableResponse response;
        try {
            // if the table is system table, we need to load table metadata from the system table's
            // data table
            Identifier loadTableIdentifier =
                    identifier.isSystemTable()
                            ? new Identifier(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    identifier.getBranchName())
                            : identifier;
            response =
                    client.get(
                            resourcePaths.table(
                                    loadTableIdentifier.getDatabaseName(),
                                    loadTableIdentifier.getObjectName()),
                            GetTableResponse.class,
                            restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }

        return toTableMetadata(identifier.getDatabaseName(), response);
    }

    private TableMetadata toTableMetadata(String db, GetTableResponse response) {
        TableSchema schema = TableSchema.create(response.getSchemaId(), response.getSchema());
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(PATH.key(), response.getPath());
        response.putAuditOptionsTo(options);
        Identifier identifier = Identifier.create(db, response.getName());
        if (identifier.getBranchName() != null) {
            options.put(BRANCH.key(), identifier.getBranchName());
        }
        return new TableMetadata(schema.copy(options), response.isExternal(), response.getId());
    }

    private Table toTable(String db, GetTableResponse response) {
        Identifier identifier = Identifier.create(db, response.getName());
        try {
            return CatalogUtils.loadTable(
                    this,
                    identifier,
                    path -> fileIOForData(path, identifier),
                    this::fileIOFromOptions,
                    i -> toTableMetadata(db, response),
                    null,
                    null);
        } catch (TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        try {
            checkNotBranch(identifier, "createTable");
            checkNotSystemTable(identifier, "createTable");
            validateAutoCreateClose(schema.options());
            CreateTableRequest request = new CreateTableRequest(identifier, schema);
            client.post(
                    resourcePaths.tables(identifier.getDatabaseName()), request, restAuthFunction);

        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(identifier);
            }
        } catch (NotImplementedException e) {
            throw new RuntimeException(new UnsupportedOperationException(e.getMessage()));
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        } catch (BadRequestException e) {
            throw new RuntimeException(new IllegalArgumentException(e.getMessage()));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        checkNotBranch(fromTable, "renameTable");
        checkNotBranch(toTable, "renameTable");
        checkNotSystemTable(fromTable, "renameTable");
        checkNotSystemTable(toTable, "renameTable");
        try {
            RenameTableRequest request = new RenameTableRequest(fromTable, toTable);
            client.post(resourcePaths.renameTable(), request, restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(fromTable);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(fromTable, e);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException(toTable);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        checkNotSystemTable(identifier, "alterTable");
        try {
            AlterTableRequest request = new AlterTableRequest(changes);
            client.post(
                    resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                    request,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_TABLE)) {
                    throw new TableNotExistException(identifier);
                } else if (StringUtils.equals(
                        e.resourceType(), ErrorResponse.RESOURCE_TYPE_COLUMN)) {
                    throw new ColumnNotExistException(identifier, e.resourceName());
                }
            }
        } catch (AlreadyExistsException e) {
            throw new ColumnAlreadyExistException(identifier, e.resourceName());
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (ServiceFailureException e) {
            throw new IllegalStateException(e.getMessage());
        } catch (NotImplementedException e) {
            throw new UnsupportedOperationException(e.getMessage());
        } catch (BadRequestException e) {
            throw new RuntimeException(new IllegalArgumentException(e.getMessage()));
        }
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        checkNotBranch(identifier, "dropTable");
        checkNotSystemTable(identifier, "dropTable");
        try {
            client.delete(
                    resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(identifier);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        try {
            MarkDonePartitionsRequest request = new MarkDonePartitionsRequest(partitions);
            client.post(
                    resourcePaths.markDonePartitions(
                            identifier.getDatabaseName(), identifier.getObjectName()),
                    request,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (NotImplementedException ignored) {
            // not a metastore partitioned table
        }
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        try {
            return listDataFromPageApi(
                    queryParams ->
                            client.get(
                                    resourcePaths.partitions(
                                            identifier.getDatabaseName(),
                                            identifier.getObjectName()),
                                    queryParams,
                                    ListPartitionsResponse.class,
                                    restAuthFunction));
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (NotImplementedException e) {
            // not a metastore partitioned table
            return listPartitionsFromFileSystem(getTable(identifier));
        }
    }

    @Override
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern)
            throws TableNotExistException {
        try {
            ListPartitionsResponse response =
                    client.get(
                            resourcePaths.partitions(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            buildPagedQueryParams(
                                    maxResults,
                                    pageToken,
                                    PARTITION_NAME_PATTERN,
                                    partitionNamePattern),
                            ListPartitionsResponse.class,
                            restAuthFunction);
            List<Partition> partitions = response.getPartitions();
            if (partitions == null) {
                return new PagedList<>(emptyList(), null);
            }
            return new PagedList<>(partitions, response.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (NotImplementedException e) {
            // not a metastore partitioned table
            return new PagedList<>(listPartitionsFromFileSystem(getTable(identifier)), null);
        }
    }

    @Override
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag)
            throws TableNotExistException, BranchAlreadyExistException, TagNotExistException {
        try {
            CreateBranchRequest request = new CreateBranchRequest(branch, fromTag);
            client.post(
                    resourcePaths.branches(
                            identifier.getDatabaseName(), identifier.getObjectName()),
                    request,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_TABLE)) {
                throw new TableNotExistException(identifier, e);
            } else if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_TAG)) {
                throw new TagNotExistException(identifier, fromTag, e);
            } else {
                throw e;
            }
        } catch (AlreadyExistsException e) {
            throw new BranchAlreadyExistException(identifier, branch, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public void dropBranch(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            client.delete(
                    resourcePaths.branch(
                            identifier.getDatabaseName(), identifier.getObjectName(), branch),
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new BranchNotExistException(identifier, branch, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            ForwardBranchRequest request = new ForwardBranchRequest();
            client.post(
                    resourcePaths.forwardBranch(
                            identifier.getDatabaseName(), identifier.getObjectName(), branch),
                    request,
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new BranchNotExistException(identifier, branch, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        try {
            ListBranchesResponse response =
                    client.get(
                            resourcePaths.branches(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            ListBranchesResponse.class,
                            restAuthFunction);
            if (response.branches() == null) {
                return emptyList();
            }
            return response.branches();
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void createPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        // partitions of the REST Catalog server are automatically calculated and do not require
        // special creating.
    }

    @Override
    public void dropPartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        Table table = getTable(identifier);
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.truncatePartitions(partitions);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void alterPartitions(Identifier identifier, List<PartitionStatistics> partitions)
            throws TableNotExistException {
        // The partition statistics of the REST Catalog server are automatically calculated and do
        // not require special reporting.
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        try {
            GetViewResponse response =
                    client.get(
                            resourcePaths.view(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            GetViewResponse.class,
                            restAuthFunction);
            return toView(identifier.getDatabaseName(), response);
        } catch (NoSuchResourceException e) {
            throw new ViewNotExistException(identifier);
        }
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        try {
            client.delete(
                    resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                    restAuthFunction);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new ViewNotExistException(identifier);
            }
        }
    }

    @Override
    public void createView(Identifier identifier, View view, boolean ignoreIfExists)
            throws ViewAlreadyExistException, DatabaseNotExistException {
        try {
            ViewSchema schema =
                    new ViewSchema(
                            view.rowType().getFields(),
                            view.query(),
                            view.dialects(),
                            view.comment().orElse(null),
                            view.options());
            CreateViewRequest request = new CreateViewRequest(identifier, schema);
            client.post(
                    resourcePaths.views(identifier.getDatabaseName()), request, restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(identifier.getDatabaseName());
        } catch (AlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new ViewAlreadyExistException(identifier);
            }
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        try {
            return listDataFromPageApi(
                    queryParams ->
                            client.get(
                                    resourcePaths.views(databaseName),
                                    queryParams,
                                    ListViewsResponse.class,
                                    restAuthFunction));
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern)
            throws DatabaseNotExistException {
        try {
            ListViewsResponse response =
                    client.get(
                            resourcePaths.views(databaseName),
                            buildPagedQueryParams(
                                    maxResults, pageToken, VIEW_NAME_PATTERN, viewNamePattern),
                            ListViewsResponse.class,
                            restAuthFunction);
            List<String> views = response.getViews();
            if (views == null) {
                return new PagedList<>(emptyList(), null);
            }
            return new PagedList<>(views, response.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<View> listViewDetailsPaged(
            String db,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern)
            throws DatabaseNotExistException {
        try {
            ListViewDetailsResponse response =
                    client.get(
                            resourcePaths.viewDetails(db),
                            buildPagedQueryParams(
                                    maxResults, pageToken, VIEW_NAME_PATTERN, viewNamePattern),
                            ListViewDetailsResponse.class,
                            restAuthFunction);
            List<GetViewResponse> views = response.getViewDetails();
            if (views == null) {
                return new PagedList<>(emptyList(), null);
            }
            return new PagedList<>(
                    views.stream().map(v -> toView(db, v)).collect(Collectors.toList()),
                    response.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(db);
        }
    }

    private ViewImpl toView(String db, GetViewResponse response) {
        ViewSchema schema = response.getSchema();
        Map<String, String> options = new HashMap<>(schema.options());
        response.putAuditOptionsTo(options);
        return new ViewImpl(
                Identifier.create(db, response.getName()),
                schema.fields(),
                schema.query(),
                schema.dialects(),
                schema.comment(),
                options);
    }

    @Override
    public void renameView(Identifier fromView, Identifier toView, boolean ignoreIfNotExists)
            throws ViewNotExistException, ViewAlreadyExistException {
        try {
            RenameTableRequest request = new RenameTableRequest(fromView, toView);
            client.post(resourcePaths.renameView(), request, restAuthFunction);

        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new ViewNotExistException(fromView);
            }
        } catch (AlreadyExistsException e) {
            throw new ViewAlreadyExistException(toView);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public void alterView(
            Identifier identifier, List<ViewChange> viewChanges, boolean ignoreIfNotExists)
            throws ViewNotExistException, DialectAlreadyExistException, DialectNotExistException {
        try {
            AlterViewRequest request = new AlterViewRequest(viewChanges);
            client.post(
                    resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                    request,
                    restAuthFunction);
        } catch (AlreadyExistsException e) {
            throw new DialectAlreadyExistException(identifier, e.resourceName());
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_DIALECT)) {
                throw new DialectNotExistException(identifier, e.resourceName());
            }
            if (!ignoreIfNotExists) {
                throw new ViewNotExistException(identifier);
            }
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
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
    Map<String, String> headers(RESTAuthParameter restAuthParameter) {
        return restAuthFunction.apply(restAuthParameter);
    }

    protected GetTableTokenResponse loadTableToken(Identifier identifier)
            throws TableNotExistException {
        GetTableTokenResponse response;
        try {
            response =
                    client.get(
                            resourcePaths.tableToken(
                                    identifier.getDatabaseName(), identifier.getObjectName()),
                            GetTableTokenResponse.class,
                            restAuthFunction);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
        return response;
    }

    protected <T> List<T> listDataFromPageApi(
            Function<Map<String, String>, PagedResponse<T>> pageApi) {
        List<T> results = new ArrayList<>();
        Map<String, String> queryParams = Maps.newHashMap();
        String pageToken = null;
        do {
            if (pageToken != null) {
                queryParams.put(PAGE_TOKEN, pageToken);
            }
            PagedResponse<T> response = pageApi.apply(queryParams);
            pageToken = response.getNextPageToken();
            if (response.data() != null) {
                results.addAll(response.data());
            }
            if (pageToken == null || response.data() == null || response.data().isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return results;
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

    private FileIO fileIOForData(Path path, Identifier identifier) {
        return dataTokenEnabled
                ? new RESTTokenFileIO(catalogLoader(), this, identifier, path)
                : fileIOFromOptions(path);
    }

    private FileIO fileIOFromOptions(Path path) {
        try {
            return FileIO.get(path, context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, String> buildPagedQueryParams(
            @Nullable Integer maxResults, @Nullable String pageToken) {
        return buildPagedQueryParams(maxResults, pageToken, null, null);
    }

    private Map<String, String> buildPagedQueryParams(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String namePatternKey,
            @Nullable String namePatternValue) {
        Map<String, String> queryParams = Maps.newHashMap();
        if (Objects.nonNull(maxResults) && maxResults > 0) {
            queryParams.put(MAX_RESULTS, maxResults.toString());
        }
        if (Objects.nonNull(pageToken)) {
            queryParams.put(PAGE_TOKEN, pageToken);
        }
        if (Objects.nonNull(namePatternValue)) {
            queryParams.put(namePatternKey, namePatternValue);
        }
        return queryParams;
    }
}
