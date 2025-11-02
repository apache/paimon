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
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TableType;
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
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
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

import org.apache.paimon.shade.org.apache.commons.lang3.StringUtils;

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
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.catalog.CatalogUtils.checkNotBranch;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.checkNotSystemTable;
import static org.apache.paimon.catalog.CatalogUtils.isSystemDatabase;
import static org.apache.paimon.catalog.CatalogUtils.listPartitionsFromFileSystem;
import static org.apache.paimon.catalog.CatalogUtils.validateCreateTable;
import static org.apache.paimon.options.CatalogOptions.CASE_SENSITIVE;

/** A catalog implementation for REST. */
public class RESTCatalog implements Catalog {

    private final RESTApi api;
    private final CatalogContext context;
    private final boolean dataTokenEnabled;

    public RESTCatalog(CatalogContext context) {
        this(context, true);
    }

    public RESTCatalog(CatalogContext context, boolean configRequired) {
        this.api = new RESTApi(context.options(), configRequired);
        this.context =
                CatalogContext.create(
                        api.options(),
                        context.hadoopConf(),
                        context.preferIO(),
                        context.fallbackIO());
        this.dataTokenEnabled = api.options().get(RESTTokenFileIO.DATA_TOKEN_ENABLED);
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
        return api.listDatabases();
    }

    @Override
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern) {
        return api.listDatabasesPaged(maxResults, pageToken, databaseNamePattern);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        checkNotSystemDatabase(name);
        try {
            api.createDatabase(name, properties);
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
            GetDatabaseResponse response = api.getDatabase(name);
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
            api.dropDatabase(name);
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
            api.alterDatabase(name, new ArrayList<>(removeKeys), updateProperties);
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
            return api.listTables(databaseName);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        } catch (ForbiddenException e) {
            throw new DatabaseNoPermissionException(databaseName, e);
        }
    }

    @Override
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        try {
            return api.listTablesPaged(
                    databaseName, maxResults, pageToken, tableNamePattern, tableType);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<Table> listTableDetailsPaged(
            String db,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType)
            throws DatabaseNotExistException {
        try {
            PagedList<GetTableResponse> tables =
                    api.listTableDetailsPaged(
                            db, maxResults, pageToken, tableNamePattern, tableType);
            return new PagedList<>(
                    tables.getElements().stream()
                            .map(t -> toTable(db, t))
                            .collect(Collectors.toList()),
                    tables.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(db);
        }
    }

    @Override
    public PagedList<Identifier> listTablesPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String tableNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        PagedList<Identifier> tables =
                api.listTablesPagedGlobally(
                        databaseNamePattern, tableNamePattern, maxResults, pageToken);
        return new PagedList<>(tables.getElements(), tables.getNextPageToken());
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
                null,
                context,
                true);
    }

    @Override
    public Optional<TableSnapshot> loadSnapshot(Identifier identifier)
            throws TableNotExistException {
        try {
            return Optional.ofNullable(api.loadSnapshot(identifier));
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_SNAPSHOT)) {
                return Optional.empty();
            }
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public Optional<Snapshot> loadSnapshot(Identifier identifier, String version)
            throws TableNotExistException {
        try {
            return Optional.ofNullable(api.loadSnapshot(identifier, version));
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_SNAPSHOT)) {
                return Optional.empty();
            }
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken)
            throws TableNotExistException {
        try {
            return api.listSnapshotsPaged(identifier, maxResults, pageToken);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
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
    public boolean supportsListTableByType() {
        return true;
    }

    @Override
    public boolean supportsVersionManagement() {
        return true;
    }

    @Override
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws TableNotExistException {
        try {
            return api.commitSnapshot(identifier, tableUuid, snapshot, statistics);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public void rollbackTo(Identifier identifier, Instant instant)
            throws Catalog.TableNotExistException {
        try {
            api.rollbackTo(identifier, instant);
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
        // if the table is system table, we need to load table metadata from the system table's data
        // table
        Identifier loadTableIdentifier =
                identifier.isSystemTable()
                        ? new Identifier(
                                identifier.getDatabaseName(),
                                identifier.getTableName(),
                                identifier.getBranchName())
                        : identifier;

        GetTableResponse response;
        try {
            response = api.getTable(loadTableIdentifier);
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
                    null,
                    context,
                    true);
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
            validateCreateTable(schema);
            createExternalTablePathIfNotExist(schema);
            Schema newSchema = inferSchemaIfExternalPaimonTable(schema);
            api.createTable(identifier, newSchema);
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
            api.renameTable(fromTable, toTable);
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
            api.alterTable(identifier, changes);
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
    public List<String> authTableQuery(Identifier identifier, @Nullable List<String> select)
            throws TableNotExistException {
        checkNotSystemTable(identifier, "authTable");
        try {
            return api.authTableQuery(identifier, select);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
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
            api.dropTable(identifier);
        } catch (NoSuchResourceException e) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(identifier);
            }
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void registerTable(Identifier identifier, String path)
            throws TableAlreadyExistException {
        try {
            api.registerTable(identifier, path);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (AlreadyExistsException e) {
            throw new TableAlreadyExistException(identifier);
        } catch (ServiceFailureException e) {
            throw new IllegalStateException(e.getMessage());
        } catch (BadRequestException e) {
            throw new RuntimeException(new IllegalArgumentException(e.getMessage()));
        }
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        try {
            api.markDonePartitions(identifier, partitions);
        } catch (NoSuchResourceException e) {
            throw new TableNotExistException(identifier);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        } catch (NotImplementedException ignored) {
            // not a metastore partitioned table
        }
    }

    @Override
    public List<Partition> listPartitions(Identifier identifier) throws TableNotExistException {
        try {
            return api.listPartitions(identifier);
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
            return api.listPartitionsPaged(identifier, maxResults, pageToken, partitionNamePattern);
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
            api.createBranch(identifier, branch, fromTag);
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
            api.dropBranch(identifier, branch);
        } catch (NoSuchResourceException e) {
            throw new BranchNotExistException(identifier, branch, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public void fastForward(Identifier identifier, String branch) throws BranchNotExistException {
        try {
            api.fastForward(identifier, branch);
        } catch (NoSuchResourceException e) {
            throw new BranchNotExistException(identifier, branch, e);
        } catch (ForbiddenException e) {
            throw new TableNoPermissionException(identifier, e);
        }
    }

    @Override
    public List<String> listBranches(Identifier identifier) throws TableNotExistException {
        try {
            return api.listBranches(identifier);
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
    public List<String> listFunctions(String databaseName) throws DatabaseNotExistException {
        try {
            return api.listFunctions(databaseName);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName, e);
        }
    }

    @Override
    public org.apache.paimon.function.Function getFunction(Identifier identifier)
            throws FunctionNotExistException {
        try {
            GetFunctionResponse response = api.getFunction(identifier);
            return response.toFunction(identifier);
        } catch (NoSuchResourceException e) {
            throw new FunctionNotExistException(identifier, e);
        }
    }

    @Override
    public void createFunction(
            Identifier identifier,
            org.apache.paimon.function.Function function,
            boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException {
        try {
            api.createFunction(identifier, function);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(identifier.getDatabaseName(), e);
        } catch (AlreadyExistsException e) {
            if (ignoreIfExists) {
                return;
            }
            throw new FunctionAlreadyExistException(identifier, e);
        }
    }

    @Override
    public void dropFunction(Identifier identifier, boolean ignoreIfNotExists)
            throws FunctionNotExistException {
        try {
            api.dropFunction(identifier);
        } catch (NoSuchResourceException e) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new FunctionNotExistException(identifier, e);
        }
    }

    @Override
    public void alterFunction(
            Identifier identifier, List<FunctionChange> changes, boolean ignoreIfNotExists)
            throws FunctionNotExistException, DefinitionAlreadyExistException,
                    DefinitionNotExistException {
        try {
            api.alterFunction(identifier, changes);
        } catch (AlreadyExistsException e) {
            throw new DefinitionAlreadyExistException(identifier, e.resourceName());
        } catch (NoSuchResourceException e) {
            if (StringUtils.equals(e.resourceType(), ErrorResponse.RESOURCE_TYPE_DEFINITION)) {
                throw new DefinitionNotExistException(identifier, e.resourceName());
            }
            if (!ignoreIfNotExists) {
                throw new FunctionNotExistException(identifier, e);
            }
        } catch (BadRequestException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    @Override
    public PagedList<String> listFunctionsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern)
            throws DatabaseNotExistException {
        try {
            return api.listFunctionsPaged(databaseName, maxResults, pageToken, functionNamePattern);
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public PagedList<Identifier> listFunctionsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String functionNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        PagedList<Identifier> functions =
                api.listFunctionsPagedGlobally(
                        databaseNamePattern, functionNamePattern, maxResults, pageToken);
        return new PagedList<>(functions.getElements(), functions.getNextPageToken());
    }

    @Override
    public PagedList<Function> listFunctionDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern)
            throws DatabaseNotExistException {
        try {
            PagedList<GetFunctionResponse> functions =
                    api.listFunctionDetailsPaged(
                            databaseName, maxResults, pageToken, functionNamePattern);
            return new PagedList<>(
                    functions.getElements().stream()
                            .map(v -> v.toFunction(Identifier.create(databaseName, v.name())))
                            .collect(Collectors.toList()),
                    functions.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(databaseName);
        }
    }

    @Override
    public View getView(Identifier identifier) throws ViewNotExistException {
        try {
            GetViewResponse response = api.getView(identifier);
            return toView(identifier.getDatabaseName(), response);
        } catch (NoSuchResourceException e) {
            throw new ViewNotExistException(identifier);
        }
    }

    @Override
    public void dropView(Identifier identifier, boolean ignoreIfNotExists)
            throws ViewNotExistException {
        try {
            api.dropView(identifier);
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
            api.createView(identifier, schema);
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
            return CatalogUtils.isSystemDatabase(databaseName)
                    ? Collections.emptyList()
                    : api.listViews(databaseName);
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
            return api.listViewsPaged(databaseName, maxResults, pageToken, viewNamePattern);
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
            PagedList<GetViewResponse> views =
                    api.listViewDetailsPaged(db, maxResults, pageToken, viewNamePattern);
            return new PagedList<>(
                    views.getElements().stream()
                            .map(v -> toView(db, v))
                            .collect(Collectors.toList()),
                    views.getNextPageToken());
        } catch (NoSuchResourceException e) {
            throw new DatabaseNotExistException(db);
        }
    }

    @Override
    public PagedList<Identifier> listViewsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String viewNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        PagedList<Identifier> views =
                api.listViewsPagedGlobally(
                        databaseNamePattern, viewNamePattern, maxResults, pageToken);
        return new PagedList<>(views.getElements(), views.getNextPageToken());
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
            api.renameView(fromView, toView);
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
            api.alterView(identifier, viewChanges);
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
    public void close() throws Exception {}

    @VisibleForTesting
    RESTApi api() {
        return api;
    }

    private FileIO fileIOForData(Path path, Identifier identifier) {
        return dataTokenEnabled
                ? new RESTTokenFileIO(context, api, identifier, path)
                : fileIOFromOptions(path);
    }

    private FileIO fileIOFromOptions(Path path) {
        try {
            return FileIO.get(path, context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createExternalTablePathIfNotExist(Schema schema) throws IOException {
        Map<String, String> options = schema.options();
        if (options.containsKey(CoreOptions.PATH.key())) {
            Path path = new Path(options.get(PATH.key()));
            try (FileIO fileIO = fileIOFromOptions(path)) {
                if (!fileIO.exists(path)) {
                    fileIO.mkdirs(path);
                }
            }
        }
    }

    private Schema inferSchemaIfExternalPaimonTable(Schema schema) throws Exception {
        TableType tableType = Options.fromMap(schema.options()).get(TYPE);
        String externalLocation = schema.options().get(PATH.key());

        if (TableType.TABLE.equals(tableType) && Objects.nonNull(externalLocation)) {
            Path externalPath = new Path(externalLocation);
            SchemaManager schemaManager =
                    new SchemaManager(fileIOFromOptions(externalPath), externalPath);
            Optional<TableSchema> latest = schemaManager.latest();
            if (latest.isPresent()) {
                // Note we just validate schema here, will not create a new table
                schemaManager.createTable(schema, true);
                Schema existsSchema = latest.get().toSchema();
                // use `owner` and `path` from the user provide schema
                if (Objects.nonNull(schema.options().get(Catalog.OWNER_PROP))) {
                    existsSchema
                            .options()
                            .put(Catalog.OWNER_PROP, schema.options().get(Catalog.OWNER_PROP));
                }
                existsSchema.options().put(PATH.key(), schema.options().get(PATH.key()));
                return existsSchema;
            }
        }
        return schema;
    }
}
