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
import org.apache.paimon.annotation.Public;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterFunctionRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.requests.AuthTableQueryRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateFunctionRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateTagRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.ForwardBranchRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RegisterTableRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.AuthTableQueryResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListFunctionDetailsResponse;
import org.apache.paimon.rest.responses.ListFunctionsGloballyResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesGloballyResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListTagsResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsGloballyResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.rest.responses.PagedResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTFunctionValidator.checkFunctionName;
import static org.apache.paimon.rest.RESTFunctionValidator.isValidFunctionName;
import static org.apache.paimon.rest.RESTUtil.extractPrefixMap;
import static org.apache.paimon.rest.auth.AuthProviderFactory.createAuthProvider;

/**
 * REST API for REST Catalog.
 *
 * <p>This API class only includes interaction with REST Server and does not have file read and
 * write operations, which makes this API lightweight enough to avoid introducing dependencies such
 * as Hadoop and file systems.
 *
 * <p>The following example show how to use the RESTApi:
 *
 * <pre>{@code
 * Options options = new Options();
 * options.set(URI, "<rest server url>");
 * options.set(WAREHOUSE, "my_instance_name");
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_ACCESS_KEY_ID, "<access-key-id>");
 * options.set(DLF_ACCESS_KEY_SECRET, "<access-key-secret>");
 *
 * RESTApi api = new RESTApi(options);
 * List<String> tables = api.listTables("my_database");
 * System.out.println(tables);
 * }</pre>
 *
 * <p>This class also provide util methods for serializing json {@link #toJson} and deserializing
 * json {@link #fromJson}.
 *
 * @since 1.2.0
 */
@Public
public class RESTApi {

    public static final String HEADER_PREFIX = "header.";
    public static final String MAX_RESULTS = "maxResults";
    public static final String PAGE_TOKEN = "pageToken";

    public static final String DATABASE_NAME_PATTERN = "databaseNamePattern";
    public static final String TABLE_NAME_PATTERN = "tableNamePattern";
    public static final String TABLE_TYPE = "tableType";
    public static final String VIEW_NAME_PATTERN = "viewNamePattern";
    public static final String FUNCTION_NAME_PATTERN = "functionNamePattern";
    public static final String PARTITION_NAME_PATTERN = "partitionNamePattern";

    public static final long TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000L;

    public static final ObjectMapper OBJECT_MAPPER = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE;

    private final HttpClient client;
    private final RESTAuthFunction restAuthFunction;
    private final Options options;
    private final ResourcePaths resourcePaths;

    /**
     * Initializes a newly created {@code RESTApi} object.
     *
     * <p>By default, {@code configRequired} is true, this means that there will be one REST request
     * to merge configurations during initialization.
     *
     * @param options contains authentication and catalog information for REST Server
     */
    public RESTApi(Options options) {
        this(options, true);
    }

    /**
     * Initializes a newly created {@code RESTApi} object.
     *
     * <p>If the {@code options} are already obtained through {@link #options()}, you can configure
     * configRequired to be false.
     *
     * @param options contains authentication and catalog information for REST Server
     * @param configRequired is there one REST request to merge configurations during initialization
     */
    public RESTApi(Options options, boolean configRequired) {
        this.client = new HttpClient(options.get(RESTCatalogOptions.URI));
        AuthProvider authProvider = createAuthProvider(options);
        Map<String, String> baseHeaders = extractPrefixMap(options, HEADER_PREFIX);
        if (configRequired) {
            String warehouse = options.get(WAREHOUSE);
            Map<String, String> queryParams =
                    StringUtils.isNotEmpty(warehouse)
                            ? ImmutableMap.of(WAREHOUSE.key(), RESTUtil.encodeString(warehouse))
                            : ImmutableMap.of();
            options =
                    new Options(
                            client.get(
                                            ResourcePaths.config(),
                                            queryParams,
                                            ConfigResponse.class,
                                            new RESTAuthFunction(baseHeaders, authProvider))
                                    .merge(options.toMap()));
            baseHeaders.putAll(extractPrefixMap(options, HEADER_PREFIX));
        }
        this.restAuthFunction = new RESTAuthFunction(baseHeaders, authProvider);
        this.options = options;
        this.resourcePaths = ResourcePaths.forCatalogProperties(options);
    }

    /** Get the configured options which has been merged from REST Server. */
    public Options options() {
        return options;
    }

    /**
     * List databases.
     *
     * <p>Gets an array of databases for a catalog. There is no guarantee of a specific ordering of
     * the elements in the array.
     */
    public List<String> listDatabases() {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.databases(),
                                queryParams,
                                ListDatabasesResponse.class,
                                restAuthFunction));
    }

    /**
     * List databases.
     *
     * <p>Gets an array of databases for a catalog. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     */
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern) {
        ListDatabasesResponse response =
                client.get(
                        resourcePaths.databases(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern)),
                        ListDatabasesResponse.class,
                        restAuthFunction);
        List<String> databases = response.getDatabases();
        if (databases == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(databases, response.getNextPageToken());
    }

    /**
     * Create a database.
     *
     * @param name name of this database
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a database already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void createDatabase(String name, Map<String, String> properties) {
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        client.post(resourcePaths.databases(), request, restAuthFunction);
    }

    /**
     * Get a database.
     *
     * @param name name of this database
     * @return {@link GetDatabaseResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public GetDatabaseResponse getDatabase(String name) {
        return client.get(
                resourcePaths.database(name), GetDatabaseResponse.class, restAuthFunction);
    }

    /**
     * Drop a database.
     *
     * @param name name of this database
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void dropDatabase(String name) {
        client.delete(resourcePaths.database(name), restAuthFunction);
    }

    /**
     * Alter a database.
     *
     * @param name name of this database
     * @param removals options to be removed
     * @param updates options to be updated or added
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void alterDatabase(String name, List<String> removals, Map<String, String> updates) {
        client.post(
                resourcePaths.database(name),
                new AlterDatabaseRequest(removals, updates),
                AlterDatabaseResponse.class,
                restAuthFunction);
    }

    /**
     * List tables for a database.
     *
     * @param databaseName name of this database
     * @return a list of table names
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public List<String> listTables(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.tables(databaseName),
                                queryParams,
                                ListTablesResponse.class,
                                restAuthFunction));
    }

    /**
     * List tables for a database.
     *
     * <p>Gets an array of tables for a database. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseName name of database.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) {
        ListTablesResponse response =
                client.get(
                        resourcePaths.tables(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern),
                                Pair.of(TABLE_TYPE, tableType)),
                        ListTablesResponse.class,
                        restAuthFunction);
        List<String> tables = response.getTables();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * List table details for a database.
     *
     * <p>Gets an array of table details for a database. There is no guarantee of a specific
     * ordering of the elements in the array.
     *
     * @param databaseName name of database.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @param tableType Optional parameter to filter tables by table type. All table types will be
     *     returned if not set or empty.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetTableResponse> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) {
        ListTableDetailsResponse response =
                client.get(
                        resourcePaths.tableDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern),
                                Pair.of(TABLE_TYPE, tableType)),
                        ListTableDetailsResponse.class,
                        restAuthFunction);
        List<GetTableResponse> tables = response.getTableDetails();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * List table for a catalog.
     *
     * <p>Gets an array of table for a catalog. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listTablesPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String tableNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListTablesGloballyResponse response =
                client.get(
                        resourcePaths.tables(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern)),
                        ListTablesGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> tables = response.getTables();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * Get table.
     *
     * @param identifier database name and table name.
     * @return {@link GetTableResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTableResponse getTable(Identifier identifier) {
        return client.get(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableResponse.class,
                restAuthFunction);
    }

    /**
     * Load latest snapshot for table.
     *
     * @param identifier database name and table name.
     * @return {@link TableSnapshot} snapshot with statistics.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the latest
     *     snapshot not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public TableSnapshot loadSnapshot(Identifier identifier) {
        GetTableSnapshotResponse response =
                client.get(
                        resourcePaths.tableSnapshot(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        GetTableSnapshotResponse.class,
                        restAuthFunction);
        return response.getSnapshot();
    }

    /**
     * Return the snapshot of table for given version. Version parsing order is:
     *
     * <ul>
     *   <li>1. If it is 'EARLIEST', get the earliest snapshot.
     *   <li>2. If it is 'LATEST', get the latest snapshot.
     *   <li>3. If it is a number, get snapshot by snapshot id.
     *   <li>4. Else try to get snapshot from Tag name.
     * </ul>
     *
     * @param identifier database name and table name.
     * @param version version to snapshot
     * @return Optional snapshot.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public Snapshot loadSnapshot(Identifier identifier, String version) {
        GetVersionSnapshotResponse response =
                client.get(
                        resourcePaths.tableSnapshot(
                                identifier.getDatabaseName(), identifier.getObjectName(), version),
                        GetVersionSnapshotResponse.class,
                        restAuthFunction);
        return response.getSnapshot();
    }

    /**
     * Get paged snapshot list of the table, the snapshot list will be returned in descending order.
     *
     * @param identifier path of the table to list partitions
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the snapshots with provided page size(@param maxResults) in this table and
     *     next page token.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the latest
     *     snapshot not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        ListSnapshotsResponse response =
                client.get(
                        resourcePaths.snapshots(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(maxResults, pageToken),
                        ListSnapshotsResponse.class,
                        restAuthFunction);
        List<Snapshot> snapshots = response.getSnapshots();
        if (snapshots == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(snapshots, response.getNextPageToken());
    }

    /**
     * Commit snapshot for table.
     *
     * @param identifier database name and table name.
     * @param tableUuid Uuid of the table to avoid wrong commit
     * @param snapshot snapshot for committing
     * @param statistics statistics for this snapshot incremental
     * @return true if commit success
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) {
        CommitTableRequest request = new CommitTableRequest(tableUuid, snapshot, statistics);
        CommitTableResponse response =
                client.post(
                        resourcePaths.commitTable(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        request,
                        CommitTableResponse.class,
                        restAuthFunction);
        return response.isSuccess();
    }

    /**
     * Rollback instant for table.
     *
     * @param identifier database name and table name.
     * @param instant instant to rollback
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     or the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void rollbackTo(Identifier identifier, Instant instant) {
        rollbackTo(identifier, instant, null);
    }

    /**
     * Rollback instant for table.
     *
     * @param identifier database name and table name.
     * @param instant instant to rollback
     * @param fromSnapshot snapshot from, success only occurs when the latest snapshot is this
     *     snapshot.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     or the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot) {
        RollbackTableRequest request = new RollbackTableRequest(instant, fromSnapshot);
        client.post(
                resourcePaths.rollbackTable(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Create table.
     *
     * @param identifier database name and table name.
     * @param schema schema to create table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a table already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     creating table
     */
    public void createTable(Identifier identifier, Schema schema) {
        CreateTableRequest request = new CreateTableRequest(identifier, schema);
        client.post(resourcePaths.tables(identifier.getDatabaseName()), request, restAuthFunction);
    }

    /**
     * Rename table.
     *
     * @param fromTable from table
     * @param toTable to table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the fromTable not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the toTable already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     renaming table
     */
    public void renameTable(Identifier fromTable, Identifier toTable) {
        RenameTableRequest request = new RenameTableRequest(fromTable, toTable);
        client.post(resourcePaths.renameTable(), request, restAuthFunction);
    }

    /**
     * Alter table.
     *
     * @param identifier database name and table name.
     * @param changes changes to alter table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void alterTable(Identifier identifier, List<SchemaChange> changes) {
        AlterTableRequest request = new AlterTableRequest(changes);
        client.post(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Auth table query.
     *
     * @param identifier database name and table name.
     * @param select select columns, null if select all
     * @return additional filter for row level access control
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public List<String> authTableQuery(Identifier identifier, @Nullable List<String> select) {
        AuthTableQueryRequest request = new AuthTableQueryRequest(select);
        AuthTableQueryResponse response =
                client.post(
                        resourcePaths.authTable(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        request,
                        AuthTableQueryResponse.class,
                        restAuthFunction);
        return response.filter();
    }

    /**
     * Drop table.
     *
     * @param identifier database name and table name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void dropTable(Identifier identifier) {
        client.delete(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    public void registerTable(Identifier identifier, String path) {
        client.post(
                resourcePaths.registerTable(identifier.getDatabaseName()),
                new RegisterTableRequest(identifier, path),
                restAuthFunction);
    }

    /**
     * Mark done partitions for table.
     *
     * @param identifier database name and table name.
     * @param partitions partitions to be marked done
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions) {
        MarkDonePartitionsRequest request = new MarkDonePartitionsRequest(partitions);
        client.post(
                resourcePaths.markDonePartitions(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * List partitions for table.
     *
     * @param identifier database name and table name.
     * @return a list for partitions
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public List<Partition> listPartitions(Identifier identifier) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.partitions(
                                        identifier.getDatabaseName(), identifier.getObjectName()),
                                queryParams,
                                ListPartitionsResponse.class,
                                restAuthFunction));
    }

    /**
     * List partitions for a table.
     *
     * <p>Gets an array of partitions for a table. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param identifier database name and table name.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param partitionNamePattern A sql LIKE pattern (%) for partition names. All partitions will
     *     be returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern) {
        ListPartitionsResponse response =
                client.get(
                        resourcePaths.partitions(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(PARTITION_NAME_PATTERN, partitionNamePattern)),
                        ListPartitionsResponse.class,
                        restAuthFunction);
        List<Partition> partitions = response.getPartitions();
        if (partitions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(partitions, response.getNextPageToken());
    }

    /**
     * Create branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @param fromTag optional from tag
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or fromTag not
     *     exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the branch already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag) {
        CreateBranchRequest request = new CreateBranchRequest(branch, fromTag);
        client.post(
                resourcePaths.branches(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Drop branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the branch not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void dropBranch(Identifier identifier, String branch) {
        client.delete(
                resourcePaths.branch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                restAuthFunction);
    }

    /**
     * Forward branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the branch or table not
     *     exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void fastForward(Identifier identifier, String branch) {
        ForwardBranchRequest request = new ForwardBranchRequest();
        client.post(
                resourcePaths.forwardBranch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                request,
                restAuthFunction);
    }

    /**
     * List branches for table.
     *
     * @param identifier database name and table name.
     * @return a list of branches
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public List<String> listBranches(Identifier identifier) {
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
    }

    /**
     * Get tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @return {@link GetTagResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTagResponse getTag(Identifier identifier, String tagName) {
        return client.get(
                resourcePaths.tag(
                        identifier.getDatabaseName(), identifier.getObjectName(), tagName),
                GetTagResponse.class,
                restAuthFunction);
    }

    /**
     * Create tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @param snapshotId optional snapshot id, if not provided uses latest snapshot
     * @param timeRetained optional time retained as string (e.g., "1d", "12h", "30m")
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or snapshot not
     *     exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the tag already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained) {
        CreateTagRequest request = new CreateTagRequest(tagName, snapshotId, timeRetained);
        client.post(
                resourcePaths.tags(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Get paged list names of tags under this table. An empty list is returned if none tag exists.
     *
     * @param identifier database name and table name.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<String> listTagsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        ListTagsResponse response =
                client.get(
                        resourcePaths.tags(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(maxResults, pageToken),
                        ListTagsResponse.class,
                        restAuthFunction);
        List<String> tags = response.tags();
        if (tags == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tags, response.getNextPageToken());
    }

    /**
     * Delete tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void deleteTag(Identifier identifier, String tagName) {
        client.delete(
                resourcePaths.tag(
                        identifier.getDatabaseName(), identifier.getObjectName(), tagName),
                restAuthFunction);
    }

    /**
     * List functions for database.
     *
     * @param databaseName database name
     * @return a list of function name
     */
    public List<String> listFunctions(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.functions(databaseName),
                                queryParams,
                                ListFunctionsResponse.class,
                                restAuthFunction));
    }

    /**
     * List functions by page.
     *
     * <p>Gets an array of functions for a database. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listFunctionsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) {
        ListFunctionsResponse response =
                client.get(
                        resourcePaths.functions(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionsResponse.class,
                        restAuthFunction);
        List<String> functions = response.functions();
        if (functions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functions, response.getNextPageToken());
    }

    /**
     * List function details.
     *
     * <p>Gets an array of function details for a database. There is no guarantee of a specific
     * ordering of the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetFunctionResponse> listFunctionDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) {
        ListFunctionDetailsResponse response =
                client.get(
                        resourcePaths.functionDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionDetailsResponse.class,
                        restAuthFunction);
        List<GetFunctionResponse> functionDetails = response.data();
        if (functionDetails == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functionDetails, response.getNextPageToken());
    }

    /**
     * List functions for a catalog.
     *
     * <p>Gets an array of functions for a catalog. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listFunctionsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String functionNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListFunctionsGloballyResponse response =
                client.get(
                        resourcePaths.functions(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionsGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> functions = response.data();
        if (functions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functions, response.getNextPageToken());
    }

    /**
     * Get a function by identifier.
     *
     * @param identifier the identifier of the function to retrieve
     * @return the function response object
     * @throws NoSuchResourceException if the function does not exist
     * @throws ForbiddenException if the user lacks permission to access the function
     */
    public GetFunctionResponse getFunction(Identifier identifier) {
        if (!isValidFunctionName(identifier.getObjectName())) {
            throw new NoSuchResourceException(
                    ErrorResponse.RESOURCE_TYPE_FUNCTION,
                    identifier.getObjectName(),
                    "Invalid function name: " + identifier.getObjectName());
        }
        return client.get(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                GetFunctionResponse.class,
                restAuthFunction);
    }

    /**
     * Create a function.
     *
     * @param identifier database name and function name.
     * @param function the function to be created
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a function already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     creating function
     */
    public void createFunction(
            Identifier identifier, org.apache.paimon.function.Function function) {
        checkFunctionName(identifier.getObjectName());
        client.post(
                resourcePaths.functions(identifier.getDatabaseName()),
                new CreateFunctionRequest(function),
                restAuthFunction);
    }

    /**
     * Drop a function.
     *
     * @param identifier database name and function name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the function not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this function
     */
    public void dropFunction(Identifier identifier) {
        checkFunctionName(identifier.getObjectName());
        client.delete(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    /**
     * Alter a function.
     *
     * @param identifier database name and function name.
     * @param changes list of function changes to apply
     * @throws NoSuchResourceException if the function does not exist
     * @throws ForbiddenException if the user lacks permission to modify the function
     */
    public void alterFunction(Identifier identifier, List<FunctionChange> changes) {
        checkFunctionName(identifier.getObjectName());
        client.post(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                new AlterFunctionRequest(changes),
                restAuthFunction);
    }

    /**
     * Get view.
     *
     * @param identifier database name and view name.
     * @return {@link GetViewResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public GetViewResponse getView(Identifier identifier) {
        return client.get(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                GetViewResponse.class,
                restAuthFunction);
    }

    /**
     * Drop view.
     *
     * @param identifier database name and view name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void dropView(Identifier identifier) {
        client.delete(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    /**
     * Create view.
     *
     * @param identifier database name and view name.
     * @param schema schema of the view
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the view already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void createView(Identifier identifier, ViewSchema schema) {
        CreateViewRequest request = new CreateViewRequest(identifier, schema);
        client.post(resourcePaths.views(identifier.getDatabaseName()), request, restAuthFunction);
    }

    /**
     * List views for a database.
     *
     * @param databaseName name of this database
     * @return a list of view names
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public List<String> listViews(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.views(databaseName),
                                queryParams,
                                ListViewsResponse.class,
                                restAuthFunction));
    }

    /**
     * List views.
     *
     * <p>Gets an array of views for a database. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
        ListViewsResponse response =
                client.get(
                        resourcePaths.views(databaseName),
                        buildPagedQueryParams(
                                maxResults, pageToken, Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewsResponse.class,
                        restAuthFunction);
        List<String> views = response.getViews();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * List view details.
     *
     * <p>Gets an array of view details for a database. There is no guarantee of a specific ordering
     * of the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetViewResponse> listViewDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
        ListViewDetailsResponse response =
                client.get(
                        resourcePaths.viewDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults, pageToken, Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewDetailsResponse.class,
                        restAuthFunction);
        List<GetViewResponse> views = response.getViewDetails();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * List views for a catalog.
     *
     * <p>Gets an array of views for a catalog. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listViewsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String viewNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListViewsGloballyResponse response =
                client.get(
                        resourcePaths.views(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewsGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> views = response.getViews();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * Rename view.
     *
     * @param fromView from view
     * @param toView to view
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means fromView not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means toView already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     views
     */
    public void renameView(Identifier fromView, Identifier toView) {
        RenameTableRequest request = new RenameTableRequest(fromView, toView);
        client.post(resourcePaths.renameView(), request, restAuthFunction);
    }

    /**
     * Alter view.
     *
     * @param identifier database name and view name.
     * @param viewChanges view changes
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void alterView(Identifier identifier, List<ViewChange> viewChanges) {
        AlterViewRequest request = new AlterViewRequest(viewChanges);
        client.post(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Load token for File System of this table.
     *
     * @param identifier database name and view name.
     * @return {@link GetTableTokenResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTableTokenResponse loadTableToken(Identifier identifier) {
        return client.get(
                resourcePaths.tableToken(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableTokenResponse.class,
                restAuthFunction);
    }

    /** Util method to deserialize object from json. */
    public static <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, clazz);
    }

    /** Util method to serialize object to json. */
    public static <T> String toJson(T t) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(t);
    }

    // ============================== Inner methods ================================

    @VisibleForTesting
    <T> List<T> listDataFromPageApi(Function<Map<String, String>, PagedResponse<T>> pageApi) {
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

    @SafeVarargs
    private final Map<String, String> buildPagedQueryParams(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            Pair<String, String>... namePatternPairs) {
        Map<String, String> queryParams = Maps.newHashMap();
        if (Objects.nonNull(maxResults) && maxResults > 0) {
            queryParams.put(MAX_RESULTS, maxResults.toString());
        }
        if (Objects.nonNull(pageToken)) {
            queryParams.put(PAGE_TOKEN, pageToken);
        }
        for (Pair<String, String> namePatternPair : namePatternPairs) {
            String namePatternKey = namePatternPair.getKey();
            String namePatternValue = namePatternPair.getValue();
            if (StringUtils.isNotEmpty(namePatternKey)
                    && StringUtils.isNotEmpty(namePatternValue)) {
                queryParams.put(namePatternKey, namePatternValue);
            }
        }
        return queryParams;
    }

    @VisibleForTesting
    RESTAuthFunction authFunction() {
        return restAuthFunction;
    }
}
