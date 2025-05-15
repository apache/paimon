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
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.ForwardBranchRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.rest.responses.PagedResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTUtil.extractPrefixMap;
import static org.apache.paimon.rest.auth.AuthProviderFactory.createAuthProvider;

/**
 * REST API for REST Catalog.
 *
 * @since 1.2.0
 */
@Public
public class RESTApi {

    public static final String QUERY_PARAMETER_WAREHOUSE_KEY = "warehouse";
    public static final String HEADER_PREFIX = "header.";
    public static final String MAX_RESULTS = "maxResults";
    public static final String PAGE_TOKEN = "pageToken";

    public static final String TABLE_NAME_PATTERN = "tableNamePattern";
    public static final String VIEW_NAME_PATTERN = "viewNamePattern";
    public static final String PARTITION_NAME_PATTERN = "partitionNamePattern";

    public static final long TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000L;

    public static final ObjectMapper OBJECT_MAPPER = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE;

    private final HttpClient client;
    private final RESTAuthFunction restAuthFunction;
    private final Options options;
    private final ResourcePaths resourcePaths;

    public RESTApi(Options options) {
        this(options, true);
    }

    public RESTApi(Options options, boolean configRequired) {
        this.client = new HttpClient(options.get(RESTCatalogOptions.URI));
        AuthProvider authProvider = createAuthProvider(options);
        Map<String, String> baseHeaders = Collections.emptyMap();
        if (configRequired) {
            String warehouse = options.get(WAREHOUSE);
            Map<String, String> queryParams =
                    StringUtils.isNotEmpty(warehouse)
                            ? ImmutableMap.of(
                                    QUERY_PARAMETER_WAREHOUSE_KEY, RESTUtil.encodeString(warehouse))
                            : ImmutableMap.of();
            baseHeaders = extractPrefixMap(options, HEADER_PREFIX);
            options =
                    new Options(
                            client.get(
                                            ResourcePaths.config(),
                                            queryParams,
                                            ConfigResponse.class,
                                            new RESTAuthFunction(
                                                    Collections.emptyMap(), authProvider))
                                    .merge(options.toMap()));
            baseHeaders.putAll(extractPrefixMap(options, HEADER_PREFIX));
        }
        this.restAuthFunction = new RESTAuthFunction(baseHeaders, authProvider);
        this.options = options;
        this.resourcePaths = ResourcePaths.forCatalogProperties(options);
    }

    public Options options() {
        return options;
    }

    public List<String> listDatabases() {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.databases(),
                                queryParams,
                                ListDatabasesResponse.class,
                                restAuthFunction));
    }

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

    public void createDatabase(String name, Map<String, String> properties) {
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        client.post(resourcePaths.databases(), request, restAuthFunction);
    }

    public GetDatabaseResponse getDatabase(String name) {
        return client.get(
                resourcePaths.database(name), GetDatabaseResponse.class, restAuthFunction);
    }

    public void dropDatabase(String name) {
        client.delete(resourcePaths.database(name), restAuthFunction);
    }

    public void alterDatabase(String name, List<String> removals, Map<String, String> updates) {
        client.post(
                resourcePaths.database(name),
                new AlterDatabaseRequest(removals, updates),
                AlterDatabaseResponse.class,
                restAuthFunction);
    }

    public List<String> listTables(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.tables(databaseName),
                                queryParams,
                                ListTablesResponse.class,
                                restAuthFunction));
    }

    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern) {
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
    }

    public PagedList<GetTableResponse> listTableDetailsPaged(
            String db,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern) {
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
        return new PagedList<>(tables, response.getNextPageToken());
    }

    public GetTableResponse getTable(Identifier identifier) {
        return client.get(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableResponse.class,
                restAuthFunction);
    }

    public TableSnapshot loadSnapshot(Identifier identifier) {
        GetTableSnapshotResponse response =
                client.get(
                        resourcePaths.tableSnapshot(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        GetTableSnapshotResponse.class,
                        restAuthFunction);
        return response.getSnapshot();
    }

    public boolean commitSnapshot(
            Identifier identifier, Snapshot snapshot, List<PartitionStatistics> statistics) {
        CommitTableRequest request = new CommitTableRequest(snapshot, statistics);
        CommitTableResponse response =
                client.post(
                        resourcePaths.commitTable(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        request,
                        CommitTableResponse.class,
                        restAuthFunction);
        return response.isSuccess();
    }

    public void rollbackTo(Identifier identifier, Instant instant) {
        RollbackTableRequest request = new RollbackTableRequest(instant);
        client.post(
                resourcePaths.rollbackTable(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    public void createTable(Identifier identifier, Schema schema) {
        CreateTableRequest request = new CreateTableRequest(identifier, schema);
        client.post(resourcePaths.tables(identifier.getDatabaseName()), request, restAuthFunction);
    }

    public void renameTable(Identifier fromTable, Identifier toTable) {
        RenameTableRequest request = new RenameTableRequest(fromTable, toTable);
        client.post(resourcePaths.renameTable(), request, restAuthFunction);
    }

    public void alterTable(Identifier identifier, List<SchemaChange> changes) {
        AlterTableRequest request = new AlterTableRequest(changes);
        client.post(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    public void authTableQuery(Identifier identifier, List<String> select, List<String> filter) {
        AuthTableQueryRequest request = new AuthTableQueryRequest(select, filter);
        client.post(
                resourcePaths.authTable(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    public void dropTable(Identifier identifier) {
        client.delete(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions) {
        MarkDonePartitionsRequest request = new MarkDonePartitionsRequest(partitions);
        client.post(
                resourcePaths.markDonePartitions(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

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
                                PARTITION_NAME_PATTERN,
                                partitionNamePattern),
                        ListPartitionsResponse.class,
                        restAuthFunction);
        List<Partition> partitions = response.getPartitions();
        if (partitions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(partitions, response.getNextPageToken());
    }

    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag) {
        CreateBranchRequest request = new CreateBranchRequest(branch, fromTag);
        client.post(
                resourcePaths.branches(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    public void dropBranch(Identifier identifier, String branch) {
        client.delete(
                resourcePaths.branch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                restAuthFunction);
    }

    public void fastForward(Identifier identifier, String branch) {
        ForwardBranchRequest request = new ForwardBranchRequest();
        client.post(
                resourcePaths.forwardBranch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                request,
                restAuthFunction);
    }

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

    public List<String> listFunctions() {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.functions(),
                                queryParams,
                                ListFunctionsResponse.class,
                                restAuthFunction));
    }

    public GetFunctionResponse getFunction(String functionName) {
        return client.get(
                resourcePaths.function(functionName), GetFunctionResponse.class, restAuthFunction);
    }

    public void createFunction(String functionName, org.apache.paimon.function.Function function) {
        client.post(
                resourcePaths.functions(), new CreateFunctionRequest(function), restAuthFunction);
    }

    public void dropFunction(String functionName) {
        client.delete(resourcePaths.function(functionName), restAuthFunction);
    }

    public void alterFunction(String functionName, List<FunctionChange> changes) {
        client.post(
                resourcePaths.function(functionName),
                new AlterFunctionRequest(changes),
                restAuthFunction);
    }

    public GetViewResponse getView(Identifier identifier) {
        return client.get(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                GetViewResponse.class,
                restAuthFunction);
    }

    public void dropView(Identifier identifier) {
        client.delete(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    public void createView(Identifier identifier, ViewSchema schema) {
        CreateViewRequest request = new CreateViewRequest(identifier, schema);
        client.post(resourcePaths.views(identifier.getDatabaseName()), request, restAuthFunction);
    }

    public List<String> listViews(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.views(databaseName),
                                queryParams,
                                ListViewsResponse.class,
                                restAuthFunction));
    }

    public PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
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
    }

    public PagedList<GetViewResponse> listViewDetailsPaged(
            String db,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
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
        return new PagedList<>(views, response.getNextPageToken());
    }

    public void renameView(Identifier fromView, Identifier toView) {
        RenameTableRequest request = new RenameTableRequest(fromView, toView);
        client.post(resourcePaths.renameView(), request, restAuthFunction);
    }

    public void alterView(Identifier identifier, List<ViewChange> viewChanges) {
        AlterViewRequest request = new AlterViewRequest(viewChanges);
        client.post(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    public GetTableTokenResponse loadTableToken(Identifier identifier) {
        return client.get(
                resourcePaths.tableToken(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableTokenResponse.class,
                restAuthFunction);
    }

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

    @VisibleForTesting
    RESTAuthFunction authFunction() {
        return restAuthFunction;
    }

    public static <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, clazz);
    }

    public static <T> String toJson(T t) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(t);
    }
}
