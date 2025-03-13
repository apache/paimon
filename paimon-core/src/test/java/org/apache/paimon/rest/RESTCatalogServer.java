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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.SupportsBranches;
import org.apache.paimon.catalog.SupportsSnapshots;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.fs.local.LocalFileIOLoader;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterPartitionsRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.DropPartitionsRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ErrorResponseResourceType;
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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** Mock REST server for testing. */
public class RESTCatalogServer {
    private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

    public static final int DEFAULT_MAX_RESULTS = 100;
    public static final String MAX_RESULTS = RESTCatalog.MAX_RESULTS;
    public static final String PAGE_TOKEN = RESTCatalog.PAGE_TOKEN;
    public static final String AUTHORIZATION_HEADER_KEY = "Authorization";

    private final String prefix;
    private final String databaseUri;

    private final FileSystemCatalog catalog;
    private final Dispatcher dispatcher;
    private final MockWebServer server;

    private final Map<String, Database> databaseStore = new HashMap<>();
    private final Map<String, TableMetadata> tableMetadataStore = new HashMap<>();
    private final Map<String, List<Partition>> tablePartitionsStore = new HashMap<>();
    private final Map<String, View> viewStore = new HashMap<>();
    private final Map<String, TableSnapshot> tableSnapshotStore = new HashMap<>();
    private final List<String> noPermissionDatabases = new ArrayList<>();
    private final List<String> noPermissionTables = new ArrayList<>();
    public final ConfigResponse configResponse;
    public final String warehouse;

    private final ResourcePaths resourcePaths;

    public RESTCatalogServer(
            String dataPath, AuthProvider authProvider, ConfigResponse config, String warehouse) {
        this.warehouse = warehouse;
        this.configResponse = config;
        this.prefix =
                this.configResponse.getDefaults().get(RESTCatalogInternalOptions.PREFIX.key());
        this.resourcePaths = new ResourcePaths(prefix);
        this.databaseUri = resourcePaths.databases();
        Options conf = new Options();
        this.configResponse.getDefaults().forEach(conf::setString);
        conf.setString(CatalogOptions.WAREHOUSE.key(), dataPath);
        CatalogContext context = CatalogContext.create(conf);
        Path warehousePath = new Path(dataPath);
        FileIO fileIO;
        try {
            fileIO = new LocalFileIO();
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.catalog = new FileSystemCatalog(fileIO, warehousePath, context.options());
        this.dispatcher = initDispatcher(authProvider);
        MockWebServer mockWebServer = new MockWebServer();
        mockWebServer.setDispatcher(dispatcher);
        server = mockWebServer;
    }

    public void start() throws IOException {
        server.start();
    }

    public String getUrl() {
        return server.url("").toString();
    }

    public void shutdown() throws IOException {
        server.shutdown();
    }

    public void setTableSnapshot(
            Identifier identifier,
            Snapshot snapshot,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        tableSnapshotStore.put(
                identifier.getFullName(),
                new TableSnapshot(
                        snapshot, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime));
    }

    public void setDataToken(Identifier identifier, RESTToken token) {
        DataTokenStore.putDataToken(warehouse, identifier.getFullName(), token);
    }

    public void removeDataToken(Identifier identifier) {
        DataTokenStore.removeDataToken(warehouse, identifier.getFullName());
    }

    public void addNoPermissionDatabase(String database) {
        noPermissionDatabases.add(database);
    }

    public void addNoPermissionTable(Identifier identifier) {
        noPermissionTables.add(identifier.getFullName());
    }

    public RESTToken getDataToken(Identifier identifier) {
        return DataTokenStore.getDataToken(warehouse, identifier.getFullName());
    }

    public Map<String, String> getHeader(RecordedRequest request) {
        Map<String, String> headers = new HashMap<>();
        for (Map.Entry<String, List<String>> header :
                request.getHeaders().toMultimap().entrySet()) {
            headers.put(header.getKey().toLowerCase(), header.getValue().get(0));
        }
        return headers;
    }

    public Dispatcher initDispatcher(AuthProvider authProvider) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String token = request.getHeaders().get(AUTHORIZATION_HEADER_KEY);
                RESTResponse response;
                try {
                    Map<String, String> headers = getHeader(request);
                    String[] paths = request.getPath().split("\\?");
                    String resourcePath = paths[0];
                    Map<String, String> parameters =
                            paths.length == 2 ? getParameters(paths[1]) : Collections.emptyMap();
                    String data = request.getBody().readUtf8();
                    RESTAuthParameter restAuthParameter =
                            new RESTAuthParameter(
                                    resourcePath, parameters, request.getMethod(), data);
                    String authToken =
                            authProvider
                                    .header(headers, restAuthParameter)
                                    .get(AUTHORIZATION_HEADER_KEY);
                    if (!authToken.equals(token)) {
                        return new MockResponse().setResponseCode(401);
                    }
                    if (request.getPath().startsWith(resourcePaths.config())
                            && request.getRequestUrl()
                                    .queryParameter(RESTCatalog.QUERY_PARAMETER_WAREHOUSE_KEY)
                                    .equals(warehouse)) {
                        return mockResponse(configResponse, 200);
                    } else if (databaseUri.equals(request.getPath())
                            || request.getPath().contains(databaseUri + "?")) {
                        return databasesApiHandler(restAuthParameter.method(), data, parameters);
                    } else if (resourcePaths.renameTable().equals(request.getPath())) {
                        return renameTableHandle(restAuthParameter.data());
                    } else if (resourcePaths.renameView().equals(request.getPath())) {
                        return renameViewHandle(restAuthParameter.data());
                    } else if (request.getPath().startsWith(databaseUri)) {
                        String[] resources =
                                request.getPath()
                                        .substring((databaseUri + "/").length())
                                        .split("/");
                        String databaseName = RESTUtil.decodeString(resources[0]);
                        if (noPermissionDatabases.contains(databaseName)) {
                            throw new Catalog.DatabaseNoPermissionException(databaseName);
                        }
                        if (!databaseStore.containsKey(databaseName)) {
                            throw new Catalog.DatabaseNotExistException(databaseName);
                        }
                        boolean isViews = resources.length == 2 && resources[1].startsWith("views");
                        boolean isViewsDetails =
                                resources.length == 2 && resources[1].startsWith("view-details");
                        boolean isTables =
                                resources.length == 2 && resources[1].startsWith("tables");
                        boolean isTableDetails =
                                resources.length == 2 && resources[1].startsWith("table-details");
                        boolean isViewRename =
                                resources.length == 3
                                        && "views".equals(resources[1])
                                        && "rename".equals(resources[2]);
                        boolean isView =
                                resources.length == 3
                                        && "views".equals(resources[1])
                                        && !"rename".equals(resources[2]);
                        boolean isTable =
                                resources.length == 3
                                        && "tables".equals(resources[1])
                                        && !"rename".equals(resources[2])
                                        && !"commit".equals(resources[2]);
                        boolean isTableToken =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "token".equals(resources[3]);
                        boolean isTableSnapshot =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "snapshot".equals(resources[3]);
                        boolean isCommitSnapshot =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "commit".equals(resources[3]);
                        boolean isPartitions =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && resources[3].startsWith("partitions");

                        boolean isDropPartitions =
                                resources.length == 5
                                        && "tables".equals(resources[1])
                                        && "partitions".equals(resources[3])
                                        && "drop".equals(resources[4]);
                        boolean isAlterPartitions =
                                resources.length == 5
                                        && "tables".equals(resources[1])
                                        && "partitions".equals(resources[3])
                                        && "alter".equals(resources[4]);
                        boolean isMarkDonePartitions =
                                resources.length == 5
                                        && "tables".equals(resources[1])
                                        && "partitions".equals(resources[3])
                                        && "mark".equals(resources[4]);

                        boolean isBranches =
                                resources.length >= 4
                                        && "tables".equals(resources[1])
                                        && "branches".equals(resources[3]);
                        Identifier identifier =
                                resources.length >= 3
                                                && !"rename".equals(resources[2])
                                                && !"commit".equals(resources[2])
                                        ? Identifier.create(
                                                databaseName, RESTUtil.decodeString(resources[2]))
                                        : null;
                        if (identifier != null && "tables".equals(resources[1])) {
                            if (!identifier.isSystemTable()
                                    && !tableMetadataStore.containsKey(identifier.getFullName())) {
                                throw new Catalog.TableNotExistException(identifier);
                            }
                            if (noPermissionTables.contains(identifier.getFullName())) {
                                throw new Catalog.TableNoPermissionException(identifier);
                            }
                        }
                        // validate partition
                        if (isPartitions
                                || isDropPartitions
                                || isAlterPartitions
                                || isMarkDonePartitions) {
                            String tableName = RESTUtil.decodeString(resources[2]);
                            Optional<MockResponse> error =
                                    checkTablePartitioned(
                                            Identifier.create(databaseName, tableName));
                            if (error.isPresent()) {
                                return error.get();
                            }
                        }
                        if (isDropPartitions) {
                            return dropPartitionsHandle(identifier, restAuthParameter.data());
                        } else if (isAlterPartitions) {
                            return alterPartitionsHandle(identifier, restAuthParameter.data());
                        } else if (isMarkDonePartitions) {
                            MarkDonePartitionsRequest markDonePartitionsRequest =
                                    OBJECT_MAPPER.readValue(data, MarkDonePartitionsRequest.class);
                            catalog.markDonePartitions(
                                    identifier, markDonePartitionsRequest.getPartitionSpecs());
                            return new MockResponse().setResponseCode(200);
                        } else if (isPartitions) {
                            return partitionsApiHandle(
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    parameters,
                                    identifier);
                        } else if (isBranches) {
                            return branchApiHandle(
                                    resources,
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    identifier);
                        } else if (isTableToken) {
                            return getDataTokenHandle(identifier);
                        } else if (isTableSnapshot) {
                            return snapshotHandle(identifier);
                        } else if (isCommitSnapshot) {
                            return commitTableHandle(identifier, restAuthParameter.data());
                        } else if (isTable) {
                            return tableHandle(
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    identifier);
                        } else if (isTables) {
                            return tablesHandle(
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    databaseName,
                                    parameters);
                        } else if (isTableDetails) {
                            return tableDetailsHandle(parameters, databaseName);
                        } else if (isViews) {
                            return viewsHandle(
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    databaseName,
                                    parameters);
                        } else if (isViewsDetails) {
                            return viewDetailsHandle(
                                    restAuthParameter.method(), databaseName, parameters);
                        } else if (isView) {
                            return viewHandle(restAuthParameter.method(), identifier);
                        } else {
                            return databaseHandle(
                                    restAuthParameter.method(),
                                    restAuthParameter.data(),
                                    databaseName);
                        }
                    }
                    return new MockResponse().setResponseCode(404);
                } catch (Catalog.DatabaseNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.TableNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.ColumnNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.COLUMN,
                                    e.column(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.DatabaseNoPermissionException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    403);
                    return mockResponse(response, 403);
                } catch (Catalog.TableNoPermissionException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    403);
                    return mockResponse(response, 403);
                } catch (Catalog.DatabaseAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.TableAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.ColumnAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.COLUMN,
                                    e.column(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.ViewNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.VIEW,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.ViewAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponseResourceType.VIEW,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (IllegalArgumentException e) {
                    response = new ErrorResponse(null, null, e.getMessage(), 400);
                    return mockResponse(response, 400);
                } catch (Exception e) {
                    e.printStackTrace();
                    if (e.getCause() instanceof IllegalArgumentException) {
                        response =
                                new ErrorResponse(
                                        null, null, e.getCause().getCause().getMessage(), 400);
                        return mockResponse(response, 400);
                    } else if (e instanceof UnsupportedOperationException) {
                        response = new ErrorResponse(null, null, e.getMessage(), 501);
                        return mockResponse(response, 501);
                    } else if (e instanceof IllegalStateException) {
                        response = new ErrorResponse(null, null, e.getMessage(), 500);
                        return mockResponse(response, 500);
                    }
                    return new MockResponse().setResponseCode(500);
                }
            }
        };
    }

    private MockResponse getDataTokenHandle(Identifier tableIdentifier) throws Exception {
        RESTToken dataToken = getDataToken(tableIdentifier);
        if (dataToken == null) {
            long currentTimeMillis = System.currentTimeMillis() + 60_000;
            dataToken =
                    new RESTToken(
                            ImmutableMap.of(
                                    "akId",
                                    "akId" + currentTimeMillis,
                                    "akSecret",
                                    "akSecret" + currentTimeMillis),
                            currentTimeMillis);
            DataTokenStore.putDataToken(warehouse, tableIdentifier.getFullName(), dataToken);
        }
        GetTableTokenResponse getTableTokenResponse =
                new GetTableTokenResponse(dataToken.token(), dataToken.expireAtMillis());
        return new MockResponse()
                .setResponseCode(200)
                .setBody(OBJECT_MAPPER.writeValueAsString(getTableTokenResponse));
    }

    private MockResponse snapshotHandle(Identifier identifier) throws Exception {
        RESTResponse response;
        Optional<TableSnapshot> snapshotOptional =
                Optional.ofNullable(tableSnapshotStore.get(identifier.getFullName()));
        if (!snapshotOptional.isPresent()) {
            response =
                    new ErrorResponse(
                            ErrorResponseResourceType.SNAPSHOT,
                            identifier.getDatabaseName(),
                            "No Snapshot",
                            404);
            return mockResponse(response, 404);
        }
        GetTableSnapshotResponse getTableSnapshotResponse =
                new GetTableSnapshotResponse(snapshotOptional.get());
        return new MockResponse()
                .setResponseCode(200)
                .setBody(OBJECT_MAPPER.writeValueAsString(getTableSnapshotResponse));
    }

    private Optional<MockResponse> checkTablePartitioned(Identifier identifier) {
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(identifier.getFullName());
            boolean partitioned =
                    CoreOptions.fromMap(tableMetadata.schema().options())
                            .partitionedTableInMetastore();
            if (!partitioned) {
                return Optional.of(mockResponse(new ErrorResponse(null, null, "", 501), 501));
            }
            return Optional.empty();
        }
        return Optional.of(
                mockResponse(
                        new ErrorResponse(ErrorResponseResourceType.TABLE, null, "", 404), 404));
    }

    private MockResponse commitTableHandle(Identifier identifier, String data) throws Exception {
        CommitTableRequest requestBody = OBJECT_MAPPER.readValue(data, CommitTableRequest.class);
        if (noPermissionTables.contains(identifier.getFullName())) {
            throw new Catalog.TableNoPermissionException(identifier);
        }
        if (!tableMetadataStore.containsKey(identifier.getFullName())) {
            throw new Catalog.TableNotExistException(identifier);
        }
        FileStoreTable table = getFileTable(identifier);
        RenamingSnapshotCommit commit =
                new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        String branchName = identifier.getBranchName();
        if (branchName == null) {
            branchName = "main";
        }
        boolean success =
                commit.commit(requestBody.getSnapshot(), branchName, Collections.emptyList());
        commitSnapshot(identifier, requestBody.getSnapshot(), null);
        CommitTableResponse response = new CommitTableResponse(success);
        return mockResponse(response, 200);
    }

    private MockResponse databasesApiHandler(
            String method, String data, Map<String, String> parameters) throws Exception {
        RESTResponse response;
        switch (method) {
            case "GET":
                List<String> databases = new ArrayList<>(databaseStore.keySet());
                return generateFinalListDatabasesResponse(parameters, databases);
            case "POST":
                CreateDatabaseRequest requestBody =
                        OBJECT_MAPPER.readValue(data, CreateDatabaseRequest.class);
                String databaseName = requestBody.getName();
                if (noPermissionDatabases.contains(databaseName)) {
                    throw new Catalog.DatabaseNoPermissionException(databaseName);
                }
                catalog.createDatabase(databaseName, false);
                databaseStore.put(
                        databaseName, Database.of(databaseName, requestBody.getOptions(), null));
                response = new CreateDatabaseResponse(databaseName, requestBody.getOptions());
                return mockResponse(response, 200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse generateFinalListDatabasesResponse(
            Map<String, String> parameters, List<String> databases) {
        RESTResponse response;
        if (!databases.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponseResourceType.TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);
            PagedList<String> pagedDbs = buildPagedEntities(databases, maxResults, pageToken);
            response =
                    new ListDatabasesResponse(pagedDbs.getElements(), pagedDbs.getNextPageToken());
        } else {
            response = new ListDatabasesResponse(new ArrayList<>(), null);
        }
        return mockResponse(response, 200);
    }

    private <T> PagedList<T> buildPagedEntities(List<T> names, int maxResults, String pageToken) {
        List<T> sortedNames = names.stream().sorted(this::compareTo).collect(Collectors.toList());
        List<T> pagedNames = new ArrayList<>();
        for (T sortedName : sortedNames) {
            if (pagedNames.size() < maxResults) {
                if (pageToken == null) {
                    pagedNames.add(sortedName);
                } else if (this.compareTo(sortedName, pageToken) > 0) {
                    pagedNames.add(sortedName);
                }
            } else {
                break;
            }
        }
        if (maxResults > sortedNames.size() && pageToken == null) {
            return new PagedList<>(pagedNames, null);
        } else {
            String nextPageToken = getNextPageTokenForEntities(pagedNames, maxResults);
            return new PagedList<>(pagedNames, nextPageToken);
        }
    }

    private int compareTo(Object o1, Object o2) {
        String pagedKey1 = getPagedKey(o1);
        String pagedKey2 = getPagedKey(o2);
        if (Objects.isNull(pagedKey1) && Objects.nonNull(pagedKey2)) {
            return 0;
        } else if (Objects.isNull(pagedKey1)) {
            return -1;
        } else if (Objects.isNull(pagedKey2)) {
            return 1;
        } else {
            return pagedKey1.compareTo(pagedKey2);
        }
    }

    private MockResponse databaseHandle(String method, String data, String databaseName)
            throws Exception {
        RESTResponse response;
        Database database;
        if (databaseStore.containsKey(databaseName)) {
            switch (method) {
                case "GET":
                    database = databaseStore.get(databaseName);
                    response =
                            new GetDatabaseResponse(
                                    UUID.randomUUID().toString(),
                                    database.name(),
                                    database.options());
                    return mockResponse(response, 200);
                case "DELETE":
                    catalog.dropDatabase(databaseName, false, true);
                    databaseStore.remove(databaseName);
                    return new MockResponse().setResponseCode(200);
                case "POST":
                    AlterDatabaseRequest requestBody =
                            OBJECT_MAPPER.readValue(data, AlterDatabaseRequest.class);
                    List<PropertyChange> changes = new ArrayList<>();
                    for (String property : requestBody.getRemovals()) {
                        changes.add(PropertyChange.removeProperty(property));
                    }
                    for (Map.Entry<String, String> entry : requestBody.getUpdates().entrySet()) {
                        changes.add(PropertyChange.setProperty(entry.getKey(), entry.getValue()));
                    }
                    if (databaseStore.containsKey(databaseName)) {
                        Pair<Map<String, String>, Set<String>> setPropertiesToRemoveKeys =
                                PropertyChange.getSetPropertiesToRemoveKeys(changes);
                        Map<String, String> setProperties = setPropertiesToRemoveKeys.getLeft();
                        Set<String> removeKeys = setPropertiesToRemoveKeys.getRight();
                        database = databaseStore.get(databaseName);
                        Map<String, String> parameter = new HashMap<>(database.options());
                        if (!setProperties.isEmpty()) {
                            parameter.putAll(setProperties);
                        }
                        if (!removeKeys.isEmpty()) {
                            parameter.keySet().removeAll(removeKeys);
                        }
                        Database alterDatabase = Database.of(databaseName, parameter, null);
                        databaseStore.put(databaseName, alterDatabase);
                    } else {
                        throw new Catalog.DatabaseNotExistException(databaseName);
                    }
                    AlterDatabaseResponse alterDatabaseResponse =
                            new AlterDatabaseResponse(
                                    requestBody.getRemovals(),
                                    requestBody.getUpdates().keySet().stream()
                                            .collect(Collectors.toList()),
                                    Collections.emptyList());
                    return mockResponse(alterDatabaseResponse, 200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
        return new MockResponse().setResponseCode(404);
    }

    private MockResponse tablesHandle(
            String method, String data, String databaseName, Map<String, String> parameters)
            throws Exception {
        if (databaseStore.containsKey(databaseName)) {
            switch (method) {
                case "GET":
                    List<String> tables = listTables(databaseName);
                    return generateFinalListTablesResponse(parameters, tables);
                case "POST":
                    CreateTableRequest requestBody =
                            OBJECT_MAPPER.readValue(data, CreateTableRequest.class);
                    Identifier identifier = requestBody.getIdentifier();
                    Schema schema = requestBody.getSchema();
                    TableMetadata tableMetadata;
                    if (isFormatTable(schema)) {
                        tableMetadata = createFormatTable(identifier, schema);
                    } else {
                        catalog.createTable(identifier, schema, false);
                        tableMetadata =
                                createTableMetadata(
                                        requestBody.getIdentifier(),
                                        1L,
                                        requestBody.getSchema(),
                                        UUID.randomUUID().toString(),
                                        false);
                    }
                    tableMetadataStore.put(
                            requestBody.getIdentifier().getFullName(), tableMetadata);
                    return new MockResponse().setResponseCode(200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
        return mockResponse(
                new ErrorResponse(ErrorResponseResourceType.DATABASE, null, "", 404), 404);
    }

    private List<String> listTables(String databaseName) {
        List<String> tables = new ArrayList<>();
        for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())) {
                tables.add(identifier.getTableName());
            }
        }
        return tables;
    }

    private MockResponse generateFinalListTablesResponse(
            Map<String, String> parameters, List<String> tables) {
        RESTResponse response;
        if (!tables.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponseResourceType.TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

            PagedList<String> pagedTables = buildPagedEntities(tables, maxResults, pageToken);
            response =
                    new ListTablesResponse(
                            pagedTables.getElements(), pagedTables.getNextPageToken());
        } else {
            response = new ListTablesResponse(new ArrayList<>(), null);
        }
        return mockResponse(response, 200);
    }

    private MockResponse tableDetailsHandle(Map<String, String> parameters, String databaseName) {
        RESTResponse response;
        List<GetTableResponse> tableDetails = listTableDetails(databaseName);
        if (!tableDetails.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponseResourceType.TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.get(PAGE_TOKEN);
            PagedList<GetTableResponse> pagedTableDetails =
                    buildPagedEntities(tableDetails, maxResults, pageToken);
            response =
                    new ListTableDetailsResponse(
                            pagedTableDetails.getElements(), pagedTableDetails.getNextPageToken());
        } else {
            response = new ListTableDetailsResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private List<GetTableResponse> listTableDetails(String databaseName) {
        List<GetTableResponse> tableDetails = new ArrayList<>();
        for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())) {
                GetTableResponse getTableResponse =
                        new GetTableResponse(
                                entry.getValue().uuid(),
                                identifier.getTableName(),
                                entry.getValue().isExternal(),
                                entry.getValue().schema().id(),
                                entry.getValue().schema().toSchema());
                tableDetails.add(getTableResponse);
            }
        }
        return tableDetails;
    }

    private boolean isFormatTable(Schema schema) {
        return Options.fromMap(schema.options()).get(TYPE) == FORMAT_TABLE;
    }

    private MockResponse tableHandle(String method, String data, Identifier identifier)
            throws Exception {
        RESTResponse response;
        if (noPermissionTables.contains(identifier.getFullName())) {
            throw new Catalog.TableNoPermissionException(identifier);
        }
        switch (method) {
            case "GET":
                TableMetadata tableMetadata;
                identifier.isSystemTable();
                if (identifier.isSystemTable()) {
                    TableSchema schema = catalog.loadTableSchema(identifier);
                    tableMetadata =
                            createTableMetadata(
                                    identifier, schema.id(), schema.toSchema(), null, false);
                } else {
                    tableMetadata = tableMetadataStore.get(identifier.getFullName());
                }
                response =
                        new GetTableResponse(
                                tableMetadata.uuid(),
                                identifier.getTableName(),
                                tableMetadata.isExternal(),
                                tableMetadata.schema().id(),
                                tableMetadata.schema().toSchema());
                return mockResponse(response, 200);
            case "POST":
                AlterTableRequest requestBody =
                        OBJECT_MAPPER.readValue(data, AlterTableRequest.class);
                alterTableImpl(identifier, requestBody.getChanges());
                return new MockResponse().setResponseCode(200);
            case "DELETE":
                try {
                    catalog.dropTable(identifier, false);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
                tableMetadataStore.remove(identifier.getFullName());
                return new MockResponse().setResponseCode(200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse renameTableHandle(String data) throws Exception {
        RenameTableRequest requestBody = OBJECT_MAPPER.readValue(data, RenameTableRequest.class);
        Identifier fromTable = requestBody.getSource();
        Identifier toTable = requestBody.getDestination();
        if (noPermissionTables.contains(fromTable.getFullName())) {
            throw new Catalog.TableNoPermissionException(fromTable);
        } else if (tableMetadataStore.containsKey(fromTable.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(fromTable.getFullName());
            if (!isFormatTable(tableMetadata.schema().toSchema())) {
                catalog.renameTable(requestBody.getSource(), requestBody.getDestination(), false);
            }
            if (tableMetadataStore.containsKey(toTable.getFullName())) {
                throw new Catalog.TableAlreadyExistException(toTable);
            }
            tableMetadataStore.remove(fromTable.getFullName());
            tableMetadataStore.put(toTable.getFullName(), tableMetadata);
        } else {
            throw new Catalog.TableNotExistException(fromTable);
        }
        return new MockResponse().setResponseCode(200);
    }

    private MockResponse partitionsApiHandle(
            String method, String data, Map<String, String> parameters, Identifier tableIdentifier)
            throws Exception {
        switch (method) {
            case "GET":
                List<Partition> partitions = new ArrayList<>();
                for (Map.Entry<String, List<Partition>> entry : tablePartitionsStore.entrySet()) {
                    String tableName = Identifier.fromString(entry.getKey()).getTableName();
                    if (tableName.equals(tableIdentifier.getTableName())) {
                        partitions.addAll(entry.getValue());
                    }
                }
                return generateFinalListPartitionsResponse(parameters, partitions);
            case "POST":
                CreatePartitionsRequest requestBody =
                        OBJECT_MAPPER.readValue(data, CreatePartitionsRequest.class);
                tablePartitionsStore.put(
                        tableIdentifier.getFullName(),
                        requestBody.getPartitionSpecs().stream()
                                .map(partition -> spec2Partition(partition))
                                .collect(Collectors.toList()));
                return new MockResponse().setResponseCode(200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse branchApiHandle(
            String[] resources, String method, String data, Identifier identifier)
            throws Exception {
        RESTResponse response;
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        BranchManager branchManager = table.branchManager();
        String fromTag = "";
        String branch = "";
        Identifier branchIdentifier;
        try {
            switch (method) {
                case "DELETE":
                    branch = RESTUtil.decodeString(resources[4]);
                    branchIdentifier =
                            new Identifier(
                                    identifier.getDatabaseName(),
                                    identifier.getTableName(),
                                    branch);
                    table.deleteBranch(branch);
                    tableMetadataStore.remove(branchIdentifier.getFullName());
                    return new MockResponse().setResponseCode(200);
                case "GET":
                    List<String> branches = branchManager.branches();
                    response = new ListBranchesResponse(branches.isEmpty() ? null : branches);
                    return mockResponse(response, 200);
                case "POST":
                    if (resources.length == 6) {
                        branch = RESTUtil.decodeString(resources[4]);
                        branchManager.fastForward(branch);
                    } else {
                        CreateBranchRequest requestBody =
                                OBJECT_MAPPER.readValue(data, CreateBranchRequest.class);
                        branch = requestBody.branch();
                        if (requestBody.fromTag() == null) {
                            branchManager.createBranch(requestBody.branch());
                        } else {
                            fromTag = requestBody.fromTag();
                            branchManager.createBranch(requestBody.branch(), requestBody.fromTag());
                        }
                        branchIdentifier =
                                new Identifier(
                                        identifier.getDatabaseName(),
                                        identifier.getTableName(),
                                        requestBody.branch());
                        tableMetadataStore.put(
                                branchIdentifier.getFullName(),
                                tableMetadataStore.get(identifier.getFullName()));
                    }
                    return new MockResponse().setResponseCode(200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        } catch (Exception e) {
            if (e.getMessage().contains("Tag")) {
                response =
                        new ErrorResponse(
                                ErrorResponseResourceType.TAG, fromTag, e.getMessage(), 404);
                return mockResponse(response, 404);
            }
            if (e.getMessage().contains("Branch name")
                    && e.getMessage().contains("already exists")) {
                response =
                        new ErrorResponse(
                                ErrorResponseResourceType.BRANCH, branch, e.getMessage(), 409);
                return mockResponse(response, 409);
            }
            if (e.getMessage().contains("Branch name")
                    && e.getMessage().contains("doesn't exist")) {
                response =
                        new ErrorResponse(
                                ErrorResponseResourceType.BRANCH, branch, e.getMessage(), 404);
                return mockResponse(response, 404);
            }
        }
        return new MockResponse().setResponseCode(404);
    }

    private MockResponse generateFinalListPartitionsResponse(
            Map<String, String> parameters, List<Partition> partitions) {
        RESTResponse response;
        if (Objects.nonNull(partitions) && !partitions.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponseResourceType.TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

            PagedList<Partition> pagedPartitions =
                    buildPagedEntities(partitions, maxResults, pageToken);
            response =
                    new ListPartitionsResponse(
                            pagedPartitions.getElements(), pagedPartitions.getNextPageToken());
        } else {
            response = new ListPartitionsResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private MockResponse viewsHandle(
            String method, String data, String databaseName, Map<String, String> parameters)
            throws Exception {
        switch (method) {
            case "GET":
                List<String> views = listViews(databaseName);
                return generateFinalListViewsResponse(parameters, views);
            case "POST":
                CreateViewRequest requestBody =
                        OBJECT_MAPPER.readValue(data, CreateViewRequest.class);
                Identifier identifier = requestBody.getIdentifier();
                ViewSchema schema = requestBody.getSchema();
                ViewImpl view =
                        new ViewImpl(
                                requestBody.getIdentifier(),
                                schema.fields(),
                                schema.query(),
                                schema.dialects(),
                                schema.comment(),
                                schema.options());
                if (viewStore.containsKey(identifier.getFullName())) {
                    throw new Catalog.ViewAlreadyExistException(identifier);
                }
                viewStore.put(identifier.getFullName(), view);
                return new MockResponse().setResponseCode(200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private List<String> listViews(String databaseName) {
        return viewStore.keySet().stream()
                .map(Identifier::fromString)
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(Identifier::getTableName)
                .collect(Collectors.toList());
    }

    private MockResponse generateFinalListViewsResponse(
            Map<String, String> parameters, List<String> views) {
        RESTResponse response;
        if (!views.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponseResourceType.TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

            PagedList<String> pagedViews = buildPagedEntities(views, maxResults, pageToken);
            response =
                    new ListViewsResponse(pagedViews.getElements(), pagedViews.getNextPageToken());
        } else {
            response = new ListViewsResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private MockResponse viewDetailsHandle(
            String method, String databaseName, Map<String, String> parameters) {
        RESTResponse response;
        if ("GET".equals(method)) {

            List<GetViewResponse> viewDetails = listViewDetails(databaseName);
            if (!viewDetails.isEmpty()) {

                int maxResults;
                try {
                    maxResults = getMaxResults(parameters);
                } catch (Exception e) {
                    LOG.error(
                            "parse maxResults {} to int failed",
                            parameters.getOrDefault(MAX_RESULTS, null));
                    return mockResponse(
                            new ErrorResponse(
                                    ErrorResponseResourceType.TABLE,
                                    null,
                                    "invalid input queryParameter maxResults"
                                            + parameters.get(MAX_RESULTS),
                                    400),
                            400);
                }
                String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

                PagedList<GetViewResponse> pagedViewDetails =
                        buildPagedEntities(viewDetails, maxResults, pageToken);
                response =
                        new ListViewDetailsResponse(
                                pagedViewDetails.getElements(),
                                pagedViewDetails.getNextPageToken());
            } else {
                response = new ListViewsResponse(Collections.emptyList(), null);
            }
            return mockResponse(response, 200);
        } else {
            return new MockResponse().setResponseCode(404);
        }
    }

    private List<GetViewResponse> listViewDetails(String databaseName) {
        return viewStore.keySet().stream()
                .map(Identifier::fromString)
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(
                        identifier -> {
                            View view = viewStore.get(identifier.getFullName());
                            ViewSchema schema =
                                    new ViewSchema(
                                            view.rowType().getFields(),
                                            view.query(),
                                            view.dialects(),
                                            view.comment().orElse(null),
                                            view.options());
                            return new GetViewResponse("id", identifier.getTableName(), schema);
                        })
                .collect(Collectors.toList());
    }

    private MockResponse viewHandle(String method, Identifier identifier) throws Exception {
        RESTResponse response;
        if (viewStore.containsKey(identifier.getFullName())) {
            switch (method) {
                case "GET":
                    if (viewStore.containsKey(identifier.getFullName())) {
                        View view = viewStore.get(identifier.getFullName());
                        ViewSchema schema =
                                new ViewSchema(
                                        view.rowType().getFields(),
                                        view.query(),
                                        view.dialects(),
                                        view.comment().orElse(null),
                                        view.options());
                        response = new GetViewResponse("id", identifier.getTableName(), schema);
                        return mockResponse(response, 200);
                    }
                    throw new Catalog.ViewNotExistException(identifier);
                case "DELETE":
                    viewStore.remove(identifier.getFullName());
                    return new MockResponse().setResponseCode(200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
        throw new Catalog.ViewNotExistException(identifier);
    }

    private MockResponse renameViewHandle(String data) throws Exception {
        RenameTableRequest requestBody = OBJECT_MAPPER.readValue(data, RenameTableRequest.class);
        Identifier fromView = requestBody.getSource();
        Identifier toView = requestBody.getDestination();
        if (!viewStore.containsKey(fromView.getFullName())) {
            throw new Catalog.ViewNotExistException(fromView);
        }
        if (viewStore.containsKey(toView.getFullName())) {
            throw new Catalog.ViewAlreadyExistException(toView);
        }
        if (viewStore.containsKey(fromView.getFullName())) {
            View view = viewStore.get(fromView.getFullName());
            viewStore.remove(fromView.getFullName());
            viewStore.put(toView.getFullName(), view);
        }
        return new MockResponse().setResponseCode(200);
    }

    protected void alterTableImpl(Identifier identifier, List<SchemaChange> changes)
            throws Catalog.TableNotExistException, Catalog.ColumnAlreadyExistException,
                    Catalog.ColumnNotExistException {
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(identifier.getFullName());
            TableSchema schema = tableMetadata.schema();
            if (isFormatTable(schema.toSchema())) {
                throw new UnsupportedOperationException("Only data table support alter table.");
            }
            try {
                catalog.alterTable(identifier, changes, false);
                FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                TableSchema newSchema = table.schema();
                TableMetadata newTableMetadata =
                        createTableMetadata(
                                identifier,
                                newSchema.id(),
                                newSchema.toSchema(),
                                tableMetadata.uuid(),
                                tableMetadata.isExternal());
                tableMetadataStore.put(identifier.getFullName(), newTableMetadata);
            } catch (Catalog.TableNotExistException
                    | Catalog.ColumnAlreadyExistException
                    | Catalog.ColumnNotExistException
                    | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean commitSnapshot(
            Identifier identifier, Snapshot snapshot, List<Partition> statistics)
            throws Catalog.TableNotExistException {
        FileStoreTable table = getFileTable(identifier);
        RenamingSnapshotCommit commit =
                new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        String branchName = identifier.getBranchName();
        if (branchName == null) {
            branchName = "main";
        }
        try {
            boolean success = commit.commit(snapshot, branchName, Collections.emptyList());
            tableSnapshotStore.compute(
                    identifier.getFullName(),
                    (k, old) -> {
                        long recordCount = 0;
                        long fileSizeInBytes = 0;
                        long fileCount = 0;
                        long lastFileCreationTime = 0;
                        if (statistics != null) {
                            for (Partition partition : statistics) {
                                recordCount += partition.recordCount();
                                fileSizeInBytes += partition.fileSizeInBytes();
                                fileCount += partition.fileCount();
                                if (partition.lastFileCreationTime() > lastFileCreationTime) {
                                    lastFileCreationTime = partition.lastFileCreationTime();
                                }
                            }
                        }
                        if (old != null) {
                            recordCount += old.recordCount();
                            fileSizeInBytes += old.fileSizeInBytes();
                            fileCount += old.fileCount();
                            if (old.lastFileCreationTime() > lastFileCreationTime) {
                                lastFileCreationTime = old.lastFileCreationTime();
                            }
                        }
                        return new TableSnapshot(
                                snapshot,
                                recordCount,
                                fileCount,
                                lastFileCreationTime,
                                fileSizeInBytes);
                    });
            return success;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MockResponse dropPartitionsHandle(Identifier identifier, String data)
            throws Catalog.TableNotExistException, JsonProcessingException {
        DropPartitionsRequest dropPartitionsRequest =
                OBJECT_MAPPER.readValue(data, DropPartitionsRequest.class);
        List<Map<String, String>> partitionSpecs = dropPartitionsRequest.getPartitionSpecs();
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            List<Partition> existPartitions = tablePartitionsStore.get(identifier.getFullName());
            partitionSpecs.forEach(
                    partition -> {
                        for (Map.Entry<String, String> entry : partition.entrySet()) {
                            existPartitions.stream()
                                    .filter(
                                            p ->
                                                    p.spec().containsKey(entry.getKey())
                                                            && p.spec()
                                                                    .get(entry.getKey())
                                                                    .equals(entry.getValue()))
                                    .findFirst()
                                    .ifPresent(
                                            existPartition ->
                                                    existPartitions.remove(existPartition));
                        }
                    });
            return new MockResponse().setResponseCode(200);

        } else {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    private MockResponse alterPartitionsHandle(Identifier identifier, String data)
            throws Catalog.TableNotExistException, JsonProcessingException {
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            AlterPartitionsRequest alterPartitionsRequest =
                    OBJECT_MAPPER.readValue(data, AlterPartitionsRequest.class);
            List<Partition> partitions = alterPartitionsRequest.getPartitions();
            List<Partition> existPartitions = tablePartitionsStore.get(identifier.getFullName());
            partitions.forEach(
                    partition -> {
                        for (Map.Entry<String, String> entry : partition.spec().entrySet()) {
                            existPartitions.stream()
                                    .filter(
                                            p ->
                                                    p.spec().containsKey(entry.getKey())
                                                            && p.spec()
                                                                    .get(entry.getKey())
                                                                    .equals(entry.getValue()))
                                    .findFirst()
                                    .ifPresent(
                                            existPartition ->
                                                    existPartitions.remove(existPartition));
                        }
                    });
            existPartitions.addAll(partitions);
            tablePartitionsStore.put(identifier.getFullName(), existPartitions);
            return new MockResponse().setResponseCode(200);
        } else {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    private MockResponse mockResponse(RESTResponse response, int httpCode) {
        try {
            return new MockResponse()
                    .setResponseCode(httpCode)
                    .setBody(OBJECT_MAPPER.writeValueAsString(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private TableMetadata createTableMetadata(
            Identifier identifier, long schemaId, Schema schema, String uuid, boolean isExternal) {
        Map<String, String> options = new HashMap<>(schema.options());
        Path path = catalog.getTableLocation(identifier);
        String restPath =
                path.toString().replaceFirst(LocalFileIOLoader.SCHEME, RESTFileIOTestLoader.SCHEME);
        options.put(PATH.key(), restPath);
        TableSchema tableSchema =
                new TableSchema(
                        schemaId,
                        schema.fields(),
                        schema.fields().size() - 1,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        options,
                        schema.comment());
        TableMetadata tableMetadata = new TableMetadata(tableSchema, isExternal, uuid);
        return tableMetadata;
    }

    private TableMetadata createFormatTable(Identifier identifier, Schema schema) {
        return createTableMetadata(identifier, 1L, schema, UUID.randomUUID().toString(), true);
    }

    private Partition spec2Partition(Map<String, String> spec) {
        // todo: need update
        return new Partition(spec, 123, 456, 789, 123);
    }

    private FileStoreTable getFileTable(Identifier identifier)
            throws Catalog.TableNotExistException {
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(identifier.getFullName());
            TableSchema schema = tableMetadata.schema();
            CatalogEnvironment catalogEnv =
                    new CatalogEnvironment(
                            identifier,
                            tableMetadata.uuid(),
                            catalog.catalogLoader(),
                            catalog.lockFactory().orElse(null),
                            catalog.lockContext().orElse(null),
                            catalog instanceof SupportsSnapshots,
                            catalog instanceof SupportsBranches);
            Path path = new Path(schema.options().get(PATH.key()));
            FileIO dataFileIO = catalog.fileIO();
            FileStoreTable table =
                    FileStoreTableFactory.create(dataFileIO, path, schema, catalogEnv);
            return table;
        }
        throw new Catalog.TableNotExistException(identifier);
    }

    private static int getMaxResults(Map<String, String> parameters) {
        String strMaxResults = parameters.get(MAX_RESULTS);
        Integer maxResults =
                Objects.nonNull(strMaxResults) ? Integer.parseInt(strMaxResults) : null;
        if (Objects.isNull(maxResults) || maxResults <= 0) {
            maxResults = DEFAULT_MAX_RESULTS;
        } else {
            maxResults = Math.min(maxResults, DEFAULT_MAX_RESULTS);
        }
        return maxResults;
    }

    private <T> String getNextPageTokenForEntities(List<T> entities, Integer maxResults) {
        if (entities == null
                || entities.isEmpty()
                || Objects.isNull(maxResults)
                || entities.size() < maxResults) {
            return null;
        }
        // return the last entity name
        return getPagedKey(entities.get(entities.size() - 1));
    }

    private <T> String getPagedKey(T entity) {
        if (Objects.isNull(entity)) {
            return null;
        } else if (entity instanceof String) {
            return (String) entity;
        } else if (entity instanceof GetTableResponse) {
            return ((GetTableResponse) entity).getName();
        } else if (entity instanceof GetViewResponse) {
            return ((GetViewResponse) entity).getName();
        } else if (entity instanceof Partition) {
            return ((Partition) entity).spec().toString().replace("{", "").replace("}", "");
        } else {
            return entity.toString();
        }
    }

    private Map<String, String> getParameters(String query) {
        Map<String, String> parameters =
                Arrays.stream(query.split("&"))
                        .map(pair -> pair.split("=", 2))
                        .collect(
                                Collectors.toMap(
                                        pair -> pair[0].trim(), // key
                                        pair -> RESTUtil.decodeString(pair[1].trim()), // value
                                        (existing, replacement) -> existing // handle duplicates
                                        ));
        return parameters;
    }
}
