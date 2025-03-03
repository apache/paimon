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
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
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
import org.apache.paimon.rest.requests.ForwardBranchRequest;
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
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
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
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private final String prefix;
    private final String databaseUri;

    private final FileSystemCatalog catalog;
    private final Dispatcher dispatcher;
    private final MockWebServer server;
    private final String authToken;

    public final Map<String, Database> databaseStore = new HashMap<>();
    public final Map<String, TableMetadata> tableMetadataStore = new HashMap<>();
    public final Map<String, List<Partition>> tablePartitionsStore = new HashMap<>();
    public final Map<String, View> viewStore = new HashMap<>();
    public final Map<String, Snapshot> tableSnapshotStore = new HashMap<>();
    public final Map<String, FileIO> tableFileIOStore = new HashMap<>();
    public final ConfigResponse configResponse;
    public final String serverId;

    public RESTCatalogServer(
            String warehouse, String initToken, ConfigResponse config, String serverId) {
        this.serverId = serverId;
        this.configResponse = config;
        this.prefix =
                this.configResponse.getDefaults().get(RESTCatalogInternalOptions.PREFIX.key());
        this.databaseUri = String.format("/v1/%s/databases", prefix);
        authToken = initToken;
        Options conf = new Options();
        this.configResponse.getDefaults().forEach((k, v) -> conf.setString(k, v));
        conf.setString("warehouse", warehouse);
        CatalogContext context = CatalogContext.create(conf);
        Path warehousePath = new Path(warehouse);
        FileIO fileIO;
        try {
            fileIO = FileIO.get(warehousePath, context);
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.catalog = new FileSystemCatalog(fileIO, warehousePath, context.options());
        this.dispatcher = initDispatcher(authToken);
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

    public void setTableSnapshot(Identifier identifier, Snapshot snapshot) {
        tableSnapshotStore.put(identifier.getFullName(), snapshot);
    }

    public void setDataToken(Identifier identifier, RESTToken token) {
        DataTokenStore.putDataToken(serverId, identifier.getFullName(), token);
    }

    public void setFileIO(Identifier identifier, FileIO fileIO) {
        tableFileIOStore.put(identifier.getFullName(), fileIO);
    }

    public void removeDataToken(Identifier identifier) {
        DataTokenStore.removeDataToken(serverId, identifier.getFullName());
    }

    public RESTToken getDataToken(Identifier identifier) {
        return DataTokenStore.getDataToken(serverId, identifier.getFullName());
    }

    public Dispatcher initDispatcher(String authToken) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String token =
                        request.getHeaders().get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY);
                RESTResponse response;
                try {
                    if (!("Bearer " + authToken).equals(token)) {
                        return new MockResponse().setResponseCode(401);
                    }
                    if (request.getPath().startsWith("/v1/config")) {
                        return mockResponse(configResponse, 200);
                    } else if (databaseUri.equals(request.getPath())) {
                        return databasesApiHandler(request);
                    } else if (request.getPath().startsWith(databaseUri)) {
                        String[] resources =
                                request.getPath()
                                        .substring((databaseUri + "/").length())
                                        .split("/");
                        String databaseName = resources[0];
                        if (!databaseStore.containsKey(databaseName)) {
                            throw new Catalog.DatabaseNotExistException(databaseName);
                        }
                        boolean isViews = resources.length == 2 && "views".equals(resources[1]);
                        boolean isTables = resources.length == 2 && "tables".equals(resources[1]);
                        boolean isTableRename =
                                resources.length == 3
                                        && "tables".equals(resources[1])
                                        && "rename".equals(resources[2]);
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
                        boolean isTableCommit =
                                resources.length == 3
                                        && "tables".equals(resources[1])
                                        && "commit".equals(resources[2]);
                        boolean isTableToken =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "token".equals(resources[3]);
                        boolean isTableSnapshot =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "snapshot".equals(resources[3]);
                        boolean isPartitions =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "partitions".equals(resources[3]);

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
                                        ? Identifier.create(databaseName, resources[2])
                                        : null;
                        // validate partition
                        if (isPartitions
                                || isDropPartitions
                                || isAlterPartitions
                                || isMarkDonePartitions) {
                            String tableName = resources[2];
                            Optional<MockResponse> error =
                                    checkTablePartitioned(
                                            Identifier.create(databaseName, tableName));
                            if (error.isPresent()) {
                                return error.get();
                            }
                        }
                        if (isDropPartitions) {
                            return dropPartitionsHandle(identifier, request);
                        } else if (isAlterPartitions) {
                            return alterPartitionsHandle(identifier, request);
                        } else if (isMarkDonePartitions) {
                            MarkDonePartitionsRequest markDonePartitionsRequest =
                                    OBJECT_MAPPER.readValue(
                                            request.getBody().readUtf8(),
                                            MarkDonePartitionsRequest.class);
                            catalog.markDonePartitions(
                                    identifier, markDonePartitionsRequest.getPartitionSpecs());
                            return new MockResponse().setResponseCode(200);
                        } else if (isPartitions) {
                            return partitionsApiHandler(request, identifier);
                        } else if (isBranches) {
                            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                            BranchManager branchManager = table.branchManager();
                            switch (request.getMethod()) {
                                case "DELETE":
                                    String branch = resources[4];
                                    table.deleteBranch(branch);
                                    return new MockResponse().setResponseCode(200);
                                case "GET":
                                    List<String> branches = branchManager.branches();
                                    response = new ListBranchesResponse(branches);
                                    return mockResponse(response, 200);
                                case "POST":
                                    if (resources.length == 5) {
                                        ForwardBranchRequest requestBody =
                                                OBJECT_MAPPER.readValue(
                                                        request.getBody().readUtf8(),
                                                        ForwardBranchRequest.class);
                                        branchManager.fastForward(requestBody.branch());
                                    } else {
                                        CreateBranchRequest requestBody =
                                                OBJECT_MAPPER.readValue(
                                                        request.getBody().readUtf8(),
                                                        CreateBranchRequest.class);
                                        if (requestBody.fromTag() == null) {
                                            branchManager.createBranch(requestBody.branch());
                                        } else {
                                            branchManager.createBranch(
                                                    requestBody.branch(), requestBody.fromTag());
                                        }
                                    }
                                    return new MockResponse().setResponseCode(200);
                                default:
                                    return new MockResponse().setResponseCode(404);
                            }
                        } else if (isTableToken) {
                            return handleDataToken(identifier);
                        } else if (isTableSnapshot) {
                            return handleSnapshot(identifier);
                        } else if (isTableRename) {
                            return renameTableApiHandler(request);
                        } else if (isTableCommit) {
                            return commitTableApiHandler(request);
                        } else if (isTable) {
                            return tableApiHandler(request, identifier);
                        } else if (isTables) {
                            return tablesApiHandler(request, databaseName);
                        } else if (isViews) {
                            return viewsApiHandler(request, databaseName);
                        } else if (isViewRename) {
                            return renameViewApiHandler(request);
                        } else if (isView) {
                            return viewApiHandler(request, identifier);
                        } else {
                            return databaseApiHandler(request, databaseName);
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

    private MockResponse handleDataToken(Identifier tableIdentifier) throws Exception {
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
            DataTokenStore.putDataToken(serverId, tableIdentifier.getFullName(), dataToken);
        }
        GetTableTokenResponse getTableTokenResponse =
                new GetTableTokenResponse(dataToken.token(), dataToken.expireAtMillis());
        return new MockResponse()
                .setResponseCode(200)
                .setBody(OBJECT_MAPPER.writeValueAsString(getTableTokenResponse));
    }

    private MockResponse handleSnapshot(Identifier identifier) throws Exception {
        RESTResponse response;
        Optional<Snapshot> snapshotOptional =
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

    private MockResponse commitTableApiHandler(RecordedRequest request) throws Exception {
        CommitTableRequest requestBody =
                OBJECT_MAPPER.readValue(request.getBody().readUtf8(), CommitTableRequest.class);
        Identifier identifier = requestBody.getIdentifier();
        FileStoreTable table = getFileTable(identifier);
        RenamingSnapshotCommit commit =
                new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        String branchName = requestBody.getIdentifier().getBranchName();
        if (branchName == null) {
            branchName = "main";
        }
        boolean success =
                commit.commit(requestBody.getSnapshot(), branchName, Collections.emptyList());
        commitSnapshot(requestBody.getIdentifier(), requestBody.getSnapshot(), null);
        CommitTableResponse response = new CommitTableResponse(success);
        return mockResponse(response, 200);
    }

    private FileStoreTable getFileTable(Identifier identifier) {
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
        FileIO dataFileIO = tableFileIOStore.get(identifier.getFullName());
        FileStoreTable table = FileStoreTableFactory.create(dataFileIO, path, schema, catalogEnv);
        return table;
    }

    private MockResponse databasesApiHandler(RecordedRequest request) throws Exception {
        RESTResponse response;
        switch (request.getMethod()) {
            case "GET":
                List<String> databaseNameList = new ArrayList<>(databaseStore.keySet());
                response = new ListDatabasesResponse(databaseNameList);
                return mockResponse(response, 200);
            case "POST":
                CreateDatabaseRequest requestBody =
                        OBJECT_MAPPER.readValue(
                                request.getBody().readUtf8(), CreateDatabaseRequest.class);
                String databaseName = requestBody.getName();
                catalog.createDatabase(databaseName, false);
                databaseStore.put(
                        databaseName, Database.of(databaseName, requestBody.getOptions(), null));
                response = new CreateDatabaseResponse(databaseName, requestBody.getOptions());
                return mockResponse(response, 200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse databaseApiHandler(RecordedRequest request, String databaseName)
            throws Exception {
        RESTResponse response;
        Database database;
        if (databaseStore.containsKey(databaseName)) {
            switch (request.getMethod()) {
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
                            OBJECT_MAPPER.readValue(
                                    request.getBody().readUtf8(), AlterDatabaseRequest.class);
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

    private MockResponse tablesApiHandler(RecordedRequest request, String databaseName)
            throws Exception {
        RESTResponse response;
        if (databaseStore.containsKey(databaseName)) {
            switch (request.getMethod()) {
                case "GET":
                    List<String> tables = new ArrayList<>();
                    for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
                        Identifier identifier = Identifier.fromString(entry.getKey());
                        if (databaseName.equals(identifier.getDatabaseName())) {
                            tables.add(identifier.getTableName());
                        }
                    }
                    response = new ListTablesResponse(tables);
                    return mockResponse(response, 200);
                case "POST":
                    CreateTableRequest requestBody =
                            OBJECT_MAPPER.readValue(
                                    request.getBody().readUtf8(), CreateTableRequest.class);
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

    private boolean isFormatTable(Schema schema) {
        return Options.fromMap(schema.options()).get(TYPE) == FORMAT_TABLE;
    }

    private MockResponse tableApiHandler(RecordedRequest request, Identifier identifier)
            throws Exception {
        RESTResponse response;
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            switch (request.getMethod()) {
                case "GET":
                    TableMetadata tableMetadata = tableMetadataStore.get(identifier.getFullName());
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
                            OBJECT_MAPPER.readValue(
                                    request.getBody().readUtf8(), AlterTableRequest.class);
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
        } else {
            throw new Catalog.TableNotExistException(identifier);
        }
    }

    private MockResponse renameTableApiHandler(RecordedRequest request) throws Exception {
        RenameTableRequest requestBody =
                OBJECT_MAPPER.readValue(request.getBody().readUtf8(), RenameTableRequest.class);
        Identifier fromTable = requestBody.getSource();
        Identifier toTable = requestBody.getDestination();
        if (tableMetadataStore.containsKey(fromTable.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(fromTable.getFullName());
            if (!isFormatTable(tableMetadata.schema().toSchema())) {
                catalog.renameTable(requestBody.getSource(), requestBody.getDestination(), false);
            }
            tableMetadataStore.remove(fromTable.getFullName());
            tableMetadataStore.put(toTable.getFullName(), tableMetadata);
        } else {
            throw new Catalog.TableNotExistException(fromTable);
        }
        return new MockResponse().setResponseCode(200);
    }

    private MockResponse partitionsApiHandler(RecordedRequest request, Identifier tableIdentifier)
            throws Exception {
        RESTResponse response;
        switch (request.getMethod()) {
            case "GET":
                List<Partition> partitions =
                        tablePartitionsStore.get(tableIdentifier.getFullName());
                response = new ListPartitionsResponse(partitions);
                return mockResponse(response, 200);
            case "POST":
                CreatePartitionsRequest requestBody =
                        OBJECT_MAPPER.readValue(
                                request.getBody().readUtf8(), CreatePartitionsRequest.class);
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

    private MockResponse viewsApiHandler(RecordedRequest request, String databaseName)
            throws Exception {
        RESTResponse response;
        switch (request.getMethod()) {
            case "GET":
                List<String> views =
                        viewStore.keySet().stream()
                                .map(Identifier::fromString)
                                .filter(
                                        identifier ->
                                                identifier.getDatabaseName().equals(databaseName))
                                .map(Identifier::getTableName)
                                .collect(Collectors.toList());
                response = new ListViewsResponse(views);
                return mockResponse(response, 200);
            case "POST":
                CreateViewRequest requestBody =
                        OBJECT_MAPPER.readValue(
                                request.getBody().readUtf8(), CreateViewRequest.class);
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

    private MockResponse viewApiHandler(RecordedRequest request, Identifier identifier)
            throws Exception {
        RESTResponse response;
        if (viewStore.containsKey(identifier.getFullName())) {
            switch (request.getMethod()) {
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

    private MockResponse renameViewApiHandler(RecordedRequest request) throws Exception {
        RenameTableRequest requestBody =
                OBJECT_MAPPER.readValue(request.getBody().readUtf8(), RenameTableRequest.class);
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
            tableSnapshotStore.put(identifier.getFullName(), snapshot);
            return success;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MockResponse dropPartitionsHandle(Identifier identifier, RecordedRequest request)
            throws Catalog.TableNotExistException, JsonProcessingException {
        DropPartitionsRequest dropPartitionsRequest =
                OBJECT_MAPPER.readValue(request.getBody().readUtf8(), DropPartitionsRequest.class);
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

    private MockResponse alterPartitionsHandle(Identifier identifier, RecordedRequest request)
            throws Catalog.TableNotExistException, JsonProcessingException {
        if (tableMetadataStore.containsKey(identifier.getFullName())) {
            AlterPartitionsRequest alterPartitionsRequest =
                    OBJECT_MAPPER.readValue(
                            request.getBody().readUtf8(), AlterPartitionsRequest.class);
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
        options.put(PATH.key(), path.toString());
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
}
