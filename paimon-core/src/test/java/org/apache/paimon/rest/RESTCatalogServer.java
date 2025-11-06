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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.catalog.RenamingSnapshotCommit;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.fs.local.LocalFileIOLoader;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.function.FunctionDefinition;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.RESTAuthParameter;
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
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
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
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsGloballyResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.LazyField;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.org.apache.commons.lang3.StringUtils;

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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.CoreOptions.SNAPSHOT_CLEAN_EMPTY_DIRECTORIES;
import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.FORMAT_TABLE;
import static org.apache.paimon.TableType.OBJECT_TABLE;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTApi.DATABASE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.FUNCTION_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.MAX_RESULTS;
import static org.apache.paimon.rest.RESTApi.PAGE_TOKEN;
import static org.apache.paimon.rest.RESTApi.PARTITION_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.TABLE_NAME_PATTERN;
import static org.apache.paimon.rest.RESTApi.TABLE_TYPE;
import static org.apache.paimon.rest.RESTApi.VIEW_NAME_PATTERN;
import static org.apache.paimon.rest.ResourcePaths.FUNCTIONS;
import static org.apache.paimon.rest.ResourcePaths.FUNCTION_DETAILS;
import static org.apache.paimon.rest.ResourcePaths.TABLE_DETAILS;
import static org.apache.paimon.rest.ResourcePaths.VIEWS;
import static org.apache.paimon.rest.ResourcePaths.VIEW_DETAILS;

/** Mock REST server for testing. */
public class RESTCatalogServer {

    private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);

    public static final int DEFAULT_MAX_RESULTS = 100;
    public static final String AUTHORIZATION_HEADER_KEY = "Authorization";

    private final String databaseUri;

    private final CatalogContext catalogContext;
    private final RESTFileSystemCatalog catalog;
    private final MockWebServer server;

    private final Map<String, Database> databaseStore = new HashMap<>();
    private final Map<String, TableMetadata> tableMetadataStore = new HashMap<>();
    private final Map<String, List<Partition>> tablePartitionsStore = new HashMap<>();
    private final Map<String, View> viewStore = new HashMap<>();
    private final Map<String, TableSnapshot> tableLatestSnapshotStore = new HashMap<>();
    private final Map<String, TableSnapshot> tableWithSnapshotId2SnapshotStore = new HashMap<>();
    private final List<String> noPermissionDatabases = new ArrayList<>();
    private final List<String> noPermissionTables = new ArrayList<>();
    private final Map<String, Function> functionStore = new HashMap<>();
    private final Map<String, List<String>> columnAuthHandler = new HashMap<>();
    public final ConfigResponse configResponse;
    public final String warehouse;

    private final ResourcePaths resourcePaths;

    private final List<Map<String, String>> receivedHeaders = new ArrayList<>();

    public RESTCatalogServer(
            String dataPath, AuthProvider authProvider, ConfigResponse config, String warehouse) {
        this.warehouse = warehouse;
        this.configResponse = config;
        String prefix =
                this.configResponse.getDefaults().get(RESTCatalogInternalOptions.PREFIX.key());
        this.resourcePaths = new ResourcePaths(prefix);
        this.databaseUri = resourcePaths.databases();
        Options conf = new Options();
        this.configResponse.getDefaults().forEach(conf::setString);
        conf.setString(WAREHOUSE.key(), dataPath);
        this.catalogContext = CatalogContext.create(conf);
        Path warehousePath = new Path(dataPath);
        FileIO fileIO;
        try {
            fileIO = new LocalFileIO();
            fileIO.checkOrMkdirs(warehousePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.catalog = new RESTFileSystemCatalog(fileIO, warehousePath, catalogContext);
        Dispatcher dispatcher = initDispatcher(authProvider);
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
        TableSnapshot tableSnapshot =
                new TableSnapshot(
                        snapshot, recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
        tableLatestSnapshotStore.put(identifier.getFullName(), tableSnapshot);
        tableWithSnapshotId2SnapshotStore.put(
                geTableFullNameWithSnapshotId(identifier, snapshot.id()), tableSnapshot);
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

    public void addTableColumnAuth(Identifier identifier, List<String> select) {
        columnAuthHandler.put(identifier.getFullName(), select);
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
                    receivedHeaders.add(new HashMap<>(headers));
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
                                    .mergeAuthHeader(headers, restAuthParameter)
                                    .get(AUTHORIZATION_HEADER_KEY);
                    if (!authToken.equals(token)) {
                        return new MockResponse().setResponseCode(401);
                    }
                    if (request.getPath().startsWith(resourcePaths.config())
                            && request.getRequestUrl()
                                    .queryParameter(WAREHOUSE.key())
                                    .equals(warehouse)) {
                        return mockResponse(configResponse, 200);
                    } else if (databaseUri.equals(request.getPath())
                            || request.getPath().contains(databaseUri + "?")) {
                        return databasesApiHandler(restAuthParameter.method(), data, parameters);
                    } else if (resourcePaths.renameTable().equals(request.getPath())) {
                        return renameTableHandle(restAuthParameter.data());
                    } else if (resourcePaths.renameView().equals(request.getPath())) {
                        return renameViewHandle(restAuthParameter.data());
                    } else if (StringUtils.startsWith(request.getPath(), resourcePaths.tables())) {
                        return tablesHandle(parameters);
                    } else if (StringUtils.startsWith(request.getPath(), resourcePaths.views())) {
                        return viewsHandle(parameters);
                    } else if (StringUtils.startsWith(
                            request.getPath(), resourcePaths.functions())) {
                        return functionsHandle(parameters);
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
                        boolean isFunctions =
                                resources.length == 2 && resources[1].startsWith(FUNCTIONS);
                        boolean isFunction =
                                resources.length == 3 && resources[1].startsWith(FUNCTIONS);
                        boolean isFunctionsDetails =
                                resources.length == 2 && resources[1].startsWith(FUNCTION_DETAILS);
                        boolean isViews = resources.length == 2 && resources[1].startsWith(VIEWS);
                        boolean isViewsDetails =
                                resources.length == 2 && resources[1].startsWith(VIEW_DETAILS);
                        boolean isTables =
                                resources.length == 2
                                        && resources[1].startsWith(ResourcePaths.TABLES);
                        boolean isTableDetails =
                                resources.length == 2 && resources[1].startsWith(TABLE_DETAILS);
                        boolean isView =
                                resources.length == 3
                                        && "views".equals(resources[1])
                                        && !"rename".equals(resources[2]);
                        boolean isTable =
                                resources.length == 3
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && !"rename".equals(resources[2])
                                        && !"commit".equals(resources[2]);
                        boolean isTableToken =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "token".equals(resources[3]);
                        boolean isTableSnapshot =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "snapshot".equals(resources[3]);
                        boolean isListSnapshots =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && ResourcePaths.SNAPSHOTS.equals(resources[3]);
                        boolean isLoadSnapshot =
                                resources.length == 5
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && ResourcePaths.SNAPSHOTS.equals(resources[3]);
                        boolean isTableAuth =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "auth".equals(resources[3]);
                        boolean isCommitSnapshot =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "commit".equals(resources[3]);
                        boolean isRollbackTable =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && ResourcePaths.ROLLBACK.equals(resources[3]);
                        boolean isPartitions =
                                resources.length == 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && resources[3].startsWith("partitions");

                        boolean isMarkDonePartitions =
                                resources.length == 5
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "partitions".equals(resources[3])
                                        && "mark".equals(resources[4]);

                        boolean isBranches =
                                resources.length >= 4
                                        && ResourcePaths.TABLES.equals(resources[1])
                                        && "branches".equals(resources[3]);
                        Identifier identifier =
                                resources.length >= 3
                                                && !"rename".equals(resources[2])
                                                && !"commit".equals(resources[2])
                                        ? Identifier.create(
                                                databaseName, RESTUtil.decodeString(resources[2]))
                                        : null;
                        if (identifier != null && ResourcePaths.TABLES.equals(resources[1])) {
                            if (!identifier.isSystemTable()
                                    && !tableMetadataStore.containsKey(identifier.getFullName())) {
                                throw new Catalog.TableNotExistException(identifier);
                            }
                            if (noPermissionTables.contains(identifier.getFullName())) {
                                throw new Catalog.TableNoPermissionException(identifier);
                            }
                        }
                        // validate partition
                        if (isPartitions || isMarkDonePartitions) {
                            String tableName = RESTUtil.decodeString(resources[2]);
                            Optional<MockResponse> error =
                                    checkTablePartitioned(
                                            Identifier.create(databaseName, tableName));
                            if (error.isPresent()) {
                                return error.get();
                            }
                        }
                        if (isMarkDonePartitions) {
                            MarkDonePartitionsRequest markDonePartitionsRequest =
                                    RESTApi.fromJson(data, MarkDonePartitionsRequest.class);
                            catalog.markDonePartitions(
                                    identifier, markDonePartitionsRequest.getPartitionSpecs());
                            return new MockResponse().setResponseCode(200);
                        } else if (isPartitions) {
                            return partitionsApiHandle(
                                    restAuthParameter.method(), parameters, identifier);
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
                        } else if (isListSnapshots) {
                            return listSnapshots(identifier);
                        } else if (isLoadSnapshot) {
                            return loadSnapshot(identifier, resources[4]);
                        } else if (isTableAuth) {
                            return authTable(identifier, restAuthParameter.data());
                        } else if (isCommitSnapshot) {
                            return commitTableHandle(identifier, restAuthParameter.data());
                        } else if (isRollbackTable) {
                            RollbackTableRequest requestBody =
                                    RESTApi.fromJson(data, RollbackTableRequest.class);
                            if (noPermissionTables.contains(identifier.getFullName())) {
                                throw new Catalog.TableNoPermissionException(identifier);
                            }
                            if (!tableMetadataStore.containsKey(identifier.getFullName())) {
                                throw new Catalog.TableNotExistException(identifier);
                            }
                            if (requestBody.getInstant() instanceof Instant.SnapshotInstant) {
                                long snapshotId =
                                        ((Instant.SnapshotInstant) requestBody.getInstant())
                                                .getSnapshotId();
                                return rollbackTableByIdHandle(identifier, snapshotId);
                            } else if (requestBody.getInstant() instanceof Instant.TagInstant) {
                                String tagName =
                                        ((Instant.TagInstant) requestBody.getInstant())
                                                .getTagName();
                                return rollbackTableByTagNameHandle(identifier, tagName);
                            }
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
                        } else if (isFunctions) {
                            return functionsApiHandler(
                                    databaseName, restAuthParameter.method(), data, parameters);
                        } else if (isFunction) {
                            return functionApiHandler(
                                    identifier, restAuthParameter.method(), data, parameters);
                        } else if (isFunctionsDetails) {
                            return functionDetailsHandle(
                                    restAuthParameter.method(), databaseName, parameters);
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
                            return viewHandle(
                                    restAuthParameter.method(),
                                    identifier,
                                    restAuthParameter.data());
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
                                    ErrorResponse.RESOURCE_TYPE_DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.TableNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.ColumnNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_COLUMN,
                                    e.column(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.DatabaseNoPermissionException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    403);
                    return mockResponse(response, 403);
                } catch (Catalog.TableNoPermissionException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    403);
                    return mockResponse(response, 403);
                } catch (Catalog.DatabaseAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DATABASE,
                                    e.database(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.TableAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_TABLE,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.ColumnAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_COLUMN,
                                    e.column(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.FunctionAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DEFINITION,
                                    e.identifier().getObjectName(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.DefinitionAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DEFINITION,
                                    e.name(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.ViewNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_VIEW,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.DialectNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DIALECT,
                                    e.dialect(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.FunctionNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_FUNCTION,
                                    e.identifier().getObjectName(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.DefinitionNotExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DEFINITION,
                                    e.name(),
                                    e.getMessage(),
                                    404);
                    return mockResponse(response, 404);
                } catch (Catalog.ViewAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_VIEW,
                                    e.identifier().getTableName(),
                                    e.getMessage(),
                                    409);
                    return mockResponse(response, 409);
                } catch (Catalog.DialectAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    ErrorResponse.RESOURCE_TYPE_DIALECT,
                                    e.dialect(),
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
                    } else if (e instanceof UnsupportedOperationException
                            || e.getCause() instanceof UnsupportedOperationException) {
                        response = new ErrorResponse(null, null, e.getMessage(), 501);
                        return mockResponse(response, 501);
                    } else if (e instanceof IllegalStateException
                            || e.getCause() instanceof IllegalStateException) {
                        response = new ErrorResponse(null, null, e.getMessage(), 500);
                        return mockResponse(response, 500);
                    }
                    return new MockResponse()
                            .setResponseCode(500)
                            .setBody(e.getCause().getMessage());
                }
            }
        };
    }

    private MockResponse getDataTokenHandle(Identifier tableIdentifier) throws Exception {
        RESTToken dataToken = getDataToken(tableIdentifier);
        if (dataToken == null) {
            long currentTimeMillis = System.currentTimeMillis() + 4000_000L;
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
                .setBody(RESTApi.toJson(getTableTokenResponse));
    }

    private MockResponse snapshotHandle(Identifier identifier) throws Exception {
        if (!tableMetadataStore.containsKey(identifier.getFullName())) {
            throw new Catalog.TableNotExistException(identifier);
        }
        TableMetadata tableMetadata = tableMetadataStore.get(identifier.getFullName());
        if (tableMetadata.isExternal()) {
            ErrorResponse response =
                    new ErrorResponse(
                            ErrorResponse.RESOURCE_TYPE_TABLE,
                            identifier.getFullName(),
                            "external paimon table does not support get table snapshot in rest server",
                            501);
            return mockResponse(response, 404);
        }
        RESTResponse response;
        Optional<TableSnapshot> snapshotOptional =
                Optional.ofNullable(tableLatestSnapshotStore.get(identifier.getFullName()));
        if (!snapshotOptional.isPresent()) {
            response =
                    new ErrorResponse(
                            ErrorResponse.RESOURCE_TYPE_SNAPSHOT,
                            identifier.getDatabaseName(),
                            "No Snapshot",
                            404);
            return mockResponse(response, 404);
        }
        GetTableSnapshotResponse getTableSnapshotResponse =
                new GetTableSnapshotResponse(snapshotOptional.get());
        return new MockResponse()
                .setResponseCode(200)
                .setBody(RESTApi.toJson(getTableSnapshotResponse));
    }

    private MockResponse listSnapshots(Identifier identifier) throws Exception {
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        List<Snapshot> snapshotList = new ArrayList<>();
        while (snapshots.hasNext()) {
            snapshotList.add(snapshots.next());
        }
        ListSnapshotsResponse response = new ListSnapshotsResponse(snapshotList, null);
        return new MockResponse().setResponseCode(200).setBody(RESTApi.toJson(response));
    }

    private MockResponse loadSnapshot(Identifier identifier, String version) throws Exception {

        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = null;
        try {
            if (version.equals("EARLIEST")) {
                snapshot = snapshotManager.earliestSnapshot();
            } else if (version.equals("LATEST")) {
                snapshot = snapshotManager.latestSnapshot();
            } else {
                try {
                    long snapshotId = Long.parseLong(version);
                    snapshot = snapshotManager.tryGetSnapshot(snapshotId);
                } catch (NumberFormatException e) {
                    Optional<Tag> tag = table.tagManager().get(version);
                    if (tag.isPresent()) {
                        snapshot = tag.get().trimToSnapshot();
                    }
                }
            }
        } catch (Exception ignored) {
        }

        if (snapshot == null) {
            RESTResponse response =
                    new ErrorResponse(
                            ErrorResponse.RESOURCE_TYPE_SNAPSHOT,
                            identifier.getDatabaseName(),
                            "No Snapshot",
                            404);
            return mockResponse(response, 404);
        }
        GetVersionSnapshotResponse response = new GetVersionSnapshotResponse(snapshot);
        return new MockResponse().setResponseCode(200).setBody(RESTApi.toJson(response));
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
                        new ErrorResponse(ErrorResponse.RESOURCE_TYPE_TABLE, null, "", 404), 404));
    }

    private MockResponse authTable(Identifier identifier, String data) throws Exception {
        AuthTableQueryRequest requestBody = RESTApi.fromJson(data, AuthTableQueryRequest.class);
        if (noPermissionTables.contains(identifier.getFullName())) {
            throw new Catalog.TableNoPermissionException(identifier);
        }

        TableMetadata metadata = tableMetadataStore.get(identifier.getFullName());
        if (metadata == null) {
            throw new Catalog.TableNotExistException(identifier);
        }
        List<String> columnAuth = columnAuthHandler.get(identifier.getFullName());
        if (columnAuth != null) {
            List<String> select = requestBody.select();
            if (select == null) {
                select = metadata.schema().fieldNames();
            }
            select.forEach(
                    column -> {
                        if (!columnAuth.contains(column)) {
                            throw new Catalog.TableNoPermissionException(identifier);
                        }
                    });
        }
        AuthTableQueryResponse response = new AuthTableQueryResponse(Collections.emptyList());
        return mockResponse(response, 200);
    }

    private MockResponse commitTableHandle(Identifier identifier, String data) throws Exception {
        CommitTableRequest requestBody = RESTApi.fromJson(data, CommitTableRequest.class);
        if (noPermissionTables.contains(identifier.getFullName())) {
            throw new Catalog.TableNoPermissionException(identifier);
        }
        if (!tableMetadataStore.containsKey(identifier.getFullName())) {
            throw new Catalog.TableNotExistException(identifier);
        }
        return commitSnapshot(
                identifier,
                requestBody.getTableId(),
                requestBody.getSnapshot(),
                requestBody.getStatistics());
    }

    private MockResponse rollbackTableByIdHandle(Identifier identifier, long snapshotId)
            throws Exception {
        FileStoreTable table = getFileTable(identifier);
        String identifierWithSnapshotId = geTableFullNameWithSnapshotId(identifier, snapshotId);
        if (tableWithSnapshotId2SnapshotStore.containsKey(identifierWithSnapshotId)) {
            table =
                    table.copy(
                            Collections.singletonMap(
                                    SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(), "true"));
            long latestSnapshotId = table.snapshotManager().latestSnapshotId();
            table.rollbackTo(snapshotId);
            cleanSnapshot(identifier, snapshotId, latestSnapshotId);
            tableLatestSnapshotStore.put(
                    identifier.getFullName(),
                    tableWithSnapshotId2SnapshotStore.get(identifierWithSnapshotId));
            return new MockResponse().setResponseCode(200);
        }
        return mockResponse(
                new ErrorResponse(ErrorResponse.RESOURCE_TYPE_SNAPSHOT, "" + snapshotId, "", 404),
                404);
    }

    private MockResponse rollbackTableByTagNameHandle(Identifier identifier, String tagName)
            throws Exception {
        FileStoreTable table = getFileTable(identifier);
        boolean isExist = table.tagManager().tagExists(tagName);
        if (isExist) {
            Snapshot snapshot = table.tagManager().getOrThrow(tagName).trimToSnapshot();
            String identifierWithSnapshotId =
                    geTableFullNameWithSnapshotId(identifier, snapshot.id());
            if (tableWithSnapshotId2SnapshotStore.containsKey(identifierWithSnapshotId)) {
                table =
                        table.copy(
                                Collections.singletonMap(
                                        SNAPSHOT_CLEAN_EMPTY_DIRECTORIES.key(), "true"));
                long latestSnapshotId = table.snapshotManager().latestSnapshotId();
                table.rollbackTo(tagName);
                cleanSnapshot(identifier, snapshot.id(), latestSnapshotId);
                tableLatestSnapshotStore.put(
                        identifier.getFullName(),
                        tableWithSnapshotId2SnapshotStore.get(identifierWithSnapshotId));
                return new MockResponse().setResponseCode(200);
            }
        }
        return mockResponse(
                new ErrorResponse(ErrorResponse.RESOURCE_TYPE_TAG, "" + tagName, "", 404), 404);
    }

    private void cleanSnapshot(Identifier identifier, Long snapshotId, Long latestSnapshotId)
            throws IOException {
        if (latestSnapshotId > snapshotId) {
            for (long i = snapshotId + 1; i < latestSnapshotId + 1; i++) {
                tableWithSnapshotId2SnapshotStore.remove(
                        geTableFullNameWithSnapshotId(identifier, i));
            }
        }
    }

    private MockResponse functionsApiHandler(
            String databaseName, String method, String data, Map<String, String> parameters)
            throws Exception {
        switch (method) {
            case "GET":
                String namePattern = parameters.get(FUNCTION_NAME_PATTERN);
                List<String> functions =
                        (new ArrayList<>(functionStore.keySet()))
                                .stream()
                                        .map(n -> Identifier.fromString(n))
                                        .filter(
                                                identifier ->
                                                        identifier
                                                                        .getDatabaseName()
                                                                        .equals(databaseName)
                                                                && (Objects.isNull(namePattern)
                                                                        || matchNamePattern(
                                                                                identifier
                                                                                        .getObjectName(),
                                                                                namePattern)))
                                        .map(i -> i.getObjectName())
                                        .collect(Collectors.toList());
                return generateFinalListFunctionsResponse(parameters, functions);
            case "POST":
                CreateFunctionRequest requestBody =
                        RESTApi.fromJson(data, CreateFunctionRequest.class);
                String functionName = requestBody.name();
                Identifier identity = Identifier.create(databaseName, functionName);
                if (!functionStore.containsKey(identity.getFullName())) {
                    Function function =
                            new FunctionImpl(
                                    identity,
                                    requestBody.inputParams(),
                                    requestBody.returnParams(),
                                    requestBody.isDeterministic(),
                                    requestBody.definitions(),
                                    requestBody.comment(),
                                    requestBody.options());
                    functionStore.put(identity.getFullName(), function);
                    return new MockResponse().setResponseCode(200);
                } else {
                    throw new Catalog.FunctionAlreadyExistException(
                            Identifier.create(databaseName, functionName));
                }
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse functionApiHandler(
            Identifier identifier, String method, String data, Map<String, String> parameters)
            throws Exception {
        if (!functionStore.containsKey(identifier.getFullName())) {
            throw new Catalog.FunctionNotExistException(identifier);
        }
        Function function = functionStore.get(identifier.getFullName());
        switch (method) {
            case "DELETE":
                functionStore.remove(identifier.getFullName());
                break;
            case "GET":
                GetFunctionResponse response = toGetFunctionResponse(function);
                return mockResponse(response, 200);
            case "POST":
                AlterFunctionRequest requestBody =
                        RESTApi.fromJson(data, AlterFunctionRequest.class);
                HashMap<String, FunctionDefinition> newDefinitions =
                        new HashMap<>(function.definitions());
                Map<String, String> newOptions =
                        function.options() != null
                                ? new HashMap<>(function.options())
                                : new HashMap<>();
                String newComment = function.comment();
                for (FunctionChange functionChange : requestBody.changes()) {
                    if (functionChange instanceof FunctionChange.SetFunctionOption) {
                        FunctionChange.SetFunctionOption setFunctionOption =
                                (FunctionChange.SetFunctionOption) functionChange;
                        newOptions.put(setFunctionOption.key(), setFunctionOption.value());
                    } else if (functionChange instanceof FunctionChange.RemoveFunctionOption) {
                        FunctionChange.RemoveFunctionOption removeFunctionOption =
                                (FunctionChange.RemoveFunctionOption) functionChange;
                        newOptions.remove(removeFunctionOption.key());
                    } else if (functionChange instanceof FunctionChange.UpdateFunctionComment) {
                        FunctionChange.UpdateFunctionComment updateFunctionComment =
                                (FunctionChange.UpdateFunctionComment) functionChange;
                        newComment = updateFunctionComment.comment();
                    } else if (functionChange instanceof FunctionChange.AddDefinition) {
                        FunctionChange.AddDefinition addDefinition =
                                (FunctionChange.AddDefinition) functionChange;
                        if (function.definition(addDefinition.name()) != null) {
                            throw new Catalog.DefinitionAlreadyExistException(
                                    identifier, addDefinition.name());
                        }
                        newDefinitions.put(addDefinition.name(), addDefinition.definition());
                    } else if (functionChange instanceof FunctionChange.UpdateDefinition) {
                        FunctionChange.UpdateDefinition updateDefinition =
                                (FunctionChange.UpdateDefinition) functionChange;
                        if (function.definition(updateDefinition.name()) != null) {
                            newDefinitions.put(
                                    updateDefinition.name(), updateDefinition.definition());
                        } else {
                            throw new Catalog.DefinitionNotExistException(
                                    identifier, updateDefinition.name());
                        }
                    } else if (functionChange instanceof FunctionChange.DropDefinition) {
                        FunctionChange.DropDefinition dropDefinition =
                                (FunctionChange.DropDefinition) functionChange;
                        if (function.definitions().containsKey(dropDefinition.name())) {
                            newDefinitions.remove(dropDefinition.name());
                        } else {
                            throw new Catalog.DefinitionNotExistException(
                                    identifier, dropDefinition.name());
                        }
                    }
                }
                function =
                        new FunctionImpl(
                                identifier,
                                function.inputParams().orElse(null),
                                function.returnParams().orElse(null),
                                function.isDeterministic(),
                                newDefinitions,
                                newComment,
                                newOptions);
                functionStore.put(identifier.getFullName(), function);
                break;
            default:
                return new MockResponse().setResponseCode(404);
        }
        return new MockResponse().setResponseCode(200);
    }

    private MockResponse functionDetailsHandle(
            String method, String databaseName, Map<String, String> parameters) {
        RESTResponse response;
        if ("GET".equals(method)) {

            List<GetFunctionResponse> functionsDetails =
                    listFunctionDetails(databaseName, parameters);
            if (!functionsDetails.isEmpty()) {

                int maxResults;
                try {
                    maxResults = getMaxResults(parameters);
                } catch (NumberFormatException e) {
                    return handleInvalidMaxResults(parameters);
                }
                String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

                PagedList<GetFunctionResponse> pagedFunctionDetails =
                        buildPagedEntities(functionsDetails, maxResults, pageToken);
                response =
                        new ListFunctionDetailsResponse(
                                pagedFunctionDetails.getElements(),
                                pagedFunctionDetails.getNextPageToken());
            } else {
                response = new ListFunctionDetailsResponse(Collections.emptyList(), null);
            }
            return mockResponse(response, 200);
        } else {
            return new MockResponse().setResponseCode(404);
        }
    }

    private List<GetFunctionResponse> listFunctionDetails(
            String databaseName, Map<String, String> parameters) {
        String namePattern = parameters.get(FUNCTION_NAME_PATTERN);
        return functionStore.keySet().stream()
                .map(Identifier::fromString)
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .filter(
                        identifier ->
                                (Objects.isNull(namePattern)
                                        || matchNamePattern(
                                                identifier.getObjectName(), namePattern)))
                .map(
                        identifier -> {
                            return toGetFunctionResponse(
                                    functionStore.get(identifier.getFullName()));
                        })
                .collect(Collectors.toList());
    }

    private GetFunctionResponse toGetFunctionResponse(Function function) {
        return new GetFunctionResponse(
                UUID.randomUUID().toString(),
                function.name(),
                function.inputParams().orElse(null),
                function.returnParams().orElse(null),
                function.isDeterministic(),
                function.definitions(),
                function.comment(),
                function.options(),
                "owner",
                1L,
                "owner",
                1L,
                "owner");
    }

    private MockResponse databasesApiHandler(
            String method, String data, Map<String, String> parameters) throws Exception {
        switch (method) {
            case "GET":
                String databaseNamePattern = parameters.get(DATABASE_NAME_PATTERN);
                List<String> databases =
                        new ArrayList<>(databaseStore.keySet())
                                .stream()
                                        .filter(
                                                databaseName ->
                                                        Objects.isNull(databaseNamePattern)
                                                                || matchNamePattern(
                                                                        databaseName,
                                                                        databaseNamePattern))
                                        .collect(Collectors.toList());
                return generateFinalListDatabasesResponse(parameters, databases);
            case "POST":
                CreateDatabaseRequest requestBody =
                        RESTApi.fromJson(data, CreateDatabaseRequest.class);
                String databaseName = requestBody.getName();
                if (noPermissionDatabases.contains(databaseName)) {
                    throw new Catalog.DatabaseNoPermissionException(databaseName);
                }
                catalog.createDatabase(databaseName, false);
                databaseStore.put(
                        databaseName, Database.of(databaseName, requestBody.getOptions(), null));
                return new MockResponse().setResponseCode(200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse generateFinalListFunctionsResponse(
            Map<String, String> parameters, List<String> functions) {
        RESTResponse response;
        if (!functions.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (Exception e) {
                LOG.error(
                        "parse maxResults {} to int failed",
                        parameters.getOrDefault(MAX_RESULTS, null));
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_TABLE,
                                null,
                                "invalid input queryParameter maxResults"
                                        + parameters.get(MAX_RESULTS),
                                400),
                        400);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);
            PagedList<String> pagedDbs = buildPagedEntities(functions, maxResults, pageToken);
            response =
                    new ListFunctionsResponse(pagedDbs.getElements(), pagedDbs.getNextPageToken());
        } else {
            response = new ListFunctionsResponse(new ArrayList<>(), null);
        }
        return mockResponse(response, 200);
    }

    private MockResponse generateFinalListDatabasesResponse(
            Map<String, String> parameters, List<String> databases) {
        RESTResponse response;
        if (!databases.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
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

    private <T> PagedList<T> buildPagedEntities(
            List<T> entities, int maxResults, String pageToken) {
        return buildPagedEntities(entities, maxResults, pageToken, false);
    }

    private <T> PagedList<T> buildPagedEntities(
            List<T> entities, int maxResults, String pageToken, boolean desc) {
        Comparator<Object> comparator = this::compareTo;
        if (desc) {
            comparator = comparator.reversed();
        }
        List<T> sortedEntities = entities.stream().sorted(comparator).collect(Collectors.toList());
        List<T> pagedEntities = new ArrayList<>();
        for (T sortedEntity : sortedEntities) {
            if (pagedEntities.size() < maxResults) {
                if (pageToken == null) {
                    pagedEntities.add(sortedEntity);
                } else if (comparator.compare(sortedEntity, pageToken) > 0) {
                    pagedEntities.add(sortedEntity);
                }
            } else {
                break;
            }
        }
        if (maxResults > sortedEntities.size() && pageToken == null) {
            return new PagedList<>(pagedEntities, null);
        } else {
            String nextPageToken = getNextPageTokenForEntities(pagedEntities, maxResults);
            return new PagedList<>(pagedEntities, nextPageToken);
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
                                    "/tmp",
                                    database.options(),
                                    "owner",
                                    1L,
                                    "created",
                                    1L,
                                    "updated");
                    return mockResponse(response, 200);
                case "DELETE":
                    catalog.dropDatabase(databaseName, false, true);
                    databaseStore.remove(databaseName);
                    return new MockResponse().setResponseCode(200);
                case "POST":
                    AlterDatabaseRequest requestBody =
                            RESTApi.fromJson(data, AlterDatabaseRequest.class);
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
                    List<String> tables = listTables(databaseName, parameters);
                    return generateFinalListTablesResponse(parameters, tables);
                case "POST":
                    CreateTableRequest requestBody =
                            RESTApi.fromJson(data, CreateTableRequest.class);
                    Identifier identifier = requestBody.getIdentifier();
                    Schema schema = requestBody.getSchema();
                    TableMetadata tableMetadata;
                    if (isFormatTable(schema)) {
                        tableMetadata = createFormatTable(identifier, schema);
                    } else if (isObjectTable(schema)) {
                        tableMetadata = createObjectTable(identifier, schema);
                    } else {
                        catalog.createTable(identifier, schema, false);
                        boolean isExternal =
                                schema.options() != null
                                        && schema.options().containsKey(PATH.key());
                        tableMetadata =
                                createTableMetadata(
                                        requestBody.getIdentifier(),
                                        0L,
                                        requestBody.getSchema(),
                                        UUID.randomUUID().toString(),
                                        isExternal);
                    }
                    tableMetadataStore.put(
                            requestBody.getIdentifier().getFullName(), tableMetadata);
                    return new MockResponse().setResponseCode(200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
        return mockResponse(
                new ErrorResponse(ErrorResponse.RESOURCE_TYPE_DATABASE, null, "", 404), 404);
    }

    private List<String> listTables(String databaseName, Map<String, String> parameters) {
        String tableNamePattern = parameters.get(TABLE_NAME_PATTERN);
        String tableType = parameters.get(TABLE_TYPE);
        List<String> tables = new ArrayList<>();
        for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())
                    && (Objects.isNull(tableNamePattern)
                            || matchNamePattern(identifier.getTableName(), tableNamePattern))) {

                // Check table type filter if specified
                if (StringUtils.isNotEmpty(tableType)) {
                    String actualTableType = entry.getValue().schema().options().get(TYPE.key());
                    if (StringUtils.equals(tableType, "table")) {
                        // When filtering by "table" type, return tables with null or "table" type
                        if (actualTableType != null && !"table".equals(actualTableType)) {
                            continue;
                        }
                    } else {
                        // For other table types, return exact matches
                        if (!StringUtils.equals(tableType, actualTableType)) {
                            continue;
                        }
                    }
                }

                tables.add(identifier.getTableName());
            }
        }
        return tables;
    }

    private boolean matchNamePattern(String name, String pattern) {
        RESTUtil.validatePrefixSqlPattern(pattern);
        String regex = sqlPatternToRegex(pattern);
        return Pattern.compile(regex).matcher(name).matches();
    }

    private MockResponse generateFinalListTablesResponse(
            Map<String, String> parameters, List<String> tables) {
        RESTResponse response;
        if (!tables.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
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
        List<GetTableResponse> tableDetails = listTableDetails(databaseName, parameters);
        if (!tableDetails.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
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

    private List<GetTableResponse> listTableDetails(
            String databaseName, Map<String, String> parameters) {
        String tableNamePattern = parameters.get(TABLE_NAME_PATTERN);
        String tableType = parameters.get(TABLE_TYPE);
        List<GetTableResponse> tableDetails = new ArrayList<>();
        for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if (databaseName.equals(identifier.getDatabaseName())
                    && (Objects.isNull(tableNamePattern)
                            || matchNamePattern(identifier.getTableName(), tableNamePattern))) {

                // Check table type filter if specified
                if (StringUtils.isNotEmpty(tableType)) {
                    String actualTableType = entry.getValue().schema().options().get(TYPE.key());
                    if (StringUtils.equals(tableType, "table")) {
                        // When filtering by "table" type, return tables with null or "table" type
                        if (actualTableType != null && !"table".equals(actualTableType)) {
                            continue;
                        }
                    } else {
                        // For other table types, return exact matches
                        if (!StringUtils.equals(tableType, actualTableType)) {
                            continue;
                        }
                    }
                }

                GetTableResponse getTableResponse =
                        new GetTableResponse(
                                entry.getValue().uuid(),
                                identifier.getTableName(),
                                entry.getValue().schema().options().get(PATH.key()),
                                entry.getValue().isExternal(),
                                entry.getValue().schema().id(),
                                entry.getValue().schema().toSchema(),
                                "owner",
                                1L,
                                "created",
                                1L,
                                "updated");
                tableDetails.add(getTableResponse);
            }
        }
        return tableDetails;
    }

    private MockResponse tablesHandle(Map<String, String> parameters) {
        RESTResponse response;
        List<Identifier> tables = listTables(parameters);
        if (!tables.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
            }
            String pageToken = parameters.get(PAGE_TOKEN);
            PagedList<Identifier> pagedTables = buildPagedEntities(tables, maxResults, pageToken);
            response =
                    new ListTablesGloballyResponse(
                            pagedTables.getElements(), pagedTables.getNextPageToken());
        } else {
            response = new ListTablesResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private List<Identifier> listTables(Map<String, String> parameters) {
        String tableNamePattern = parameters.get(TABLE_NAME_PATTERN);
        String databaseNamePattern = parameters.get(DATABASE_NAME_PATTERN);
        List<Identifier> tables = new ArrayList<>();
        for (Map.Entry<String, TableMetadata> entry : tableMetadataStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if ((Objects.isNull(databaseNamePattern))
                    || matchNamePattern(identifier.getDatabaseName(), databaseNamePattern)
                            && (Objects.isNull(tableNamePattern)
                                    || matchNamePattern(
                                            identifier.getTableName(), tableNamePattern))) {
                tables.add(identifier);
            }
        }
        return tables;
    }

    private boolean isFormatTable(Schema schema) {
        return Options.fromMap(schema.options()).get(TYPE) == FORMAT_TABLE;
    }

    private boolean isObjectTable(Schema schema) {
        return Options.fromMap(schema.options()).get(TYPE) == OBJECT_TABLE;
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
                if (identifier.isSystemTable()) {
                    TableSchema schema = catalog.loadTableSchema(identifier);
                    tableMetadata =
                            createTableMetadata(
                                    identifier, schema.id(), schema.toSchema(), null, false);
                } else {
                    tableMetadata = tableMetadataStore.get(identifier.getFullName());
                }
                Schema schema = tableMetadata.schema().toSchema();
                String path = schema.options().remove(PATH.key());
                response =
                        new GetTableResponse(
                                tableMetadata.uuid(),
                                identifier.getObjectName(),
                                path,
                                tableMetadata.isExternal(),
                                tableMetadata.schema().id(),
                                schema,
                                "owner",
                                1L,
                                "created",
                                1L,
                                "updated");
                return mockResponse(response, 200);
            case "POST":
                AlterTableRequest requestBody = RESTApi.fromJson(data, AlterTableRequest.class);
                alterTableImpl(identifier, requestBody.getChanges());
                return new MockResponse().setResponseCode(200);
            case "DELETE":
                if (!tableMetadataStore.containsKey(identifier.getFullName())) {
                    return new MockResponse().setResponseCode(404);
                }
                tableMetadata = tableMetadataStore.get(identifier.getFullName());
                if (!tableMetadata.isExternal()) {
                    try {
                        catalog.dropTable(identifier, false);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
                tableMetadataStore.remove(identifier.getFullName());
                tableLatestSnapshotStore.remove(identifier.getFullName());
                tablePartitionsStore.remove(identifier.getFullName());
                return new MockResponse().setResponseCode(200);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    private MockResponse renameTableHandle(String data) throws Exception {
        RenameTableRequest requestBody = RESTApi.fromJson(data, RenameTableRequest.class);
        Identifier fromTable = requestBody.getSource();
        Identifier toTable = requestBody.getDestination();
        if (noPermissionTables.contains(fromTable.getFullName())) {
            throw new Catalog.TableNoPermissionException(fromTable);
        } else if (tableMetadataStore.containsKey(fromTable.getFullName())) {
            TableMetadata tableMetadata = tableMetadataStore.get(fromTable.getFullName());
            if (!isFormatTable(tableMetadata.schema().toSchema()) && !tableMetadata.isExternal()) {
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
            String method, Map<String, String> parameters, Identifier tableIdentifier) {
        String partitionNamePattern = parameters.get(PARTITION_NAME_PATTERN);
        switch (method) {
            case "GET":
                List<Partition> partitions = new ArrayList<>();
                for (Map.Entry<String, List<Partition>> entry : tablePartitionsStore.entrySet()) {
                    String objectName = Identifier.fromString(entry.getKey()).getObjectName();
                    if (objectName.equals(tableIdentifier.getObjectName())) {
                        partitions.addAll(
                                entry.getValue().stream()
                                        .filter(
                                                partition ->
                                                        Objects.isNull(partitionNamePattern)
                                                                || matchNamePattern(
                                                                        getPagedKey(partition),
                                                                        partitionNamePattern))
                                        .collect(Collectors.toList()));
                    }
                }
                return generateFinalListPartitionsResponse(parameters, partitions);
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
                        branchIdentifier =
                                new Identifier(
                                        identifier.getDatabaseName(),
                                        identifier.getTableName(),
                                        branch);
                        tableLatestSnapshotStore.put(
                                identifier.getFullName(),
                                tableLatestSnapshotStore.get(branchIdentifier.getFullName()));
                    } else {
                        CreateBranchRequest requestBody =
                                RESTApi.fromJson(data, CreateBranchRequest.class);
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
                        tableLatestSnapshotStore.put(
                                branchIdentifier.getFullName(),
                                tableLatestSnapshotStore.get(identifier.getFullName()));
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
                                ErrorResponse.RESOURCE_TYPE_TAG, fromTag, e.getMessage(), 404);
                return mockResponse(response, 404);
            }
            if (e.getMessage().contains("Branch name")
                    && e.getMessage().contains("already exists")) {
                response =
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_BRANCH, branch, e.getMessage(), 409);
                return mockResponse(response, 409);
            }
            if (e.getMessage().contains("Branch name")
                    && e.getMessage().contains("doesn't exist")) {
                response =
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_BRANCH, branch, e.getMessage(), 404);
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
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
            }
            String pageToken = parameters.getOrDefault(PAGE_TOKEN, null);

            PagedList<Partition> pagedPartitions =
                    buildPagedEntities(partitions, maxResults, pageToken, true);
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
                List<String> views = listViews(databaseName, parameters);
                return generateFinalListViewsResponse(parameters, views);
            case "POST":
                CreateViewRequest requestBody = RESTApi.fromJson(data, CreateViewRequest.class);
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

    private List<String> listViews(String databaseName, Map<String, String> parameters) {
        String viewNamePattern = parameters.get(VIEW_NAME_PATTERN);
        return viewStore.keySet().stream()
                .map(Identifier::fromString)
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .map(Identifier::getTableName)
                .filter(
                        viewName ->
                                (Objects.isNull(viewNamePattern)
                                        || matchNamePattern(viewName, viewNamePattern)))
                .collect(Collectors.toList());
    }

    private MockResponse generateFinalListViewsResponse(
            Map<String, String> parameters, List<String> views) {
        RESTResponse response;
        if (!views.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
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

            List<GetViewResponse> viewDetails = listViewDetails(databaseName, parameters);
            if (!viewDetails.isEmpty()) {

                int maxResults;
                try {
                    maxResults = getMaxResults(parameters);
                } catch (NumberFormatException e) {
                    return handleInvalidMaxResults(parameters);
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

    private List<GetViewResponse> listViewDetails(
            String databaseName, Map<String, String> parameters) {
        String viewNamePattern = parameters.get(VIEW_NAME_PATTERN);
        return viewStore.keySet().stream()
                .map(Identifier::fromString)
                .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
                .filter(
                        identifier ->
                                (Objects.isNull(viewNamePattern)
                                        || matchNamePattern(
                                                identifier.getTableName(), viewNamePattern)))
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
                            return new GetViewResponse(
                                    "id",
                                    identifier.getTableName(),
                                    schema,
                                    "owner",
                                    1L,
                                    "created",
                                    1L,
                                    "updated");
                        })
                .collect(Collectors.toList());
    }

    private MockResponse functionsHandle(Map<String, String> parameters) {
        RESTResponse response;
        List<Identifier> functions = listFunctions(parameters);
        if (!functions.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
            }
            String pageToken = parameters.get(PAGE_TOKEN);
            PagedList<Identifier> pagedFunctions =
                    buildPagedEntities(functions, maxResults, pageToken);
            response =
                    new ListFunctionsGloballyResponse(
                            pagedFunctions.getElements(), pagedFunctions.getNextPageToken());
        } else {
            response = new ListFunctionsGloballyResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private List<Identifier> listFunctions(Map<String, String> parameters) {
        String namePattern = parameters.get(FUNCTION_NAME_PATTERN);
        String databaseNamePattern = parameters.get(DATABASE_NAME_PATTERN);
        List<Identifier> fullFunctions = new ArrayList<>();
        for (Map.Entry<String, Function> entry : functionStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if ((Objects.isNull(databaseNamePattern))
                    || matchNamePattern(identifier.getDatabaseName(), databaseNamePattern)
                            && (Objects.isNull(namePattern)
                                    || matchNamePattern(identifier.getObjectName(), namePattern))) {
                fullFunctions.add(identifier);
            }
        }
        return fullFunctions;
    }

    private MockResponse viewsHandle(Map<String, String> parameters) {
        RESTResponse response;
        List<Identifier> views = listViews(parameters);
        if (!views.isEmpty()) {
            int maxResults;
            try {
                maxResults = getMaxResults(parameters);
            } catch (NumberFormatException e) {
                return handleInvalidMaxResults(parameters);
            }
            String pageToken = parameters.get(PAGE_TOKEN);
            PagedList<Identifier> pagedViews = buildPagedEntities(views, maxResults, pageToken);
            response =
                    new ListViewsGloballyResponse(
                            pagedViews.getElements(), pagedViews.getNextPageToken());
        } else {
            response = new ListViewsResponse(Collections.emptyList(), null);
        }
        return mockResponse(response, 200);
    }

    private List<Identifier> listViews(Map<String, String> parameters) {
        String viewNamePattern = parameters.get(VIEW_NAME_PATTERN);
        String databaseNamePattern = parameters.get(DATABASE_NAME_PATTERN);
        List<Identifier> fullViews = new ArrayList<>();
        for (Map.Entry<String, View> entry : viewStore.entrySet()) {
            Identifier identifier = Identifier.fromString(entry.getKey());
            if ((Objects.isNull(databaseNamePattern))
                    || matchNamePattern(identifier.getDatabaseName(), databaseNamePattern)
                            && (Objects.isNull(viewNamePattern)
                                    || matchNamePattern(
                                            identifier.getTableName(), viewNamePattern))) {
                fullViews.add(identifier);
            }
        }
        return fullViews;
    }

    private MockResponse viewHandle(String method, Identifier identifier, String requestData)
            throws Exception {
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
                        response =
                                new GetViewResponse(
                                        "id",
                                        identifier.getTableName(),
                                        schema,
                                        "owner",
                                        1L,
                                        "created",
                                        1L,
                                        "updated");
                        return mockResponse(response, 200);
                    }
                    throw new Catalog.ViewNotExistException(identifier);
                case "DELETE":
                    viewStore.remove(identifier.getFullName());
                    return new MockResponse().setResponseCode(200);
                case "POST":
                    if (viewStore.containsKey(identifier.getFullName())) {
                        AlterViewRequest request =
                                RESTApi.fromJson(requestData, AlterViewRequest.class);
                        ViewImpl view = (ViewImpl) viewStore.get(identifier.getFullName());
                        HashMap<String, String> newDialects = new HashMap<>(view.dialects());
                        Map<String, String> newOptions = new HashMap<>(view.options());
                        String newComment = view.comment().orElse(null);
                        for (ViewChange viewChange : request.viewChanges()) {
                            if (viewChange instanceof ViewChange.SetViewOption) {
                                ViewChange.SetViewOption setViewOption =
                                        (ViewChange.SetViewOption) viewChange;
                                newOptions.put(setViewOption.key(), setViewOption.value());

                            } else if (viewChange instanceof ViewChange.RemoveViewOption) {
                                ViewChange.RemoveViewOption removeViewOption =
                                        (ViewChange.RemoveViewOption) viewChange;
                                newOptions.remove(removeViewOption.key());
                            } else if (viewChange instanceof ViewChange.UpdateViewComment) {
                                ViewChange.UpdateViewComment updateViewComment =
                                        (ViewChange.UpdateViewComment) viewChange;
                                newComment = updateViewComment.comment();
                            } else if (viewChange instanceof ViewChange.AddDialect) {
                                ViewChange.AddDialect addDialect =
                                        (ViewChange.AddDialect) viewChange;
                                if (view.dialects().containsKey(addDialect.dialect())) {

                                    throw new Catalog.DialectAlreadyExistException(
                                            identifier, addDialect.dialect());
                                } else {
                                    newDialects.put(addDialect.dialect(), addDialect.query());
                                }
                            } else if (viewChange instanceof ViewChange.UpdateDialect) {
                                ViewChange.UpdateDialect updateDialect =
                                        (ViewChange.UpdateDialect) viewChange;
                                if (view.dialects().containsKey(updateDialect.dialect())) {
                                    newDialects.put(updateDialect.dialect(), updateDialect.query());
                                } else {
                                    throw new Catalog.DialectNotExistException(
                                            identifier, updateDialect.dialect());
                                }
                            } else if (viewChange instanceof ViewChange.DropDialect) {
                                ViewChange.DropDialect dropDialect =
                                        (ViewChange.DropDialect) viewChange;
                                if (view.dialects().containsKey(dropDialect.dialect())) {
                                    newDialects.remove(dropDialect.dialect());
                                } else {
                                    throw new Catalog.DialectNotExistException(
                                            identifier, dropDialect.dialect());
                                }
                            }
                        }
                        view =
                                new ViewImpl(
                                        identifier,
                                        view.rowType().getFields(),
                                        view.query(),
                                        newDialects,
                                        newComment,
                                        newOptions);
                        viewStore.put(identifier.getFullName(), view);
                        return new MockResponse().setResponseCode(200);
                    } else {
                        throw new Catalog.ViewNotExistException(identifier);
                    }
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
        throw new Catalog.ViewNotExistException(identifier);
    }

    private MockResponse renameViewHandle(String data) throws Exception {
        RenameTableRequest requestBody = RESTApi.fromJson(data, RenameTableRequest.class);
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
            try {
                TableSchema schema = tableMetadata.schema();
                if (isFormatTable(schema.toSchema())) {
                    TableSchema newSchema =
                            SchemaManager.generateTableSchema(
                                    schema,
                                    changes,
                                    new LazyField<>(() -> false),
                                    new LazyField<>(() -> identifier));
                    TableMetadata newTableMetadata =
                            createTableMetadata(
                                    identifier,
                                    newSchema.id(),
                                    newSchema.toSchema(),
                                    tableMetadata.uuid(),
                                    tableMetadata.isExternal());
                    tableMetadataStore.put(identifier.getFullName(), newTableMetadata);
                    return;
                }
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

    private String geTableFullNameWithSnapshotId(Identifier identifier, long snapshotId) {
        return String.format("%s-%d", identifier.getFullName(), snapshotId);
    }

    public static volatile boolean commitSuccessThrowException = false;

    private MockResponse commitSnapshot(
            Identifier identifier,
            String tableId,
            Snapshot snapshot,
            List<PartitionStatistics> statistics)
            throws Catalog.TableNotExistException {
        if (!tableMetadataStore.containsKey(identifier.getFullName())) {
            throw new Catalog.TableNotExistException(identifier);
        }
        boolean isExternal = tableMetadataStore.get(identifier.getFullName()).isExternal();
        if (isExternal) {
            new ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_TABLE,
                    identifier.getFullName(),
                    "external paimon table does not support commit in rest server",
                    501);
        }
        FileStoreTable table = getFileTable(identifier);
        if (!tableId.equals(table.catalogEnvironment().uuid())) {
            throw new Catalog.TableNotExistException(identifier);
        }
        RenamingSnapshotCommit commit =
                new RenamingSnapshotCommit(table.snapshotManager(), Lock.empty());
        String branchName = identifier.getBranchName();
        if (branchName == null) {
            branchName = "main";
        }
        TableSnapshot tableSnapshot;
        try {
            boolean success = commit.commit(snapshot, branchName, Collections.emptyList());
            if (!success) {
                return mockResponse(new CommitTableResponse(success), 200);
            }

            // update snapshot and stats
            tableSnapshot =
                    tableLatestSnapshotStore.compute(
                            identifier.getFullName(),
                            (k, old) -> {
                                long recordCount = 0;
                                long fileSizeInBytes = 0;
                                long fileCount = 0;
                                long lastFileCreationTime = 0;
                                if (statistics != null) {
                                    for (PartitionStatistics stats : statistics) {
                                        recordCount += stats.recordCount();
                                        fileSizeInBytes += stats.fileSizeInBytes();
                                        fileCount += stats.fileCount();
                                        if (stats.lastFileCreationTime() > lastFileCreationTime) {
                                            lastFileCreationTime = stats.lastFileCreationTime();
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
                                        fileSizeInBytes,
                                        fileCount,
                                        lastFileCreationTime);
                            });
            tableWithSnapshotId2SnapshotStore.put(
                    geTableFullNameWithSnapshotId(identifier, snapshot.id()), tableSnapshot);
            // upsert partitions stats
            if (!tablePartitionsStore.containsKey(identifier.getFullName())) {
                if (statistics != null) {
                    List<Partition> newPartitions =
                            statistics.stream()
                                    .map(
                                            stats ->
                                                    new Partition(
                                                            stats.spec(),
                                                            stats.recordCount(),
                                                            stats.fileSizeInBytes(),
                                                            stats.fileCount(),
                                                            stats.lastFileCreationTime(),
                                                            false))
                                    .collect(Collectors.toList());
                    tablePartitionsStore.put(identifier.getFullName(), newPartitions);
                }
            } else {
                tablePartitionsStore.compute(
                        identifier.getFullName(),
                        (k, oldPartitions) -> {
                            if (oldPartitions == null || statistics == null) {
                                return oldPartitions;
                            }
                            Map<Map<String, String>, PartitionStatistics> partitionStatisticsMap =
                                    statistics.stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            PartitionStatistics::spec,
                                                            y -> y,
                                                            (a, b) -> a));
                            List<Partition> updatedPartitions =
                                    oldPartitions.stream()
                                            .map(
                                                    oldPartition -> {
                                                        PartitionStatistics stats =
                                                                partitionStatisticsMap.get(
                                                                        oldPartition.spec());
                                                        if (stats == null) {
                                                            return oldPartition; // 如果没有新的统计信息，保持原样
                                                        }
                                                        return new Partition(
                                                                oldPartition.spec(),
                                                                oldPartition.recordCount()
                                                                        + stats.recordCount(),
                                                                oldPartition.fileSizeInBytes()
                                                                        + stats.fileSizeInBytes(),
                                                                oldPartition.fileCount()
                                                                        + stats.fileCount(),
                                                                Math.max(
                                                                        oldPartition
                                                                                .lastFileCreationTime(),
                                                                        stats
                                                                                .lastFileCreationTime()),
                                                                oldPartition.done());
                                                    })
                                            .collect(Collectors.toList());
                            return updatedPartitions;
                        });
            }
            // clean up partitions
            tablePartitionsStore
                    .entrySet()
                    .removeIf(
                            entry -> {
                                List<Partition> partitions = entry.getValue();
                                if (partitions == null) {
                                    return true;
                                }
                                partitions.removeIf(
                                        partition ->
                                                partition.fileSizeInBytes() <= 0
                                                        && partition.fileCount() <= 0
                                                        && partition.recordCount() <= 0);
                                return partitions.isEmpty();
                            });
            if (commitSuccessThrowException) {
                commitSuccessThrowException = false;
                return mockResponse(
                        new ErrorResponse(
                                ErrorResponse.RESOURCE_TYPE_TABLE, null, "Service Failure", 500),
                        500);
            }
            CommitTableResponse response = new CommitTableResponse(success);
            return mockResponse(response, 200);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private MockResponse mockResponse(RESTResponse response, int httpCode) {
        try {
            return new MockResponse()
                    .setResponseCode(httpCode)
                    .setBody(RESTApi.toJson(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private TableMetadata createTableMetadata(
            Identifier identifier, long schemaId, Schema schema, String uuid, boolean isExternal) {
        Map<String, String> options = new HashMap<>(schema.options());
        Path path =
                isExternal && Objects.nonNull(schema.options().get(PATH.key()))
                        ? new Path(schema.options().get(PATH.key()))
                        : catalog.getTableLocation(identifier);
        String restPath = path.toString();
        if (this.configResponse
                .getDefaults()
                .getOrDefault(RESTTokenFileIO.DATA_TOKEN_ENABLED.key(), "false")
                .equals("true")) {
            restPath =
                    path.toString()
                            .replaceFirst(LocalFileIOLoader.SCHEME, RESTFileIOTestLoader.SCHEME);
        }
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
        return new TableMetadata(tableSchema, isExternal, uuid);
    }

    private TableMetadata createFormatTable(Identifier identifier, Schema schema) {
        return createTableMetadata(identifier, 1L, schema, UUID.randomUUID().toString(), true);
    }

    private TableMetadata createObjectTable(Identifier identifier, Schema schema) {
        Schema newSchema =
                new Schema(
                        ObjectTable.SCHEMA.getFields(),
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        schema.options(),
                        schema.comment());
        return createTableMetadata(identifier, 1L, newSchema, UUID.randomUUID().toString(), false);
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
                        catalogContext,
                        false);
        Path path = new Path(schema.options().get(PATH.key()));
        FileIO dataFileIO = catalog.fileIO();
        FileStoreTable table = FileStoreTableFactory.create(dataFileIO, path, schema, catalogEnv);
        return table;
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
            return PartitionUtils.buildPartitionName(((Partition) entity).spec());
        } else if (entity instanceof GetFunctionResponse) {
            GetFunctionResponse functionResponse = (GetFunctionResponse) entity;
            return functionResponse.name();
        } else if (entity instanceof Identifier) {
            Identifier identifier = (Identifier) entity;
            return identifier.getFullName();
        } else {
            return entity.toString();
        }
    }

    private Map<String, String> getParameters(String query) {
        return Arrays.stream(query.split("&"))
                .map(pair -> pair.split("=", 2))
                .collect(
                        Collectors.toMap(
                                pair -> pair[0].trim(), // key
                                pair -> RESTUtil.decodeString(pair[1].trim()), // value
                                (existing, replacement) -> existing // handle duplicates
                                ));
    }

    private String sqlPatternToRegex(String pattern) {
        StringBuilder regex = new StringBuilder();
        boolean escaped = false;

        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);

            if (escaped) {
                regex.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '%') {
                regex.append(".*");
            } else {
                regex.append(c);
            }
        }
        return "^" + regex + "$";
    }

    private MockResponse handleInvalidMaxResults(Map<String, String> parameters) {
        String maxResults = parameters.get(MAX_RESULTS);
        LOG.error("Invalid maxResults value: {}", maxResults);
        return mockResponse(
                new ErrorResponse(
                        ErrorResponse.RESOURCE_TYPE_TABLE,
                        null,
                        String.format(
                                "Invalid input for queryParameter maxResults: %s", maxResults),
                        400),
                400);
    }

    public List<Map<String, String>> getReceivedHeaders() {
        return receivedHeaders;
    }

    public void clearReceivedHeaders() {
        receivedHeaders.clear();
    }
}
