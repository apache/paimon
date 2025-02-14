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
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ErrorResponseResourceType;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;

/** Mock REST server for testing. */
public class RESTCatalogServer {

    private static final String PREFIX = "paimon";
    private static final String DATABASE_URI = String.format("/v1/%s/databases", PREFIX);

    private final Catalog catalog;
    private final Dispatcher dispatcher;
    private final MockWebServer server;
    private final String authToken;

    public RESTCatalogServer(String warehouse, String initToken) {
        authToken = initToken;
        Options conf = new Options();
        conf.setString("warehouse", warehouse);
        this.catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(conf), this.getClass().getClassLoader());
        this.dispatcher = initDispatcher(catalog, authToken);
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

    public static Dispatcher initDispatcher(Catalog catalog, String authToken) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                String token = request.getHeaders().get("Authorization");
                RESTResponse response;
                try {
                    if (!("Bearer " + authToken).equals(token)) {
                        return new MockResponse().setResponseCode(401);
                    }
                    if ("/v1/config".equals(request.getPath())) {
                        return new MockResponse()
                                .setResponseCode(200)
                                .setBody(getConfigBody(catalog.warehouse()));
                    } else if (DATABASE_URI.equals(request.getPath())) {
                        return databasesApiHandler(catalog, request);
                    } else if (request.getPath().startsWith(DATABASE_URI)) {
                        String[] resources =
                                request.getPath()
                                        .substring((DATABASE_URI + "/").length())
                                        .split("/");
                        String databaseName = resources[0];
                        boolean isTables = resources.length == 2 && "tables".equals(resources[1]);
                        boolean isTable = resources.length == 3 && "tables".equals(resources[1]);
                        boolean isTableRename =
                                resources.length == 4 && "rename".equals(resources[3]);
                        boolean isPartitions =
                                resources.length == 4
                                        && "tables".equals(resources[1])
                                        && "partitions".equals(resources[3]);
                        if (isPartitions) {
                            String tableName = resources[2];
                            List<Partition> partitions =
                                    catalog.listPartitions(
                                            Identifier.create(databaseName, tableName));
                            response = new ListPartitionsResponse(partitions);
                            return mockResponse(response, 200);
                        } else if (isTableRename) {
                            return renameTableApiHandler(
                                    catalog, request, databaseName, resources[2]);
                        } else if (isTable) {
                            String tableName = resources[2];
                            return tableApiHandler(catalog, request, databaseName, tableName);
                        } else if (isTables) {
                            return tablesApiHandler(catalog, request, databaseName);
                        } else {
                            return databaseApiHandler(catalog, request, databaseName);
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
                } catch (IllegalArgumentException e) {
                    response = new ErrorResponse(null, null, e.getMessage(), 400);
                    return mockResponse(response, 400);
                } catch (Exception e) {
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

    private static MockResponse renameTableApiHandler(
            Catalog catalog, RecordedRequest request, String databaseName, String tableName)
            throws Exception {
        RenameTableRequest requestBody =
                OBJECT_MAPPER.readValue(request.getBody().readUtf8(), RenameTableRequest.class);
        catalog.renameTable(
                Identifier.create(databaseName, tableName), requestBody.getNewIdentifier(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(requestBody.getNewIdentifier());
        RESTResponse response =
                new GetTableResponse(
                        UUID.randomUUID().toString(),
                        tableName,
                        table.schema().id(),
                        table.schema().toSchema());
        return mockResponse(response, 200);
    }

    private static MockResponse databasesApiHandler(Catalog catalog, RecordedRequest request)
            throws Exception {
        RESTResponse response;
        if (request.getMethod().equals("GET")) {
            List<String> databaseNameList = catalog.listDatabases();
            response = new ListDatabasesResponse(databaseNameList);
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("POST")) {
            CreateDatabaseRequest requestBody =
                    OBJECT_MAPPER.readValue(
                            request.getBody().readUtf8(), CreateDatabaseRequest.class);
            String databaseName = requestBody.getName();
            catalog.createDatabase(databaseName, false);
            response = new CreateDatabaseResponse(databaseName, requestBody.getOptions());
            return mockResponse(response, 200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse databaseApiHandler(
            Catalog catalog, RecordedRequest request, String databaseName) throws Exception {
        RESTResponse response;
        if (request.getMethod().equals("GET")) {
            Database database = catalog.getDatabase(databaseName);
            response =
                    new GetDatabaseResponse(
                            UUID.randomUUID().toString(), database.name(), database.options());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("DELETE")) {
            catalog.dropDatabase(databaseName, false, true);
            return new MockResponse().setResponseCode(200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse tablesApiHandler(
            Catalog catalog, RecordedRequest request, String databaseName) throws Exception {
        RESTResponse response;
        if (request.getMethod().equals("POST")) {
            CreateTableRequest requestBody =
                    OBJECT_MAPPER.readValue(request.getBody().readUtf8(), CreateTableRequest.class);
            catalog.createTable(requestBody.getIdentifier(), requestBody.getSchema(), false);
            response =
                    new GetTableResponse(
                            UUID.randomUUID().toString(),
                            requestBody.getIdentifier().getTableName(),
                            1L,
                            requestBody.getSchema());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("GET")) {
            catalog.listTables(databaseName);
            response = new ListTablesResponse(catalog.listTables(databaseName));
            return mockResponse(response, 200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse tableApiHandler(
            Catalog catalog, RecordedRequest request, String databaseName, String tableName)
            throws Exception {
        RESTResponse response;
        if (request.getMethod().equals("GET")) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            response =
                    new GetTableResponse(
                            UUID.randomUUID().toString(),
                            tableName,
                            table.schema().id(),
                            table.schema().toSchema());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("POST")) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            AlterTableRequest requestBody =
                    OBJECT_MAPPER.readValue(request.getBody().readUtf8(), AlterTableRequest.class);
            catalog.alterTable(identifier, requestBody.getChanges(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            response =
                    new GetTableResponse(
                            UUID.randomUUID().toString(),
                            tableName,
                            table.schema().id(),
                            table.schema().toSchema());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("DELETE")) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            catalog.dropTable(identifier, false);
            return new MockResponse().setResponseCode(200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse mockResponse(RESTResponse response, int httpCode) {
        try {
            return new MockResponse()
                    .setResponseCode(httpCode)
                    .setBody(OBJECT_MAPPER.writeValueAsString(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getConfigBody(String warehouseStr) {
        return String.format(
                "{\"defaults\": {\"%s\": \"%s\", \"%s\": \"%s\"}}",
                RESTCatalogInternalOptions.PREFIX.key(),
                PREFIX,
                CatalogOptions.WAREHOUSE.key(),
                warehouseStr);
    }
}
