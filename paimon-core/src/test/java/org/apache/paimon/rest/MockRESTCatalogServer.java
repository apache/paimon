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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.util.List;

/** Mock REST server for testing. */
public class MockRESTCatalogServer {

    private static final ObjectMapper mapper = RESTObjectMapper.create();

    private final Catalog catalog;
    private final Dispatcher dispatcher;
    private final MockWebServer server;
    private final String authToken;

    public MockRESTCatalogServer(String warehouse, String initToken) {
        authToken = initToken;
        Options conf = new Options();
        conf.setString("warehouse", warehouse);
        this.catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(conf), this.getClass().getClassLoader());
        this.dispatcher = initDispatcher(catalog, authToken);
        try {
            catalog.createDatabase("default", true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                System.out.println(request.getPath() + "  method " + request.getMethod());
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
                    } else if ("/v1/prefix/databases".equals(request.getPath())) {
                        return databasesApiHandler(catalog, request);
                    } else if (request.getPath().startsWith("/v1/prefix/databases/")) {
                        String[] resources =
                                request.getPath()
                                        .substring("/v1/prefix/databases/".length())
                                        .split("/");
                        String databaseName = resources[0];
                        boolean isTables = resources.length == 2 && "tables".equals(resources[1]);
                        boolean isTable = resources.length == 3 && "tables".equals(resources[1]);
                        if (isTable) {
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
                    response = new ErrorResponse("database", e.database(), e.getMessage(), 404);
                    return mockResponse(response, 404);
                } catch (Catalog.TableNotExistException e) {
                    response =
                            new ErrorResponse(
                                    "table", e.identifier().getTableName(), e.getMessage(), 404);
                    return mockResponse(response, 404);
                } catch (Catalog.ColumnNotExistException e) {
                    response = new ErrorResponse("column", e.column(), e.getMessage(), 404);
                    return mockResponse(response, 404);
                } catch (Catalog.TableAlreadyExistException e) {
                    response =
                            new ErrorResponse(
                                    "table", e.identifier().getTableName(), e.getMessage(), 409);
                    return mockResponse(response, 409);
                } catch (Catalog.ColumnAlreadyExistException e) {
                    response = new ErrorResponse("column", e.column(), e.getMessage(), 409);
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
                    }
                    return new MockResponse().setResponseCode(500);
                }
            }
        };
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
                    mapper.readValue(request.getBody().readUtf8(), CreateDatabaseRequest.class);
            String databaseName = requestBody.getName();
            catalog.createDatabase(databaseName, true);
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
            response = new GetDatabaseResponse(database.name(), database.options());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("DELETE")) {
            catalog.dropDatabase(databaseName, true, false);
            return new MockResponse().setResponseCode(200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse tablesApiHandler(
            Catalog catalog, RecordedRequest request, String databaseName) throws Exception {
        RESTResponse response;
        if (request.getMethod().equals("POST")) {
            CreateTableRequest requestBody =
                    mapper.readValue(request.getBody().readUtf8(), CreateTableRequest.class);
            catalog.createTable(requestBody.getIdentifier(), requestBody.getSchema(), false);
            response = new GetTableResponse("", 1L, requestBody.getSchema());
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
                            AbstractCatalog.newTableLocation(catalog.warehouse(), identifier)
                                    .toString(),
                            table.schema().id(),
                            table.schema().toSchema());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("POST")) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            AlterTableRequest requestBody =
                    mapper.readValue(request.getBody().readUtf8(), AlterTableRequest.class);
            catalog.alterTable(identifier, requestBody.getChanges(), true);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            response = new GetTableResponse("", table.schema().id(), table.schema().toSchema());
            return mockResponse(response, 200);
        } else if (request.getMethod().equals("DELETE")) {
            Identifier identifier = Identifier.create(databaseName, tableName);
            catalog.dropTable(identifier, true);
            return new MockResponse().setResponseCode(200);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse mockResponse(RESTResponse response, int httpCode) {
        try {
            return new MockResponse()
                    .setResponseCode(httpCode)
                    .setBody(mapper.writeValueAsString(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getConfigBody(String warehouseStr) {
        return String.format(
                "{\"defaults\": {\"%s\": \"%s\", \"%s\": \"%s\"}}",
                RESTCatalogInternalOptions.PREFIX.key(),
                "prefix",
                CatalogOptions.WAREHOUSE.key(),
                warehouseStr);
    }
}
