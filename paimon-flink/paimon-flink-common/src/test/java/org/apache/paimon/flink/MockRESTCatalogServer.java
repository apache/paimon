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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTObjectMapper;
import org.apache.paimon.rest.RESTResponse;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;

/** Mock REST server for testing. */
public class MockRESTCatalogServer {

    private static final ObjectMapper mapper = RESTObjectMapper.create();

    private final Catalog catalog;
    private final Dispatcher dispatcher;
    private final MockWebServer server;

    public MockRESTCatalogServer(String warehouse) {
        Options conf = new Options();
        conf.setString("warehouse", warehouse);
        conf.set(LOG_SYSTEM_AUTO_REGISTER, true);
        this.catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(conf), this.getClass().getClassLoader());
        this.dispatcher = initDispatcher(catalog);
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

    public static Dispatcher initDispatcher(Catalog catalog) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {

                RESTResponse response;
                System.out.println(request.getPath() + "  method " + request.getMethod());
                try {
                    if ("/v1/config".equals(request.getPath())) {
                        return new MockResponse()
                                .setResponseCode(200)
                                .setBody(getConfigBody(catalog.warehouse()));
                    } else if ("/v1/prefix/databases".equals(request.getPath())) {
                        if (request.getMethod().equals("GET")) {
                            List<String> databaseNameList = catalog.listDatabases();
                            response = new ListDatabasesResponse(databaseNameList);
                            return mockResponse(response, 200);
                        } else if (request.getMethod().equals("POST")) {
                            CreateDatabaseRequest requestBody =
                                    mapper.readValue(
                                            request.getBody().readUtf8(),
                                            CreateDatabaseRequest.class);
                            String databaseName = requestBody.getName();
                            catalog.createDatabase(databaseName, true);
                            response =
                                    new CreateDatabaseResponse(
                                            databaseName, requestBody.getOptions());
                            return mockResponse(response, 200);
                        }
                    } else if (request.getPath().startsWith("/v1/prefix/databases/")) {
                        String databaseName =
                                request.getPath().substring("/v1/prefix/databases/".length());
                        if (request.getMethod().equals("GET")) {
                            Database database = catalog.getDatabase(databaseName);
                            response = new GetDatabaseResponse(database.name(), database.options());
                            return mockResponse(response, 200);
                        }
                    }
                    return new MockResponse().setResponseCode(404);
                } catch (Catalog.DatabaseNotExistException e) {
                    return new MockResponse().setResponseCode(404);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
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
