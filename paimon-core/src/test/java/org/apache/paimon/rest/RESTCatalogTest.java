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
import org.apache.paimon.catalog.Database;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Test for REST Catalog. */
public class RESTCatalogTest {

    private final ObjectMapper mapper = RESTObjectMapper.create();
    private MockWebServer mockWebServer;
    private RESTCatalog restCatalog;
    private RESTCatalog mockRestCatalog;

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        String baseUrl = mockWebServer.url("").toString();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, baseUrl);
        String initToken = "init_token";
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        String mockResponse =
                String.format(
                        "{\"defaults\": {\"%s\": \"%s\"}}",
                        RESTCatalogInternalOptions.PREFIX.key(), "prefix");
        mockResponse(mockResponse, 200);
        restCatalog = new RESTCatalog(options);
        mockRestCatalog = spy(restCatalog);
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testInitFailWhenDefineWarehouse() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, "/a/b/c");
        assertThrows(IllegalArgumentException.class, () -> new RESTCatalog(options));
    }

    @Test
    public void testGetConfig() {
        String key = "a";
        String value = "b";
        String mockResponse = String.format("{\"defaults\": {\"%s\": \"%s\"}}", key, value);
        mockResponse(mockResponse, 200);
        Map<String, String> header = new HashMap<>();
        Map<String, String> response = restCatalog.fetchOptionsFromServer(header, new HashMap<>());
        assertEquals(value, response.get(key));
    }

    @Test
    public void testListDatabases() throws JsonProcessingException {
        String name = MockRESTMessage.databaseName();
        ListDatabasesResponse response = MockRESTMessage.listDatabasesResponse(name);
        mockResponse(mapper.writeValueAsString(response), 200);
        List<String> result = restCatalog.listDatabases();
        assertEquals(response.getDatabases().size(), result.size());
        assertEquals(name, result.get(0));
    }

    @Test
    public void testCreateDatabaseImpl() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseResponse response = MockRESTMessage.createDatabaseResponse(name);
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(() -> restCatalog.createDatabaseImpl(name, response.getOptions()));
    }

    @Test
    public void testGetDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        GetDatabaseResponse response = MockRESTMessage.getDatabaseResponse(name);
        mockResponse(mapper.writeValueAsString(response), 200);
        Database result = restCatalog.getDatabase(name);
        assertEquals(name, result.name());
        assertEquals(response.getOptions().size(), result.options().size());
        assertEquals(response.comment().get(), result.comment().get());
    }

    @Test
    public void testDropDatabaseImpl() throws Exception {
        String name = MockRESTMessage.databaseName();
        mockResponse("", 200);
        assertDoesNotThrow(() -> mockRestCatalog.dropDatabaseImpl(name));
        verify(mockRestCatalog, times(1)).dropDatabaseImpl(eq(name));
    }

    @Test
    public void testAlterDatabaseImpl() throws Exception {
        String name = MockRESTMessage.databaseName();
        AlterDatabaseResponse response = MockRESTMessage.alterDatabaseResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(() -> mockRestCatalog.alterDatabaseImpl(name, new ArrayList<>()));
    }

    @Test
    public void testAlterDatabaseImplWhenDatabaseNotExist() throws Exception {
        String name = MockRESTMessage.databaseName();
        ErrorResponse response = MockRESTMessage.noSuchResourceExceptionErrorResponse();
        mockResponse(mapper.writeValueAsString(response), 404);
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> mockRestCatalog.alterDatabaseImpl(name, new ArrayList<>()));
    }

    private void mockResponse(String mockResponse, int httpCode) {
        MockResponse mockResponseObj =
                new MockResponse()
                        .setResponseCode(httpCode)
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
    }
}
