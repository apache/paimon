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
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
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
import static org.mockito.Mockito.when;

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
        mockResponse(mockResponse);
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
        mockResponse(mockResponse);
        Map<String, String> header = new HashMap<>();
        Map<String, String> response = restCatalog.fetchOptionsFromServer(header, new HashMap<>());
        assertEquals(value, response.get(key));
    }

    @Test
    public void testListDatabases() throws JsonProcessingException {
        String name = MockRESTMessage.databaseName();
        ListDatabasesResponse response = MockRESTMessage.listDatabasesResponse(name);
        mockResponse(mapper.writeValueAsString(response));
        List<String> result = restCatalog.listDatabases();
        assertEquals(response.getDatabases().size(), result.size());
        assertEquals(name, result.get(0));
    }

    @Test
    public void testCreateDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseResponse response = MockRESTMessage.createDatabaseResponse(name);
        mockResponse(mapper.writeValueAsString(response));
        assertDoesNotThrow(() -> restCatalog.createDatabase(name, false, response.getOptions()));
    }

    @Test
    public void testGetDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        GetDatabaseResponse response = MockRESTMessage.getDatabaseResponse(name);
        mockResponse(mapper.writeValueAsString(response));
        Database result = restCatalog.getDatabase(name);
        assertEquals(name, result.name());
        assertEquals(response.getOptions().size(), result.options().size());
        assertEquals(response.comment().get(), result.comment().get());
    }

    @Test
    public void testDropDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        mockResponse("");
        assertDoesNotThrow(() -> mockRestCatalog.dropDatabase(name, false, true));
        verify(mockRestCatalog, times(1)).dropDatabase(eq(name), eq(false), eq(true));
        verify(mockRestCatalog, times(0)).listTables(eq(name));
    }

    @Test
    public void testDropDatabaseWhenCascadeIsFalseAndNoTables() throws Exception {
        String name = MockRESTMessage.databaseName();
        boolean cascade = false;
        mockResponse("");
        when(mockRestCatalog.listTables(name)).thenReturn(new ArrayList<>());
        assertDoesNotThrow(() -> mockRestCatalog.dropDatabase(name, false, cascade));
        verify(mockRestCatalog, times(1)).dropDatabase(eq(name), eq(false), eq(cascade));
        verify(mockRestCatalog, times(1)).listTables(eq(name));
    }

    @Test
    public void testDropDatabaseWhenCascadeIsFalseAndTablesExist() throws Exception {
        String name = MockRESTMessage.databaseName();
        boolean cascade = false;
        mockResponse("");
        List<String> tables = new ArrayList<>();
        tables.add("t1");
        when(mockRestCatalog.listTables(name)).thenReturn(tables);
        assertThrows(
                Catalog.DatabaseNotEmptyException.class,
                () -> mockRestCatalog.dropDatabase(name, false, cascade));
        verify(mockRestCatalog, times(1)).dropDatabase(eq(name), eq(false), eq(cascade));
        verify(mockRestCatalog, times(1)).listTables(eq(name));
    }

    private void mockResponse(String mockResponse) {
        MockResponse mockResponseObj =
                new MockResponse()
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
    }
}
