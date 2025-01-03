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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.MockRESTMessage;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogInternalOptions;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTObjectMapper;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectPath;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.junit.Assert.assertEquals;

public class FlinkRESTCatalogTest {
    private static final String TESTING_LOG_STORE = "testing";

    private final ObjectPath path1 = new ObjectPath("db1", "t1");
    private final ObjectPath path3 = new ObjectPath("db1", "t2");

    private final ObjectPath tableInDefaultDb = new ObjectPath("default", "t1");

    private final ObjectPath tableInDefaultDb1 = new ObjectPath("default-db", "t1");
    private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
    private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

    private static final String DEFINITION_QUERY = "SELECT id, region, county FROM T";

    private static final IntervalFreshness FRESHNESS = IntervalFreshness.ofMinute("3");
    private final ObjectMapper mapper = RESTObjectMapper.create();
    private MockWebServer mockWebServer;
    private String serverUrl;
    private String warehouse;
    private Catalog catalog;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        serverUrl = mockWebServer.url("").toString();
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, serverUrl);
        String initToken = "init_token";
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        warehouse = new File(temporaryFolder.newFolder(), UUID.randomUUID().toString()).toString();
        options.set(LOG_SYSTEM_AUTO_REGISTER, true);
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        mockConfig(warehouse);
        GetDatabaseResponse response =
                MockRESTMessage.getDatabaseResponse(
                        org.apache.paimon.catalog.Catalog.DEFAULT_DATABASE);
        mockResponse(mapper.writeValueAsString(response), 200);
        catalog =
                FlinkCatalogFactory.createCatalog(
                        "test-catalog",
                        CatalogContext.create(options),
                        FlinkCatalogTest.class.getClassLoader());
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testListDatabases() throws JsonProcessingException {
        String name = MockRESTMessage.databaseName();
        ListDatabasesResponse response = MockRESTMessage.listDatabasesResponse(name);
        mockResponse(mapper.writeValueAsString(response), 200);
        List<String> result = catalog.listDatabases();
        assertEquals(response.getDatabases().size(), result.size());
        assertEquals(name, result.get(0));
    }

    private void mockConfig(String warehouseStr) {
        String mockResponse =
                String.format(
                        "{\"defaults\": {\"%s\": \"%s\", \"%s\": \"%s\"}}",
                        RESTCatalogInternalOptions.PREFIX.key(),
                        "prefix",
                        CatalogOptions.WAREHOUSE.key(),
                        warehouseStr);
        mockResponse(mockResponse, 200);
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
