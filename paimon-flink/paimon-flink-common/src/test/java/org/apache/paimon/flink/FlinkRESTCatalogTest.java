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
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.flink.FlinkCatalogOptions.LOG_SYSTEM_AUTO_REGISTER;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link FlinkCatalog} when catalog type is RESTCatalog. */
public class FlinkRESTCatalogTest {
    private final ObjectMapper mapper = RESTObjectMapper.create();
    private final ObjectPath path1 = new ObjectPath("db1", "t1");
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

    @Test
    public void testCreateTable() throws Exception {
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        CatalogTable table = this.createTable(ImmutableMap.of());
        assertDoesNotThrow(() -> catalog.createTable(path1, table, false));
    }

    private CatalogTable createTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    private ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING()),
                        Column.physical(
                                "four",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.INT()),
                                        DataTypes.FIELD(
                                                "f3",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.INT()))))),
                Collections.emptyList(),
                null);
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

    private void mockResponse(String mockContent, int httpCode) {
        MockResponse mockResponse = MockRESTMessage.mockResponse(mockContent, httpCode);
        mockWebServer.enqueue(mockResponse);
    }
}
