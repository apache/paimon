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
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Partition;
import org.apache.paimon.table.Table;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for REST Catalog. */
public class RESTCatalogTest {

    private final ObjectMapper mapper = RESTObjectMapper.create();
    private MockWebServer mockWebServer;
    private RESTCatalog restCatalog;
    private String warehouseStr;
    private String serverUrl;
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        serverUrl = mockWebServer.url("").toString();
        Options options = mockInitOptions();
        warehouseStr = folder.getRoot().getPath();
        mockConfig(warehouseStr);
        restCatalog = new RESTCatalog(CatalogContext.create(options));
    }

    @After
    public void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    public void testInitFailWhenDefineWarehouse() {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouseStr);
        assertThrows(
                IllegalArgumentException.class,
                () -> new RESTCatalog(CatalogContext.create(options)));
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
    public void testCreateDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseResponse response = MockRESTMessage.createDatabaseResponse(name);
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(() -> restCatalog.createDatabase(name, false, response.getOptions()));
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
    public void testDropDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        mockResponse("", 200);
        assertDoesNotThrow(() -> restCatalog.dropDatabase(name, false, true));
    }

    @Test
    public void testDropDatabaseWhenNoExistAndIgnoreIfNotExistsIsFalse() throws Exception {
        String name = MockRESTMessage.databaseName();
        ErrorResponse response = MockRESTMessage.noSuchResourceExceptionErrorResponse();
        mockResponse(mapper.writeValueAsString(response), 404);
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> restCatalog.dropDatabase(name, false, true));
    }

    @Test
    public void testDropDatabaseWhenNoExistAndIgnoreIfNotExistsIsTrue() throws Exception {
        String name = MockRESTMessage.databaseName();
        ErrorResponse response = MockRESTMessage.noSuchResourceExceptionErrorResponse();
        mockResponse(mapper.writeValueAsString(response), 404);
        assertDoesNotThrow(() -> restCatalog.dropDatabase(name, true, true));
    }

    @Test
    public void testDropDatabaseWhenCascadeIsFalseAndNoTables() throws Exception {
        String name = MockRESTMessage.databaseName();
        boolean cascade = false;
        ListTablesResponse response = MockRESTMessage.listTablesEmptyResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        mockResponse("", 200);
        assertDoesNotThrow(() -> restCatalog.dropDatabase(name, false, cascade));
    }

    @Test
    public void testDropDatabaseWhenCascadeIsFalseAndTablesExist() throws Exception {
        String name = MockRESTMessage.databaseName();
        boolean cascade = false;
        ListTablesResponse response = MockRESTMessage.listTablesResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertThrows(
                Catalog.DatabaseNotEmptyException.class,
                () -> restCatalog.dropDatabase(name, false, cascade));
    }

    @Test
    public void testAlterDatabase() throws Exception {
        String name = MockRESTMessage.databaseName();
        AlterDatabaseResponse response = MockRESTMessage.alterDatabaseResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(() -> restCatalog.alterDatabase(name, new ArrayList<>(), true));
    }

    @Test
    public void testAlterDatabaseWhenDatabaseNotExistAndIgnoreIfNotExistsIsFalse()
            throws Exception {
        String name = MockRESTMessage.databaseName();
        ErrorResponse response = MockRESTMessage.noSuchResourceExceptionErrorResponse();
        mockResponse(mapper.writeValueAsString(response), 404);
        assertThrows(
                Catalog.DatabaseNotExistException.class,
                () -> restCatalog.alterDatabase(name, new ArrayList<>(), false));
    }

    @Test
    public void testAlterDatabaseWhenDatabaseNotExistAndIgnoreIfNotExistsIsTrue() throws Exception {
        String name = MockRESTMessage.databaseName();
        ErrorResponse response = MockRESTMessage.noSuchResourceExceptionErrorResponse();
        mockResponse(mapper.writeValueAsString(response), 404);
        assertDoesNotThrow(() -> restCatalog.alterDatabase(name, new ArrayList<>(), true));
    }

    @Test
    public void testListTables() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        ListTablesResponse response = MockRESTMessage.listTablesResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        List<String> result = restCatalog.listTables(databaseName);
        assertEquals(response.getTables().size(), result.size());
    }

    @Test
    public void testGetTable() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        Table result = restCatalog.getTable(Identifier.create(databaseName, "table"));
        assertEquals(response.getSchema().options().size() + 1, result.options().size());
    }

    @Test
    public void testCreateTable() throws Exception {
        CreateTableRequest request = MockRESTMessage.createTableRequest("table");
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(
                () -> restCatalog.createTable(request.getIdentifier(), request.getSchema(), false));
    }

    @Test
    public void testCreateTableWhenTableAlreadyExistAndIgnoreIfExistsIsFalse() throws Exception {
        CreateTableRequest request = MockRESTMessage.createTableRequest("table");
        mockResponse("", 409);
        assertThrows(
                Catalog.TableAlreadyExistException.class,
                () -> restCatalog.createTable(request.getIdentifier(), request.getSchema(), false));
    }

    @Test
    public void testRenameTable() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        String fromTableName = "fromTable";
        String toTableName = "toTable";
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(
                () ->
                        restCatalog.renameTable(
                                Identifier.create(databaseName, fromTableName),
                                Identifier.create(databaseName, toTableName),
                                true));
    }

    @Test
    public void testRenameTableWhenTableNotExistAndIgnoreIfNotExistsIsFalse() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        String fromTableName = "fromTable";
        String toTableName = "toTable";
        mockResponse("", 404);
        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.renameTable(
                                Identifier.create(databaseName, fromTableName),
                                Identifier.create(databaseName, toTableName),
                                false));
    }

    @Test
    public void testRenameTableWhenToTableAlreadyExist() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        String fromTableName = "fromTable";
        String toTableName = "toTable";
        mockResponse("", 409);
        assertThrows(
                Catalog.TableAlreadyExistException.class,
                () ->
                        restCatalog.renameTable(
                                Identifier.create(databaseName, fromTableName),
                                Identifier.create(databaseName, toTableName),
                                false));
    }

    @Test
    public void testAlterTable() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        List<SchemaChange> changes = MockRESTMessage.getChanges();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        assertDoesNotThrow(
                () -> restCatalog.alterTable(Identifier.create(databaseName, "t1"), changes, true));
    }

    @Test
    public void testAlterTableWhenTableNotExistAndIgnoreIfNotExistsIsFalse() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        List<SchemaChange> changes = MockRESTMessage.getChanges();
        mockResponse("", 404);
        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.alterTable(
                                Identifier.create(databaseName, "t1"), changes, false));
    }

    @Test
    public void testDropTable() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        String tableName = "table";
        mockResponse("", 200);
        assertDoesNotThrow(
                () -> restCatalog.dropTable(Identifier.create(databaseName, tableName), true));
    }

    @Test
    public void testDropTableWhenTableNotExistAndIgnoreIfNotExistsIsFalse() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        String tableName = "table";
        mockResponse("", 404);
        assertThrows(
                Catalog.TableNotExistException.class,
                () -> restCatalog.dropTable(Identifier.create(databaseName, tableName), false));
    }

    @Test
    public void testCreatePartition() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        mockResponse(mapper.writeValueAsString(response), 200);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("p1", "v1");
        mockResponse(mapper.writeValueAsString(MockRESTMessage.partitionResponse()), 200);
        assertDoesNotThrow(
                () ->
                        restCatalog.createPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testCreatePartitionWhenTableNotExist() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("p1", "v1");
        mockResponse("", 404);
        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.createPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testCreatePartitionWhenTableNoPermissionException() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put("p1", "v1");
        mockResponse("", 403);
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () ->
                        restCatalog.createPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testDropPartition() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        Map<String, String> partitionSpec = new HashMap<>();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        partitionSpec.put(response.getSchema().primaryKeys().get(0), "1");
        mockResponse(mapper.writeValueAsString(""), 200);
        mockResponse(mapper.writeValueAsString(response), 200);
        assertThrows(
                RuntimeException.class,
                () ->
                        restCatalog.dropPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testDropPartitionWhenPartitionNoExist() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        GetTableResponse response = MockRESTMessage.getTableResponseEnablePartition();
        mockResponse(mapper.writeValueAsString(response), 200);

        Map<String, String> partitionSpec = new HashMap<>();
        partitionSpec.put(response.getSchema().primaryKeys().get(0), "1");
        mockResponse(mapper.writeValueAsString(""), 404);
        assertThrows(
                Catalog.PartitionNotExistException.class,
                () ->
                        restCatalog.dropPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testDropPartitionWhenTableNoPermission() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        Map<String, String> partitionSpec = new HashMap<>();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        partitionSpec.put(response.getSchema().primaryKeys().get(0), "1");
        mockResponse(mapper.writeValueAsString(""), 403);
        assertThrows(
                Catalog.TableNoPermissionException.class,
                () ->
                        restCatalog.dropPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testDropPartitionWhenTableNoExist() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        Map<String, String> partitionSpec = new HashMap<>();
        GetTableResponse response = MockRESTMessage.getTableResponse();
        partitionSpec.put(response.getSchema().primaryKeys().get(0), "1");
        mockResponse("", 404);
        assertThrows(
                Catalog.TableNotExistException.class,
                () ->
                        restCatalog.dropPartition(
                                Identifier.create(databaseName, "table"), partitionSpec));
    }

    @Test
    public void testListPartitionsWhenMetastorePartitionedIsTrue() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        GetTableResponse getTableResponse = MockRESTMessage.getTableResponseEnablePartition();
        mockResponse(mapper.writeValueAsString(getTableResponse), 200);
        ListPartitionsResponse response = MockRESTMessage.listPartitionsResponse();
        mockResponse(mapper.writeValueAsString(response), 200);
        List<Partition> result =
                restCatalog.listPartitions(Identifier.create(databaseName, "table"));
        assertEquals(response.getPartitions().size(), result.size());
    }

    @Test
    public void testListPartitionsFromFile() throws Exception {
        String databaseName = MockRESTMessage.databaseName();
        GetTableResponse response = MockRESTMessage.getTableResponseEnablePartition();
        mockResponse(mapper.writeValueAsString(response), 200);
        mockResponse(mapper.writeValueAsString(response), 200);
        List<Partition> partitionEntries =
                restCatalog.listPartitions(Identifier.create(databaseName, "table"));
        assertEquals(partitionEntries.size(), 0);
    }

    private void mockResponse(String mockResponse, int httpCode) {
        MockResponse mockResponseObj =
                new MockResponse()
                        .setResponseCode(httpCode)
                        .setBody(mockResponse)
                        .addHeader("Content-Type", "application/json");
        mockWebServer.enqueue(mockResponseObj);
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

    public Options mockInitOptions() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, serverUrl);
        String initToken = "init_token";
        options.set(RESTCatalogOptions.TOKEN, initToken);
        options.set(RESTCatalogOptions.THREAD_POOL_SIZE, 1);
        return options;
    }
}
