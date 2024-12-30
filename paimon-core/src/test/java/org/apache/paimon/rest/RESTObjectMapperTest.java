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

import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreatePartitionRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.DropPartitionRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.PartitionResponse;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Test for {@link RESTObjectMapper}. */
public class RESTObjectMapperTest {
    private ObjectMapper mapper = RESTObjectMapper.create();

    @Test
    public void configResponseParseTest() throws Exception {
        String confKey = "a";
        Map<String, String> conf = new HashMap<>();
        conf.put(confKey, "b");
        ConfigResponse response = new ConfigResponse(conf, conf);
        String responseStr = mapper.writeValueAsString(response);
        ConfigResponse parseData = mapper.readValue(responseStr, ConfigResponse.class);
        assertEquals(conf.get(confKey), parseData.getDefaults().get(confKey));
    }

    @Test
    public void errorResponseParseTest() throws Exception {
        String message = "message";
        Integer code = 400;
        ErrorResponse response = new ErrorResponse(message, code, new ArrayList<String>());
        String responseStr = mapper.writeValueAsString(response);
        ErrorResponse parseData = mapper.readValue(responseStr, ErrorResponse.class);
        assertEquals(message, parseData.getMessage());
        assertEquals(code, parseData.getCode());
    }

    @Test
    public void createDatabaseRequestParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseRequest request = MockRESTMessage.createDatabaseRequest(name);
        String requestStr = mapper.writeValueAsString(request);
        CreateDatabaseRequest parseData = mapper.readValue(requestStr, CreateDatabaseRequest.class);
        assertEquals(request.getName(), parseData.getName());
        assertEquals(request.getOptions().size(), parseData.getOptions().size());
    }

    @Test
    public void createDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseResponse response = MockRESTMessage.createDatabaseResponse(name);
        String responseStr = mapper.writeValueAsString(response);
        CreateDatabaseResponse parseData =
                mapper.readValue(responseStr, CreateDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(response.getOptions().size(), parseData.getOptions().size());
    }

    @Test
    public void getDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        GetDatabaseResponse response = MockRESTMessage.getDatabaseResponse(name);
        String responseStr = mapper.writeValueAsString(response);
        GetDatabaseResponse parseData = mapper.readValue(responseStr, GetDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(response.getOptions().size(), parseData.getOptions().size());
        assertEquals(response.comment().get(), parseData.comment().get());
    }

    @Test
    public void listDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        ListDatabasesResponse response = MockRESTMessage.listDatabasesResponse(name);
        String responseStr = mapper.writeValueAsString(response);
        ListDatabasesResponse parseData =
                mapper.readValue(responseStr, ListDatabasesResponse.class);
        assertEquals(response.getDatabases().size(), parseData.getDatabases().size());
        assertEquals(name, parseData.getDatabases().get(0));
    }

    @Test
    public void alterDatabaseRequestParseTest() throws Exception {
        AlterDatabaseRequest request = MockRESTMessage.alterDatabaseRequest();
        String requestStr = mapper.writeValueAsString(request);
        AlterDatabaseRequest parseData = mapper.readValue(requestStr, AlterDatabaseRequest.class);
        assertEquals(request.getRemovals().size(), parseData.getRemovals().size());
        assertEquals(request.getUpdates().size(), parseData.getUpdates().size());
    }

    @Test
    public void alterDatabaseResponseParseTest() throws Exception {
        AlterDatabaseResponse response = MockRESTMessage.alterDatabaseResponse();
        String responseStr = mapper.writeValueAsString(response);
        AlterDatabaseResponse parseData =
                mapper.readValue(responseStr, AlterDatabaseResponse.class);
        assertEquals(response.getRemoved().size(), parseData.getRemoved().size());
        assertEquals(response.getUpdated().size(), parseData.getUpdated().size());
        assertEquals(response.getMissing().size(), parseData.getMissing().size());
    }

    @Test
    public void createTableRequestParseTest() throws Exception {
        CreateTableRequest request = MockRESTMessage.createTableRequest("t1");
        String requestStr = mapper.writeValueAsString(request);
        CreateTableRequest parseData = mapper.readValue(requestStr, CreateTableRequest.class);
        assertEquals(request.getIdentifier(), parseData.getIdentifier());
        assertEquals(request.getSchema(), parseData.getSchema());
    }

    // This test is to guarantee the compatibility of field name in RESTCatalog.
    @Test
    public void dataFieldParseTest() throws Exception {
        int id = 1;
        String name = "col1";
        IntType type = DataTypes.INT();
        String descStr = "desc";
        String dataFieldStr =
                String.format(
                        "{\"id\": %d,\"name\":\"%s\",\"type\":\"%s\", \"description\":\"%s\"}",
                        id, name, type, descStr);
        DataField parseData = mapper.readValue(dataFieldStr, DataField.class);
        assertEquals(id, parseData.id());
        assertEquals(name, parseData.name());
        assertEquals(type, parseData.type());
        assertEquals(descStr, parseData.description());
    }

    @Test
    public void renameTableRequestParseTest() throws Exception {
        RenameTableRequest request = MockRESTMessage.renameRequest("t2");
        String requestStr = mapper.writeValueAsString(request);
        RenameTableRequest parseData = mapper.readValue(requestStr, RenameTableRequest.class);
        assertEquals(request.getNewIdentifier(), parseData.getNewIdentifier());
    }

    @Test
    public void getTableResponseParseTest() throws Exception {
        GetTableResponse response = MockRESTMessage.getTableResponse();
        String responseStr = mapper.writeValueAsString(response);
        GetTableResponse parseData = mapper.readValue(responseStr, GetTableResponse.class);
        assertEquals(response.getSchemaId(), parseData.getSchemaId());
        assertEquals(response.getSchema(), parseData.getSchema());
    }

    @Test
    public void listTablesResponseParseTest() throws Exception {
        ListTablesResponse response = MockRESTMessage.listTablesResponse();
        String responseStr = mapper.writeValueAsString(response);
        ListTablesResponse parseData = mapper.readValue(responseStr, ListTablesResponse.class);
        assertEquals(response.getTables(), parseData.getTables());
    }

    @Test
    public void alterTableRequestParseTest() throws Exception {
        AlterTableRequest request = MockRESTMessage.alterTableRequest();
        String requestStr = mapper.writeValueAsString(request);
        AlterTableRequest parseData = mapper.readValue(requestStr, AlterTableRequest.class);
        assertEquals(parseData.getChanges().size(), parseData.getChanges().size());
    }

    @Test
    public void createPartitionRequestParseTest() throws JsonProcessingException {
        CreatePartitionRequest request = MockRESTMessage.createPartitionRequest("t1");
        String requestStr = mapper.writeValueAsString(request);
        CreatePartitionRequest parseData =
                mapper.readValue(requestStr, CreatePartitionRequest.class);
        assertEquals(parseData.getIdentifier(), parseData.getIdentifier());
        assertEquals(parseData.getPartitionSpec().size(), parseData.getPartitionSpec().size());
    }

    @Test
    public void dropPartitionRequestParseTest() throws JsonProcessingException {
        DropPartitionRequest request = MockRESTMessage.dropPartitionRequest();
        String requestStr = mapper.writeValueAsString(request);
        DropPartitionRequest parseData = mapper.readValue(requestStr, DropPartitionRequest.class);
        assertEquals(parseData.getPartitionSpec().size(), parseData.getPartitionSpec().size());
    }

    @Test
    public void listPartitionsResponseParseTest() throws Exception {
        ListPartitionsResponse response = MockRESTMessage.listPartitionsResponse();
        String responseStr = mapper.writeValueAsString(response);
        ListPartitionsResponse parseData =
                mapper.readValue(responseStr, ListPartitionsResponse.class);
        assertEquals(
                response.getPartitions().get(0).getFileCount(),
                parseData.getPartitions().get(0).getFileCount());
    }

    @Test
    public void partitionResponseParseTest() throws Exception {
        PartitionResponse response = MockRESTMessage.partitionResponse();
        assertDoesNotThrow(() -> mapper.writeValueAsString(response));
        assertDoesNotThrow(
                () ->
                        mapper.readValue(
                                mapper.writeValueAsString(response), PartitionResponse.class));
    }
}
