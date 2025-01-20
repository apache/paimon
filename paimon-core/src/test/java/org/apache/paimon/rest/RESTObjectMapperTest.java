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
import org.apache.paimon.rest.requests.AlterPartitionsRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.DropPartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableCredentialsResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTObjectMapper.OBJECT_MAPPER;
import static org.junit.Assert.assertEquals;

/** Test for {@link RESTObjectMapper}. */
public class RESTObjectMapperTest {

    @Test
    public void configResponseParseTest() throws Exception {
        String confKey = "a";
        Map<String, String> conf = new HashMap<>();
        conf.put(confKey, "b");
        ConfigResponse response = new ConfigResponse(conf, conf);
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ConfigResponse parseData = OBJECT_MAPPER.readValue(responseStr, ConfigResponse.class);
        assertEquals(conf.get(confKey), parseData.getDefaults().get(confKey));
    }

    @Test
    public void errorResponseParseTest() throws Exception {
        String message = "message";
        Integer code = 400;
        ErrorResponse response =
                new ErrorResponse(null, null, message, code, new ArrayList<String>());
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ErrorResponse parseData = OBJECT_MAPPER.readValue(responseStr, ErrorResponse.class);
        assertEquals(message, parseData.getMessage());
        assertEquals(code, parseData.getCode());
    }

    @Test
    public void createDatabaseRequestParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseRequest request = MockRESTMessage.createDatabaseRequest(name);
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        CreateDatabaseRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, CreateDatabaseRequest.class);
        assertEquals(request.getName(), parseData.getName());
        assertEquals(request.getOptions().size(), parseData.getOptions().size());
    }

    @Test
    public void createDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        CreateDatabaseResponse response = MockRESTMessage.createDatabaseResponse(name);
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        CreateDatabaseResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, CreateDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(response.getOptions().size(), parseData.getOptions().size());
    }

    @Test
    public void getDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        GetDatabaseResponse response = MockRESTMessage.getDatabaseResponse(name);
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        GetDatabaseResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, GetDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(response.getOptions().size(), parseData.getOptions().size());
        assertEquals(response.comment().get(), parseData.comment().get());
    }

    @Test
    public void listDatabaseResponseParseTest() throws Exception {
        String name = MockRESTMessage.databaseName();
        ListDatabasesResponse response = MockRESTMessage.listDatabasesResponse(name);
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ListDatabasesResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, ListDatabasesResponse.class);
        assertEquals(response.getDatabases().size(), parseData.getDatabases().size());
        assertEquals(name, parseData.getDatabases().get(0));
    }

    @Test
    public void alterDatabaseRequestParseTest() throws Exception {
        AlterDatabaseRequest request = MockRESTMessage.alterDatabaseRequest();
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        AlterDatabaseRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, AlterDatabaseRequest.class);
        assertEquals(request.getRemovals().size(), parseData.getRemovals().size());
        assertEquals(request.getUpdates().size(), parseData.getUpdates().size());
    }

    @Test
    public void alterDatabaseResponseParseTest() throws Exception {
        AlterDatabaseResponse response = MockRESTMessage.alterDatabaseResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        AlterDatabaseResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, AlterDatabaseResponse.class);
        assertEquals(response.getRemoved().size(), parseData.getRemoved().size());
        assertEquals(response.getUpdated().size(), parseData.getUpdated().size());
        assertEquals(response.getMissing().size(), parseData.getMissing().size());
    }

    @Test
    public void createTableRequestParseTest() throws Exception {
        CreateTableRequest request = MockRESTMessage.createTableRequest("t1");
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        CreateTableRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, CreateTableRequest.class);
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
        DataField parseData = OBJECT_MAPPER.readValue(dataFieldStr, DataField.class);
        assertEquals(id, parseData.id());
        assertEquals(name, parseData.name());
        assertEquals(type, parseData.type());
        assertEquals(descStr, parseData.description());
    }

    @Test
    public void renameTableRequestParseTest() throws Exception {
        RenameTableRequest request = MockRESTMessage.renameRequest("t1", "t2");
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        RenameTableRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, RenameTableRequest.class);
        assertEquals(request.getSource(), parseData.getSource());
        assertEquals(request.getDestination(), parseData.getDestination());
    }

    @Test
    public void getTableResponseParseTest() throws Exception {
        GetTableResponse response = MockRESTMessage.getTableResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        GetTableResponse parseData = OBJECT_MAPPER.readValue(responseStr, GetTableResponse.class);
        assertEquals(response.getSchemaId(), parseData.getSchemaId());
        assertEquals(response.getSchema(), parseData.getSchema());
    }

    @Test
    public void listTablesResponseParseTest() throws Exception {
        ListTablesResponse response = MockRESTMessage.listTablesResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ListTablesResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, ListTablesResponse.class);
        assertEquals(response.getTables(), parseData.getTables());
    }

    @Test
    public void alterTableRequestParseTest() throws Exception {
        AlterTableRequest request = MockRESTMessage.alterTableRequest();
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        AlterTableRequest parseData = OBJECT_MAPPER.readValue(requestStr, AlterTableRequest.class);
        assertEquals(parseData.getChanges().size(), parseData.getChanges().size());
    }

    @Test
    public void createPartitionRequestParseTest() throws JsonProcessingException {
        CreatePartitionsRequest request = MockRESTMessage.createPartitionRequest();
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        CreatePartitionsRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, CreatePartitionsRequest.class);
        assertEquals(parseData.getPartitionSpecs().size(), parseData.getPartitionSpecs().size());
    }

    @Test
    public void dropPartitionRequestParseTest() throws JsonProcessingException {
        DropPartitionsRequest request = MockRESTMessage.dropPartitionsRequest();
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        DropPartitionsRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, DropPartitionsRequest.class);
        assertEquals(parseData.getPartitionSpecs().size(), parseData.getPartitionSpecs().size());
    }

    @Test
    public void listPartitionsResponseParseTest() throws Exception {
        ListPartitionsResponse response = MockRESTMessage.listPartitionsResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ListPartitionsResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, ListPartitionsResponse.class);
        assertEquals(
                response.getPartitions().get(0).fileCount(),
                parseData.getPartitions().get(0).fileCount());
    }

    @Test
    public void alterPartitionsRequestParseTest() throws Exception {
        AlterPartitionsRequest request = MockRESTMessage.alterPartitionsRequest();
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        AlterPartitionsRequest parseData =
                OBJECT_MAPPER.readValue(requestStr, AlterPartitionsRequest.class);
        assertEquals(request.getPartitions(), parseData.getPartitions());
    }

    @Test
    public void createViewRequestParseTest() throws Exception {
        CreateViewRequest request = MockRESTMessage.createViewRequest("t1");
        String requestStr = OBJECT_MAPPER.writeValueAsString(request);
        CreateViewRequest parseData = OBJECT_MAPPER.readValue(requestStr, CreateViewRequest.class);
        assertEquals(request.getIdentifier(), parseData.getIdentifier());
        assertEquals(request.getSchema(), parseData.getSchema());
    }

    @Test
    public void getViewResponseParseTest() throws Exception {
        GetViewResponse response = MockRESTMessage.getViewResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        GetViewResponse parseData = OBJECT_MAPPER.readValue(responseStr, GetViewResponse.class);
        assertEquals(response.getId(), parseData.getId());
        assertEquals(response.getName(), parseData.getName());
        assertEquals(response.getSchema(), parseData.getSchema());
    }

    @Test
    public void listViewsResponseParseTest() throws Exception {
        ListViewsResponse response = MockRESTMessage.listViewsResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        ListViewsResponse parseData = OBJECT_MAPPER.readValue(responseStr, ListViewsResponse.class);
        assertEquals(response.getViews(), parseData.getViews());
    }

    @Test
    public void getTableCredentialsResponseParseTest() throws Exception {
        GetTableCredentialsResponse response = MockRESTMessage.getTableCredentialsResponse();
        String responseStr = OBJECT_MAPPER.writeValueAsString(response);
        GetTableCredentialsResponse parseData =
                OBJECT_MAPPER.readValue(responseStr, GetTableCredentialsResponse.class);
        assertEquals(response.getCredential(), parseData.getCredential());
        assertEquals(response.getExpiresAtMillis(), parseData.getExpiresAtMillis());
    }
}
