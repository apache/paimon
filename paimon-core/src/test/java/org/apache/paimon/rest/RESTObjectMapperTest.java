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

import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.DropDatabaseRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.DatabaseName;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
        String name = "name";
        boolean ignoreIfExists = true;
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, ignoreIfExists, options);
        String requestStr = mapper.writeValueAsString(request);
        CreateDatabaseRequest parseData = mapper.readValue(requestStr, CreateDatabaseRequest.class);
        assertEquals(name, parseData.getName());
        assertEquals(ignoreIfExists, parseData.getIgnoreIfExists());
        assertEquals(options, parseData.getOptions());
    }

    @Test
    public void dropDatabaseRequestParseTest() throws Exception {
        boolean ignoreIfNotExists = true;
        boolean cascade = true;
        DropDatabaseRequest request = new DropDatabaseRequest(ignoreIfNotExists, cascade);
        String requestStr = mapper.writeValueAsString(request);
        DropDatabaseRequest parseData = mapper.readValue(requestStr, DropDatabaseRequest.class);
        assertEquals(ignoreIfNotExists, parseData.getIgnoreIfNotExists());
        assertEquals(cascade, parseData.getCascade());
    }

    @Test
    public void createDatabaseResponseParseTest() throws Exception {
        String name = "name";
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        CreateDatabaseResponse response = new CreateDatabaseResponse(name, options);
        String responseStr = mapper.writeValueAsString(response);
        CreateDatabaseResponse parseData =
                mapper.readValue(responseStr, CreateDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(options, parseData.getOptions());
    }

    @Test
    public void getDatabaseResponseParseTest() throws Exception {
        String name = "name";
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        String comment = "comment";
        GetDatabaseResponse response = new GetDatabaseResponse(name, options, comment);
        String responseStr = mapper.writeValueAsString(response);
        GetDatabaseResponse parseData = mapper.readValue(responseStr, GetDatabaseResponse.class);
        assertEquals(name, parseData.getName());
        assertEquals(options, parseData.getOptions());
        assertEquals(comment, parseData.getComment());
    }

    @Test
    public void listDatabaseResponseParseTest() throws Exception {
        String name = "name";
        DatabaseName databaseName = new DatabaseName(name);
        List<DatabaseName> databaseNameList = new ArrayList<>();
        databaseNameList.add(databaseName);
        ListDatabasesResponse response = new ListDatabasesResponse(databaseNameList);
        String responseStr = mapper.writeValueAsString(response);
        ListDatabasesResponse parseData =
                mapper.readValue(responseStr, ListDatabasesResponse.class);
        assertEquals(databaseNameList.size(), parseData.getDatabases().size());
        assertEquals(name, parseData.getDatabases().get(0).getName());
    }
}
