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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.UpdateTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.DATABASE_COMMENT;

/** Mock REST message. */
public class MockRESTMessage {

    public static String databaseName() {
        return "database";
    }

    public static CreateDatabaseRequest createDatabaseRequest(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        return new CreateDatabaseRequest(name, options);
    }

    public static CreateDatabaseResponse createDatabaseResponse(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        return new CreateDatabaseResponse(name, options);
    }

    public static GetDatabaseResponse getDatabaseResponse(String name) {
        Map<String, String> options = new HashMap<>();
        options.put("a", "b");
        options.put(DATABASE_COMMENT.key(), "comment");
        return new GetDatabaseResponse(name, options);
    }

    public static ListDatabasesResponse listDatabasesResponse(String name) {
        List<String> databaseNameList = new ArrayList<>();
        databaseNameList.add(name);
        return new ListDatabasesResponse(databaseNameList);
    }

    public static ErrorResponse noSuchResourceExceptionErrorResponse() {
        return new ErrorResponse("message", 404, new ArrayList<>());
    }

    public static AlterDatabaseRequest alterDatabaseRequest() {
        Map<String, String> add = new HashMap<>();
        add.put("add", "value");
        return new AlterDatabaseRequest(Lists.newArrayList("remove"), add);
    }

    public static AlterDatabaseResponse alterDatabaseResponse() {
        return new AlterDatabaseResponse(
                Lists.newArrayList("remove"), Lists.newArrayList("add"), new ArrayList<>());
    }

    public static ListTablesResponse listTablesResponse() {
        return new ListTablesResponse(Lists.newArrayList("table"));
    }

    public static ListTablesResponse listTablesEmptyResponse() {
        return new ListTablesResponse(Lists.newArrayList());
    }

    public static CreateTableRequest createTableRequest(String name) {
        Identifier identifier = Identifier.create(databaseName(), name);
        Map<String, String> options = new HashMap<>();
        options.put("k1", "v1");
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("pk", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("pk", "pt")
                        .options(options)
                        .build();
        return new CreateTableRequest(identifier, schema);
    }

    public static UpdateTableRequest updateTableRequest(String fromTableName, String toTableName) {
        Identifier fromIdentifier = Identifier.create(databaseName(), fromTableName);
        Identifier toIdentifier = Identifier.create(databaseName(), toTableName);
        return new UpdateTableRequest(fromIdentifier, toIdentifier, null);
    }
}
