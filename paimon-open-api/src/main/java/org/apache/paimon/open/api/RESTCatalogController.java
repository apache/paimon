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

package org.apache.paimon.open.api;

import org.apache.paimon.rest.ResourcePaths;
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
import org.apache.paimon.table.Partition;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/** RESTCatalog management APIs. */
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class RESTCatalogController {

    @Operation(
            summary = "Get Config",
            tags = {"config"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = ConfigResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping(ResourcePaths.V1_CONFIG)
    public ConfigResponse getConfig() {
        Map<String, String> defaults = new HashMap<>();
        Map<String, String> overrides = new HashMap<>();
        return new ConfigResponse(defaults, overrides);
    }

    @Operation(
            summary = "List Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = ListDatabasesResponse.class))
                }),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/v1/{prefix}/databases")
    public ListDatabasesResponse listDatabases(@PathVariable String prefix) {
        return new ListDatabasesResponse(ImmutableList.of("account"));
    }

    @Operation(
            summary = "Create Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = CreateDatabaseResponse.class))
                }),
        @ApiResponse(
                responseCode = "409",
                description = "Resource has exist",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases")
    public CreateDatabaseResponse createDatabases(
            @PathVariable String prefix, @RequestBody CreateDatabaseRequest request) {
        Map<String, String> properties = new HashMap<>();
        return new CreateDatabaseResponse("name", properties);
    }

    @Operation(
            summary = "Get Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetDatabaseResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/v1/{prefix}/databases/{database}")
    public GetDatabaseResponse getDatabases(
            @PathVariable String prefix, @PathVariable String database) {
        Map<String, String> options = new HashMap<>();
        return new GetDatabaseResponse("name", options);
    }

    @Operation(
            summary = "Drop Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @DeleteMapping("/v1/{prefix}/databases/{database}")
    public void dropDatabase(@PathVariable String prefix, @PathVariable String database) {}

    @Operation(
            summary = "Alter Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = AlterDatabaseResponse.class))
                }),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "409",
                description = "Resource has exist",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/properties")
    public AlterDatabaseResponse alterDatabase(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestBody AlterDatabaseRequest request) {
        return new AlterDatabaseResponse(
                Lists.newArrayList("remove"),
                Lists.newArrayList("add"),
                Lists.newArrayList("missing"));
    }

    @Operation(
            summary = "List tables",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = ListTablesResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables")
    public ListTablesResponse listTables(
            @PathVariable String prefix, @PathVariable String database) {
        return new ListTablesResponse(ImmutableList.of("user"));
    }

    @Operation(
            summary = "Get table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetTableResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "409",
                description = "Resource has exist",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}")
    public GetTableResponse getTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        return new GetTableResponse(
                "",
                1,
                new org.apache.paimon.schema.Schema(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new HashMap<>(),
                        "comment"));
    }

    @Operation(
            summary = "Create table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetTableResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables")
    public GetTableResponse createTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestBody CreateTableRequest request) {
        return new GetTableResponse(
                "",
                1,
                new org.apache.paimon.schema.Schema(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new HashMap<>(),
                        "comment"));
    }

    @Operation(
            summary = "Alter table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetTableResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}")
    public GetTableResponse alterTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody AlterTableRequest request) {
        return new GetTableResponse(
                "",
                1,
                new org.apache.paimon.schema.Schema(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new HashMap<>(),
                        "comment"));
    }

    @Operation(
            summary = "Drop table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @DeleteMapping("/v1/{prefix}/databases/{database}/tables/table")
    public void dropTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {}

    @Operation(
            summary = "Rename table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetTableResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/rename")
    public GetTableResponse renameTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody RenameTableRequest request) {
        return new GetTableResponse(
                "",
                1,
                new org.apache.paimon.schema.Schema(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new HashMap<>(),
                        "comment"));
    }

    @Operation(
            summary = "List partitions",
            tags = {"partition"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = ListPartitionsResponse.class))
                }),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}/partitions")
    public ListPartitionsResponse listPartitions(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        Map<String, String> spec = new HashMap<>();
        spec.put("f1", "1");
        Partition partition = new Partition(spec, 1, 2, 3, 4);
        return new ListPartitionsResponse(ImmutableList.of(partition));
    }

    @Operation(
            summary = "Create partition",
            tags = {"partition"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = PartitionResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/partitions")
    public PartitionResponse createPartition(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody CreatePartitionRequest request) {
        Map<String, String> spec = new HashMap<>();
        spec.put("f1", "1");
        return new PartitionResponse(new Partition(spec, 0, 0, 0, 4));
    }

    @Operation(
            summary = "Drop partition",
            tags = {"partition"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @DeleteMapping("/v1/{prefix}/databases/{database}/tables/{table}/partitions")
    public void dropPartition(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody DropPartitionRequest request) {}
}
