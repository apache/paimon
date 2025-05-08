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

import org.apache.paimon.partition.Partition;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.ForwardBranchRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.rest.RESTCatalog.QUERY_PARAMETER_WAREHOUSE_KEY;

/** RESTCatalog open APIs. */
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
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/config")
    public ConfigResponse getConfig(@RequestParam(QUERY_PARAMETER_WAREHOUSE_KEY) String warehouse) {
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
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases")
    public ListDatabasesResponse listDatabases(
            @PathVariable String prefix,
            @PathVariable Integer maxResults,
            @PathVariable String pageToken) {
        return new ListDatabasesResponse(ImmutableList.of("account"), null);
    }

    @Operation(
            summary = "Create Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "409",
                description = "Resource has exist",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases")
    public void createDatabases(
            @PathVariable String prefix, @RequestBody CreateDatabaseRequest request) {}

    @Operation(
            summary = "Get Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetDatabaseResponse.class))}),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}")
    public GetDatabaseResponse getDatabases(
            @PathVariable String prefix, @PathVariable String database) {
        Map<String, String> options = new HashMap<>();
        return new GetDatabaseResponse(
                UUID.randomUUID().toString(),
                "name",
                "/tmp/",
                options,
                "owner",
                System.currentTimeMillis(),
                "created",
                System.currentTimeMillis(),
                "updated");
    }

    @Operation(
            summary = "Drop Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
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
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
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
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}")
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
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables")
    public ListTablesResponse listTables(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestParam(required = false) Integer maxResults,
            @RequestParam(required = false) String pageToken,
            @RequestParam(required = false) String tableNamePattern) {
        // paged list tables in this database with provided maxResults and pageToken
        return new ListTablesResponse(ImmutableList.of("user"), null);
    }

    @Operation(
            summary = "List table details",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = ListTableDetailsResponse.class))
                }),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/table-details")
    public ListTableDetailsResponse listTableDetails(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestParam(required = false) Integer maxResults,
            @RequestParam(required = false) String pageToken,
            @RequestParam(required = false) String tableNamePattern) {
        // paged list table details in this database with provided maxResults and pageToken
        GetTableResponse singleTable =
                new GetTableResponse(
                        UUID.randomUUID().toString(),
                        "",
                        "/tmp/",
                        false,
                        1,
                        new org.apache.paimon.schema.Schema(
                                ImmutableList.of(),
                                ImmutableList.of(),
                                ImmutableList.of(),
                                new HashMap<>(),
                                "test-comment"),
                        "owner",
                        System.currentTimeMillis(),
                        "created",
                        System.currentTimeMillis(),
                        "updated");
        return new ListTableDetailsResponse(ImmutableList.of(singleTable), null);
    }

    @Operation(
            summary = "Get table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetTableResponse.class))}),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
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
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}")
    public GetTableResponse getTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        return new GetTableResponse(
                UUID.randomUUID().toString(),
                "",
                "/tmp/",
                false,
                1,
                new org.apache.paimon.schema.Schema(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        new HashMap<>(),
                        "comment"),
                "owner",
                System.currentTimeMillis(),
                "created",
                System.currentTimeMillis(),
                "updated");
    }

    @Operation(
            summary = "Create table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables")
    public void createTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestBody CreateTableRequest request) {}

    @Operation(
            summary = "Alter table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}")
    public void alterTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody AlterTableRequest request) {}

    @Operation(
            summary = "Drop table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
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
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/tables/rename")
    public void renameTable(@PathVariable String prefix, @RequestBody RenameTableRequest request) {}

    @Operation(
            summary = "Commit table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = CommitTableResponse.class))}),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/commit")
    public CommitTableResponse commitTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody CommitTableRequest request) {
        return new CommitTableResponse(true);
    }

    @Operation(
            summary = "Rollback table",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/rollback")
    public void rollbackTable(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody RollbackTableRequest request) {}

    @Operation(
            summary = "Get table token",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = GetTableTokenResponse.class))
                }),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}/token")
    public GetTableTokenResponse getTableToken(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        return new GetTableTokenResponse(
                ImmutableMap.of("key", "value"), System.currentTimeMillis());
    }

    @Operation(
            summary = "Get table snapshot",
            tags = {"table"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = GetTableSnapshotResponse.class))
                }),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}/snapshot")
    public GetTableSnapshotResponse getTableSnapshot(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        return new GetTableSnapshotResponse(null);
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
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}/partitions")
    public ListPartitionsResponse listPartitions(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestParam(required = false) Integer maxResults,
            @RequestParam(required = false) String pageToken,
            @RequestParam(required = false) String partitionNamePattern) {
        // paged list partitions in this table with provided maxResults and pageToken
        Map<String, String> spec = new HashMap<>();
        spec.put("f1", "1");
        Partition partition = new Partition(spec, 1, 2, 3, 4, false);
        return new ListPartitionsResponse(ImmutableList.of(partition));
    }

    @Operation(
            summary = "MarkDone partitions",
            tags = {"partition"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/partitions/mark")
    public void markDonePartitions(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody MarkDonePartitionsRequest request) {}

    @Operation(
            summary = "List branches",
            tags = {"branch"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = ListBranchesResponse.class))
                }),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/tables/{table}/branches")
    public ListBranchesResponse listBranches(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table) {
        return new ListBranchesResponse(ImmutableList.of("branch"));
    }

    @Operation(
            summary = "Create branch",
            tags = {"branch"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/branches")
    public void createBranch(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @RequestBody CreateBranchRequest request) {}

    @Operation(
            summary = "Forward branch",
            tags = {"branch"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}/forward")
    public void forwardBranch(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @PathVariable String branch,
            @RequestBody ForwardBranchRequest request) {}

    @Operation(
            summary = "Drop branch",
            tags = {"branch"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @DeleteMapping("/v1/{prefix}/databases/{database}/tables/table/branches/branch")
    public void dropBranch(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String table,
            @PathVariable String branch) {}

    @Operation(
            summary = "List views",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = ListViewsResponse.class))}),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/views")
    public ListViewsResponse listViews(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestParam(required = false) Integer maxResults,
            @RequestParam(required = false) String pageToken,
            @RequestParam(required = false) String viewNamePattern) {
        // paged list tables in this database with provided maxResults and pageToken
        return new ListViewsResponse(ImmutableList.of("user"), null);
    }

    @Operation(
            summary = "List view details",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(schema = @Schema(implementation = ListViewDetailsResponse.class))
                }),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/view-details")
    public ListViewDetailsResponse listViewDetails(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestParam(required = false) Integer maxResults,
            @RequestParam(required = false) String pageToken,
            @RequestParam(required = false) String viewNamePattern) {
        // paged list view details in this database with provided maxResults and pageToken
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()));
        ViewSchema schema =
                new ViewSchema(
                        fields,
                        "select * from t1",
                        Collections.emptyMap(),
                        "comment",
                        Collections.singletonMap("pt", "1"));
        GetViewResponse singleView =
                new GetViewResponse(
                        "id",
                        "name",
                        schema,
                        "owner",
                        System.currentTimeMillis(),
                        "created",
                        System.currentTimeMillis(),
                        "updated");
        return new ListViewDetailsResponse(ImmutableList.of(singleView), null);
    }

    @Operation(
            summary = "create view",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/views")
    public void createView(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestBody CreateViewRequest request) {}

    @Operation(
            summary = "Get view",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {@Content(schema = @Schema(implementation = GetViewResponse.class))}),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @GetMapping("/v1/{prefix}/databases/{database}/views/{view}")
    public GetViewResponse getView(
            @PathVariable String prefix, @PathVariable String database, @PathVariable String view) {
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", new IntType()),
                        new DataField(1, "f1", new IntType()));
        ViewSchema schema =
                new ViewSchema(
                        fields,
                        "select * from t1",
                        Collections.emptyMap(),
                        "comment",
                        Collections.singletonMap("pt", "1"));
        return new GetViewResponse(
                "id",
                "name",
                schema,
                "owner",
                System.currentTimeMillis(),
                "created",
                System.currentTimeMillis(),
                "updated");
    }

    @Operation(
            summary = "Rename view",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/views/rename")
    public void renameView(@PathVariable String prefix, @RequestBody RenameTableRequest request) {}

    @Operation(
            summary = "Drop view",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @DeleteMapping("/v1/{prefix}/databases/{database}/views/{view}")
    public void dropView(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String view) {}

    @Operation(
            summary = "Alter view",
            tags = {"view"})
    @ApiResponses({
        @ApiResponse(responseCode = "200", description = "Success, no content"),
        @ApiResponse(
                responseCode = "401",
                description = "Unauthorized",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
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
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))})
    })
    @PostMapping("/v1/{prefix}/databases/{database}/views/{view}")
    public void alterView(
            @PathVariable String prefix,
            @PathVariable String database,
            @PathVariable String view,
            @RequestBody AlterViewRequest request) {}
}
