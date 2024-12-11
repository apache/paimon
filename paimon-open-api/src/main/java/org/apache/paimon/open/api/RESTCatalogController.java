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
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.DropDatabaseRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.DatabaseName;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

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

/** * RESTCatalog management APIs. */
@CrossOrigin(origins = "http://localhost:8081")
@RestController
public class RESTCatalogController {

    @Operation(
            summary = "Get Config",
            tags = {"config"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(
                            schema = @Schema(implementation = ConfigResponse.class),
                            mediaType = "application/json")
                }),
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
                    @Content(
                            schema = @Schema(implementation = ListDatabasesResponse.class),
                            mediaType = "application/json")
                }),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/api/v1/{prefix}/databases")
    public ListDatabasesResponse listDatabases(@PathVariable String prefix) {
        return new ListDatabasesResponse(ImmutableList.of(new DatabaseName("account")));
    }

    @Operation(
            summary = "Create Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "200",
                content = {
                    @Content(
                            schema = @Schema(implementation = CreateDatabaseResponse.class),
                            mediaType = "application/json")
                }),
        @ApiResponse(
                responseCode = "409",
                description = "Resource has exist",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @PostMapping("/api/v1/{prefix}/databases")
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
                content = {
                    @Content(
                            schema = @Schema(implementation = GetDatabaseResponse.class),
                            mediaType = "application/json")
                }),
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping("/api/v1/{prefix}/databases/{database}")
    public GetDatabaseResponse getDatabases(
            @PathVariable String prefix, @PathVariable String database) {
        Map<String, String> options = new HashMap<>();
        return new GetDatabaseResponse("name", options, "comment");
    }

    @Operation(
            summary = "Drop Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "404",
                description = "Resource not found",
                content = {@Content(schema = @Schema(implementation = ErrorResponse.class))}),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @DeleteMapping("/api/v1/{prefix}/databases/{database}")
    public void dropDatabases(
            @PathVariable String prefix,
            @PathVariable String database,
            @RequestBody DropDatabaseRequest request) {}
}
