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
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.CreateDatabaseResponse;
import org.apache.paimon.rest.responses.DatabaseName;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
                responseCode = "201",
                content = {
                    @Content(
                            schema = @Schema(implementation = ConfigResponse.class),
                            examples = {
                                @ExampleObject(
                                        name = "defaults",
                                        value =
                                                "{\n"
                                                        + "    \"k1\": \"v1\",\n"
                                                        + "    \"k2\": \"v2\",\n"
                                                        + "}"),
                                @ExampleObject(
                                        name = "overrides",
                                        value =
                                                "{\n"
                                                        + "    \"k3\": \"v1\",\n"
                                                        + "    \"k4\": \"v2\",\n"
                                                        + "}"),
                            },
                            mediaType = "application/json")
                }),
        @ApiResponse(
                responseCode = "500",
                content = {@Content(schema = @Schema())})
    })
    @GetMapping(ResourcePaths.V1_CONFIG)
    public ResponseEntity<ConfigResponse> getConfig() {
        try {
            Map<String, String> defaults = new HashMap<>();
            Map<String, String> overrides = new HashMap<>();
            ConfigResponse response = new ConfigResponse(defaults, overrides);
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Operation(
            summary = "List Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "201",
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
    public ResponseEntity<ListDatabasesResponse> listDatabases(@PathVariable String prefix) {
        try {
            ListDatabasesResponse response =
                    new ListDatabasesResponse(ImmutableList.of(new DatabaseName("account")));
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Operation(
            summary = "Create Databases",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "201",
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
    public ResponseEntity<CreateDatabaseResponse> createDatabases(
            @PathVariable String prefix, @RequestBody CreateDatabaseRequest request) {
        try {
            Map<String, String> properties = new HashMap<>();
            CreateDatabaseResponse response = new CreateDatabaseResponse("name", properties);
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @Operation(
            summary = "Get Database",
            tags = {"database"})
    @ApiResponses({
        @ApiResponse(
                responseCode = "201",
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
    public ResponseEntity<GetDatabaseResponse> getDatabases(
            @PathVariable String prefix, @PathVariable String database) {
        try {
            Map<String, String> options = new HashMap<>();
            GetDatabaseResponse response = new GetDatabaseResponse("name", options, "comment");
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
