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
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.DatabaseName;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
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
    public ResponseEntity<ListDatabasesResponse> listDatabases(String prefix) {
        try {
            ListDatabasesResponse response =
                    new ListDatabasesResponse(ImmutableList.of(new DatabaseName("account")));
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
