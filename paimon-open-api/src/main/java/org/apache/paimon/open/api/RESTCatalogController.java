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

import org.apache.paimon.rest.requests.ConfigRequest;
import org.apache.paimon.rest.responses.ConfigResponse;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/** * RESTCatalog management APIs. */
@CrossOrigin(origins = "http://localhost:8081")
@RestController
@RequestMapping("/api/v1/{prefix}")
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
    @PostMapping("/config")
    public ResponseEntity<ConfigResponse> getConfig(
            @PathVariable String prefix, @RequestBody ConfigRequest request) {
        try {
            Map<String, String> defaults = new HashMap<>();
            ConfigResponse response = new ConfigResponse(defaults);
            return new ResponseEntity<>(response, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
