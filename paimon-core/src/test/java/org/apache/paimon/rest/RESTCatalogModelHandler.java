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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Model;
import org.apache.paimon.rest.requests.CreateModelRequest;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetModelResponse;
import org.apache.paimon.rest.responses.ListModelsResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.MockResponse;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.paimon.rest.RESTApi.MAX_RESULTS;
import static org.apache.paimon.rest.RESTApi.PAGE_TOKEN;
import static org.apache.paimon.rest.RESTCatalogServerUtils.parseRequest;

/** Stateful model endpoints used by {@link RESTCatalogServer}. */
final class RESTCatalogModelHandler {

    private final Map<String, Database> databaseStore;
    private final Map<String, Object> databaseLifecycleLocks;
    private final Map<String, Model> modelStore = new ConcurrentHashMap<>();

    RESTCatalogModelHandler(
            Map<String, Database> databaseStore, Map<String, Object> databaseLifecycleLocks) {
        this.databaseStore = databaseStore;
        this.databaseLifecycleLocks = databaseLifecycleLocks;
    }

    MockResponse handleCollection(
            String databaseName, String method, String data, Map<String, String> parameters)
            throws Exception {
        switch (method) {
            case "GET":
                return list(databaseName, parameters);
            case "POST":
                return create(databaseName, data);
            default:
                return new MockResponse().setResponseCode(404);
        }
    }

    MockResponse handleModel(Identifier identifier, String method) throws Exception {
        synchronized (databaseLifecycleLock(identifier.getDatabaseName())) {
            requireDatabase(identifier.getDatabaseName());
            Model model = modelStore.get(identifier.getFullName());
            if (model == null) {
                Catalog.ModelNotExistException exception =
                        new Catalog.ModelNotExistException(identifier);
                return error(
                        ErrorResponse.RESOURCE_TYPE_MODEL,
                        identifier.getObjectName(),
                        exception.getMessage(),
                        404);
            }
            switch (method) {
                case "GET":
                    return response(toResponse(model), 200);
                case "DELETE":
                    modelStore.remove(identifier.getFullName());
                    return new MockResponse().setResponseCode(200);
                default:
                    return new MockResponse().setResponseCode(404);
            }
        }
    }

    void removeDatabase(String databaseName) {
        modelStore
                .entrySet()
                .removeIf(
                        entry ->
                                entry.getValue()
                                        .identifier()
                                        .getDatabaseName()
                                        .equals(databaseName));
    }

    private MockResponse list(String databaseName, Map<String, String> parameters) {
        int maxResults = maxResults(parameters);
        String pageToken = parameters.get(PAGE_TOKEN);
        List<GetModelResponse> models =
                modelStore.values().stream()
                        .filter(model -> model.identifier().getDatabaseName().equals(databaseName))
                        .map(RESTCatalogModelHandler::toResponse)
                        .sorted(Comparator.comparing(GetModelResponse::getName))
                        .filter(
                                model ->
                                        pageToken == null
                                                || model.getName().compareTo(pageToken) > 0)
                        .limit(maxResults)
                        .collect(Collectors.toList());
        String nextPageToken =
                models.size() < maxResults ? null : models.get(models.size() - 1).getName();
        return response(new ListModelsResponse(models, nextPageToken), 200);
    }

    private MockResponse create(String databaseName, String data) throws Exception {
        CreateModelRequest request = parseRequest(data, CreateModelRequest.class);
        Identifier identifier = Identifier.create(databaseName, request.getName());
        synchronized (databaseLifecycleLock(databaseName)) {
            requireDatabase(databaseName);
            if (modelStore.containsKey(identifier.getFullName())) {
                Catalog.ModelAlreadyExistException exception =
                        new Catalog.ModelAlreadyExistException(identifier);
                return error(
                        ErrorResponse.RESOURCE_TYPE_MODEL,
                        identifier.getObjectName(),
                        exception.getMessage(),
                        409);
            }
            Model model = new Model(identifier, request.getComment(), request.getDefinition());
            modelStore.put(identifier.getFullName(), model);
            return response(toResponse(model), 200);
        }
    }

    private void requireDatabase(String databaseName) throws Catalog.DatabaseNotExistException {
        if (!databaseStore.containsKey(databaseName)) {
            throw new Catalog.DatabaseNotExistException(databaseName);
        }
    }

    private Object databaseLifecycleLock(String databaseName) {
        return databaseLifecycleLocks.computeIfAbsent(databaseName, ignored -> new Object());
    }

    private static int maxResults(Map<String, String> parameters) {
        String value = parameters.get(MAX_RESULTS);
        int maxResults =
                value == null ? RESTCatalogServer.DEFAULT_MAX_RESULTS : Integer.parseInt(value);
        return maxResults <= 0
                ? RESTCatalogServer.DEFAULT_MAX_RESULTS
                : Math.min(maxResults, RESTCatalogServer.DEFAULT_MAX_RESULTS);
    }

    private static GetModelResponse toResponse(Model model) {
        return new GetModelResponse(model.name(), model.comment(), model.definition());
    }

    private static MockResponse error(
            String resourceType, String resourceName, String message, int status) {
        return response(new ErrorResponse(resourceType, resourceName, message, status), status);
    }

    private static MockResponse response(RESTResponse response, int status) {
        try {
            return new MockResponse()
                    .setResponseCode(status)
                    .setBody(RESTApi.toJson(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
