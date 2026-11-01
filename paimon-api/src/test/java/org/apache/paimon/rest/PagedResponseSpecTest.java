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

import org.apache.paimon.rest.responses.PagedResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.MissingNode;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/** The last page of every {@link PagedResponse} must be valid against the OpenAPI spec. */
class PagedResponseSpecTest {

    private static final String NEXT_PAGE_TOKEN = "nextPageToken";

    @Test
    void testLastPagesMatchTheOpenApiSpec() throws Exception {
        Map<String, JsonNode> schemas = OpenApiSpec.schemas();
        List<Class<?>> responses = pagedResponses();
        assertThat(responses).isNotEmpty();

        Map<String, String> rejected = new TreeMap<>();
        for (Class<?> response : responses) {
            String name = response.getSimpleName();
            JsonNode token =
                    schemas.getOrDefault(name, MissingNode.getInstance())
                            .path("properties")
                            .path(NEXT_PAGE_TOKEN);
            String lastPage = RESTApi.toJson(RESTApi.fromJson("{}", response));
            if (token.isMissingNode()) {
                rejected.put(name, "the spec has no " + name + "." + NEXT_PAGE_TOKEN);
            } else if (new ObjectMapper().readTree(lastPage).path(NEXT_PAGE_TOKEN).isNull()
                    && !allowsNull(token)) {
                rejected.put(name, "sends a null " + NEXT_PAGE_TOKEN + " the spec does not allow");
            }
        }
        assertThat(rejected).as("last pages the OpenAPI spec rejects").isEmpty();
    }

    private static boolean allowsNull(JsonNode schema) {
        if (schema.path("nullable").asBoolean(false)) {
            return true;
        }
        for (JsonNode type : schema.path("type")) {
            if ("null".equals(type.asText())) {
                return true;
            }
        }
        return "null".equals(schema.path("type").asText());
    }

    /** Every concrete {@link PagedResponse} next to it on the class path. */
    private static List<Class<?>> pagedResponses() throws Exception {
        String suffix = ".class";
        File directory =
                new File(PagedResponse.class.getResource("PagedResponse" + suffix).toURI())
                        .getParentFile();
        List<Class<?>> responses = new ArrayList<>();
        for (String file : directory.list()) {
            if (!file.endsWith(suffix) || file.contains("$")) {
                continue;
            }
            Class<?> type =
                    Class.forName(
                            PagedResponse.class.getPackage().getName()
                                    + "."
                                    + file.substring(0, file.length() - suffix.length()));
            if (PagedResponse.class.isAssignableFrom(type)
                    && !Modifier.isAbstract(type.getModifiers())) {
                responses.add(type);
            }
        }
        return responses;
    }
}
