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

import org.apache.paimon.PagedList;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionIdentity;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.ForbiddenException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral tests for REST permission management. */
public class RESTPermissionManagementTest {

    private static final String BASE_PATH = "/v1/catalog+id/permissions";

    private HttpServer server;
    private PermissionManagement management;
    private final AtomicReference<String> grantBody = new AtomicReference<>();
    private final AtomicReference<String> revokeBody = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> listQuery = new AtomicReference<>();
    private final AtomicInteger revokeCalls = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/v1/",
                exchange -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    String path = exchange.getRequestURI().getRawPath();
                    if (BASE_PATH.equals(path) && "GET".equals(exchange.getRequestMethod())) {
                        listQuery.set(exchange.getRequestURI().getRawQuery());
                        respond(
                                exchange,
                                200,
                                "{\"permissions\":[{\"resource\":{\"type\":\"TABLE\","
                                        + "\"database\":\"sales\",\"table\":\"orders\"},"
                                        + "\"access\":\"SELECT\","
                                        + "\"principal\":\"analyst\"}],"
                                        + "\"nextPageToken\":\"next\"}");
                    } else if ((BASE_PATH + "/grant").equals(path)) {
                        String body = readBody(exchange);
                        grantBody.set(body);
                        if (body.contains("denied")) {
                            respond(exchange, 403, "{\"message\":\"forbidden\",\"code\":403}");
                        } else {
                            respond(exchange, 200, null);
                        }
                    } else if ((BASE_PATH + "/revoke").equals(path)) {
                        revokeBody.set(readBody(exchange));
                        revokeCalls.incrementAndGet();
                        respond(exchange, 200, null);
                    } else {
                        respond(exchange, 404, "{\"message\":\"missing\",\"code\":404}");
                    }
                });
        server.start();

        Options options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "secret");
        options.set(PREFIX, "catalog id");
        management = new RESTPermissionManagement(new RESTApi(options, false));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testListUsesEncodedPrefixAndCompleteFilters() throws Exception {
        PagedList<PermissionAssignment> page =
                management.listPermissions(
                        new ListPermissionsRequest(
                                ResourceType.TABLE,
                                "sales",
                                "orders",
                                null,
                                null,
                                "analyst",
                                null,
                                "start",
                                25));

        assertThat(page.getElements()).hasSize(1);
        assertThat(page.getElements().get(0).getPrincipal()).isEqualTo("analyst");
        assertThat(page.getNextPageToken()).isEqualTo("next");
        assertThat(queryParameters(listQuery.get()))
                .containsEntry("principal", "analyst")
                .containsEntry("resourceType", "TABLE")
                .containsEntry("database", "sales")
                .containsEntry("table", "orders")
                .containsEntry("maxResults", "25")
                .containsEntry("pageToken", "start");
        assertThat(authorization.get()).isEqualTo("Bearer secret");
    }

    @Test
    void testGrantAndRevokeUseStructuredWireShapes() throws Exception {
        PermissionAssignment assignment = assignment("analyst");
        management.grantPermission(assignment);
        management.revokePermission(PermissionIdentity.fromAssignment(assignment));

        Map<?, ?> grant = RESTApi.fromJson(grantBody.get(), Map.class);
        Map<?, ?> grantResource = (Map<?, ?>) grant.get("resource");
        assertThat(grantResource.get("type")).isEqualTo("TABLE");
        assertThat(grantResource.get("database")).isEqualTo("sales");
        assertThat(grantResource.get("table")).isEqualTo("orders");
        assertThat(grant.get("principal")).isEqualTo("analyst");
        assertThat(grant.containsKey("columns")).isFalse();
        assertThat(grant.containsKey("policy")).isFalse();
        assertThat(grant.containsKey("grantOption")).isFalse();
        assertThat(grant.containsKey("catalog")).isFalse();

        Map<?, ?> revoke = RESTApi.fromJson(revokeBody.get(), Map.class);
        Map<?, ?> revokeResource = (Map<?, ?>) revoke.get("resource");
        assertThat(revokeResource.get("type")).isEqualTo("TABLE");
        assertThat(revokeResource.get("database")).isEqualTo("sales");
        assertThat(revokeResource.get("table")).isEqualTo("orders");
        assertThat(revoke.get("principal")).isEqualTo("analyst");
        assertThat(revoke.containsKey("expireTime")).isFalse();
        assertThat(revoke.containsKey("grantOption")).isFalse();
    }

    @Test
    void testForbiddenGrantPreservesRESTErrorTranslation() {
        assertThatThrownBy(() -> management.grantPermission(assignment("denied")))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("forbidden");
    }

    @Test
    void testRepeatedRevokeIsIdempotent() {
        PermissionAssignment assignment = assignment("missing");
        PermissionIdentity identity = PermissionIdentity.fromAssignment(assignment);
        management.revokePermission(identity);
        management.revokePermission(identity);

        assertThat(revokeCalls).hasValue(2);
    }

    private static PermissionAssignment assignment(String principal) {
        return new PermissionAssignment(
                new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null),
                "SELECT",
                principal,
                null);
    }

    private static Map<String, String> queryParameters(String query) throws Exception {
        Map<String, String> values = new HashMap<>();
        for (String parameter : query.split("&")) {
            String[] pair = parameter.split("=", 2);
            values.put(URLDecoder.decode(pair[0], "UTF-8"), URLDecoder.decode(pair[1], "UTF-8"));
        }
        return values;
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        byte[] data = new byte[8192];
        int read = exchange.getRequestBody().read(data);
        return read < 0 ? "" : new String(data, 0, read, StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int code, String body) throws IOException {
        if (body == null) {
            exchange.sendResponseHeaders(code, 0);
            exchange.getResponseBody().close();
        } else {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(code, bytes.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(bytes);
            }
            exchange.close();
        }
    }
}
