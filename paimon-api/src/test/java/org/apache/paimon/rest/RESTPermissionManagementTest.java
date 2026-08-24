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
import org.apache.paimon.management.Permission;
import org.apache.paimon.management.PermissionManagement;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral tests for REST permission management. */
public class RESTPermissionManagementTest {

    private static final String BASE_PATH = "/v1/catalogs/catalog+name/permissions";

    private HttpServer server;
    private PermissionManagement management;
    private final AtomicReference<String> grantBody = new AtomicReference<>();
    private final AtomicReference<String> revokeBody = new AtomicReference<>();
    private final AtomicReference<String> authorization = new AtomicReference<>();
    private final AtomicReference<String> listQuery = new AtomicReference<>();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/v1/catalogs/",
                exchange -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    String path = exchange.getRequestURI().getRawPath();
                    if (BASE_PATH.equals(path) && "GET".equals(exchange.getRequestMethod())) {
                        listQuery.set(exchange.getRequestURI().getRawQuery());
                        respond(
                                exchange,
                                200,
                                "{\"permissions\":[{\"resourceType\":\"TABLE\","
                                        + "\"database\":\"sales\",\"table\":\"orders\","
                                        + "\"access\":\"SELECT\","
                                        + "\"principal\":\"role:analyst\"}],"
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
                        String body = readBody(exchange);
                        revokeBody.set(body);
                        if (body.contains("missing")) {
                            respond(
                                    exchange,
                                    404,
                                    "{\"message\":\"permission does not exist\","
                                            + "\"resourceType\":\"PERMISSION\","
                                            + "\"resourceName\":\"missing\",\"code\":404}");
                        } else {
                            respond(exchange, 200, null);
                        }
                    } else {
                        respond(exchange, 404, "{\"message\":\"missing\",\"code\":404}");
                    }
                });
        server.start();

        Options options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "secret");
        options.set(PREFIX, "unused");
        management = new RESTPermissionManagement(new RESTApi(options, false), "catalog name");
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testListUsesEncodedCatalogAndCompleteFilters() throws Exception {
        PagedList<Permission> page =
                management.listPermissions(
                        new ListPermissionsRequest(
                                ResourceType.TABLE,
                                "sales",
                                "orders",
                                null,
                                null,
                                "role:analyst",
                                "start",
                                25));

        assertThat(page.getElements()).hasSize(1);
        assertThat(page.getElements().get(0).getPrincipal()).isEqualTo("role:analyst");
        assertThat(page.getNextPageToken()).isEqualTo("next");
        assertThat(queryParameters(listQuery.get()))
                .containsEntry("principal", "role:analyst")
                .containsEntry("resourceType", "TABLE")
                .containsEntry("database", "sales")
                .containsEntry("table", "orders")
                .containsEntry("maxResults", "25")
                .containsEntry("pageToken", "start");
        assertThat(authorization.get()).isEqualTo("Bearer secret");
    }

    @Test
    void testGrantAndRevokeUseFlatWireShapes() throws Exception {
        Permission permission = permission("role:analyst");
        management.grantPermission(permission);
        management.revokePermission(permission);

        Map<?, ?> grant = RESTApi.fromJson(grantBody.get(), Map.class);
        assertThat(grant.get("resourceType")).isEqualTo("TABLE");
        assertThat(grant.get("database")).isEqualTo("sales");
        assertThat(grant.get("table")).isEqualTo("orders");
        assertThat(grant.get("access")).isEqualTo("SELECT");
        assertThat(grant.get("principal")).isEqualTo("role:analyst");
        assertThat(grant.containsKey("permission")).isFalse();

        Map<?, ?> revoke = RESTApi.fromJson(revokeBody.get(), Map.class);
        assertThat(revoke.get("resourceType")).isEqualTo("TABLE");
        assertThat(revoke.get("database")).isEqualTo("sales");
        assertThat(revoke.get("table")).isEqualTo("orders");
        assertThat(revoke.get("access")).isEqualTo("SELECT");
        assertThat(revoke.get("principal")).isEqualTo("role:analyst");
        assertThat(revoke.containsKey("expireTime")).isFalse();
    }

    @Test
    void testForbiddenGrantPreservesRESTErrorTranslation() {
        assertThatThrownBy(() -> management.grantPermission(permission("denied")))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("forbidden");
    }

    @Test
    void testMissingRevokeIsNotSilentlyIdempotent() {
        assertThatThrownBy(() -> management.revokePermission(permission("missing")))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("permission does not exist");
    }

    private static Permission permission(String principal) {
        return new Permission(
                ResourceType.TABLE,
                null,
                "sales",
                "orders",
                null,
                null,
                null,
                null,
                null,
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
        }
        exchange.close();
    }
}
