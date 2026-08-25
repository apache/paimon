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
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPoliciesRequest;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PolicyManagement;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.utils.JsonSerdeUtil;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Behavioral tests for REST data-policy management. */
public class RESTPolicyManagementTest {

    private static final String COLLECTION_PATH =
            "/v1/catalog+id/databases/sales/tables/orders/policies";

    private HttpServer server;
    private PolicyManagement management;
    private final AtomicReference<String> listQuery = new AtomicReference<>();
    private final AtomicReference<String> createBody = new AtomicReference<>();
    private final AtomicReference<String> updateBody = new AtomicReference<>();
    private final AtomicReference<String> deleteBody = new AtomicReference<>();
    private final AtomicReference<String> deleteError = new AtomicReference<>();
    private final AtomicInteger deleteCalls = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/v1/",
                exchange -> {
                    String path = exchange.getRequestURI().getRawPath();
                    String method = exchange.getRequestMethod();
                    if (COLLECTION_PATH.equals(path) && "GET".equals(method)) {
                        listQuery.set(exchange.getRequestURI().getRawQuery());
                        respond(exchange, 200, listResponse());
                    } else if (COLLECTION_PATH.equals(path) && "POST".equals(method)) {
                        createBody.set(readBody(exchange));
                        respond(exchange, 200, null);
                    } else if (COLLECTION_PATH.equals(path) && "PUT".equals(method)) {
                        updateBody.set(readBody(exchange));
                        respond(exchange, 200, null);
                    } else if (COLLECTION_PATH.equals(path) && "DELETE".equals(method)) {
                        deleteBody.set(readBody(exchange));
                        deleteCalls.incrementAndGet();
                        if (deleteError.get() == null) {
                            respond(exchange, 200, null);
                        } else {
                            respond(exchange, 404, deleteError.get());
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
        options.set(PREFIX, "catalog id");
        management = new RESTPolicyManagement(new RESTApi(options, false));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testListUsesResourceNestedPathAndIdentityFilters() {
        PagedList<DataPolicy> policies =
                management.listPolicies(
                        new ListPoliciesRequest(
                                tableResource(),
                                PolicyType.COLUMN_MASKING,
                                "analyst",
                                "email",
                                "start",
                                25));

        assertThat(policies.getElements()).hasSize(1);
        assertThat(policies.getNextPageToken()).isEqualTo("next");
        assertThat(listQuery.get())
                .contains("type=COLUMN_MASKING")
                .contains("principal=analyst")
                .contains("column=email")
                .contains("maxResults=25")
                .contains("pageToken=start");
    }

    @Test
    void testCreateUpdateAndDropUseCrudMethods() {
        DataPolicy policy = policy();
        management.createPolicy(policy);
        management.createOrReplacePolicy(policy);
        management.dropPolicy(
                policy.getResource(),
                policy.type(),
                policy.getPrincipal(),
                policy.getColumnMask().getOnColumn(),
                false);

        assertThat(createBody.get()).contains("\"principal\":\"analyst\"");
        assertThat(createBody.get()).doesNotContain("\"resource\"");
        assertThat(updateBody.get()).contains("\"columnMask\"");
        assertThat(deleteBody.get())
                .contains("\"type\":\"COLUMN_MASKING\"")
                .contains("\"column\":\"email\"");
        assertThat(deleteCalls).hasValue(1);
    }

    @Test
    void testDropIfExistsOnlyIgnoresMissingPolicy() {
        DataPolicy policy = policy();
        deleteError.set(
                "{\"resourceType\":\"POLICY\",\"resourceName\":"
                        + "\"COLUMN_MASKING:analyst:email\","
                        + "\"message\":\"missing\",\"code\":404}");

        management.dropPolicy(
                policy.getResource(),
                policy.type(),
                policy.getPrincipal(),
                policy.getColumnMask().getOnColumn(),
                true);

        deleteError.set(
                "{\"resourceType\":\"TABLE\",\"resourceName\":\"orders\","
                        + "\"message\":\"missing table\",\"code\":404}");
        assertThatThrownBy(
                        () ->
                                management.dropPolicy(
                                        policy.getResource(),
                                        policy.type(),
                                        policy.getPrincipal(),
                                        policy.getColumnMask().getOnColumn(),
                                        true))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("missing table");
    }

    private static DataPolicy policy() {
        return DataPolicy.columnMask(
                tableResource(),
                new ColumnMask(
                        "email",
                        "{\"name\":\"FIELD_REF\",\"fieldRef\":{\"index\":0,"
                                + "\"name\":\"region\",\"type\":\"STRING\"}}"),
                "analyst");
    }

    private static PermissionResource tableResource() {
        return new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null);
    }

    private static String listResponse() {
        return "{\"policies\":[" + policyJson() + "],\"nextPageToken\":\"next\"}";
    }

    private static String policyJson() {
        return JsonSerdeUtil.toFlatJson(policy());
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
