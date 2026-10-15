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

package org.apache.paimon.rest.auth;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.function.Function;
import org.apache.paimon.function.FunctionImpl;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.ListPoliciesRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.view.SemanticViewDefinition;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_REGION;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_SIGNING_ALGORITHM;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;

/** Every request {@link RESTApi} sends with openapi-v4 must carry an x-acs-action. */
class DLFOpenApiActionsCoverageTest {

    /** RESTApi methods with no entry in DLFOpenApiActions; they are signed without an action. */
    private static final Set<String> WITHOUT_ACTION = Collections.emptySet();

    /** A value per parameter type; "SELECT" is valid both as a name and as an access. */
    private static final Map<Class<?>, Object> ARGUMENTS = arguments();

    private final List<String[]> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private RESTApi api;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/v1/",
                exchange -> {
                    requests.add(
                            new String[] {
                                exchange.getRequestMethod(),
                                exchange.getRequestURI().getRawPath(),
                                exchange.getRequestHeaders().getFirst("x-acs-action")
                            });
                    byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, data.length);
                    exchange.getResponseBody().write(data);
                    exchange.close();
                });
        server.start();

        Options options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(PREFIX, "clg-paimon-1");
        options.set(TOKEN_PROVIDER, "dlf");
        options.set(DLF_ACCESS_KEY_ID, "ak");
        options.set(DLF_ACCESS_KEY_SECRET, "sk");
        options.set(DLF_REGION, "cn-hangzhou");
        options.set(DLF_SIGNING_ALGORITHM, "openapi-v4");
        api = new RESTApi(options, false);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    /** A new REST call fails here until it is added to DLFOpenApiActions or WITHOUT_ACTION. */
    @Test
    void testEveryRequestCarriesAnAction() throws Exception {
        List<String> silent = new ArrayList<>();
        Map<String, List<String>> withoutAction = new TreeMap<>();
        for (Method method : RESTApi.class.getDeclaredMethods()) {
            if (!Modifier.isPublic(method.getModifiers())
                    || Modifier.isStatic(method.getModifiers())
                    || method.getName().equals("options")) {
                continue;
            }
            requests.clear();
            try {
                method.invoke(api, arguments(method));
            } catch (InvocationTargetException ignored) {
                // a reply of {} may not parse; only the request matters
            }
            if (requests.isEmpty()) {
                silent.add(method.toGenericString());
            }
            for (String[] request : requests) {
                if (request[2] == null) {
                    withoutAction
                            .computeIfAbsent(method.getName(), k -> new ArrayList<>())
                            .add(request[0] + " " + request[1]);
                }
            }
        }

        assertThat(silent).as("methods that sent no request; give them arguments").isEmpty();
        Map<String, List<String>> missing = new TreeMap<>(withoutAction);
        missing.keySet().removeAll(WITHOUT_ACTION);
        assertThat(missing)
                .as("requests without x-acs-action; map them in DLFOpenApiActions")
                .isEmpty();
        assertThat(withoutAction.keySet())
                .as("WITHOUT_ACTION entries that now resolve; remove them")
                .containsAll(WITHOUT_ACTION);
    }

    private static Object[] arguments(Method method) {
        Parameter[] parameters = method.getParameters();
        Object[] arguments = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            Class<?> type = parameters[i].getType();
            if (parameters[i].isAnnotationPresent(Nullable.class)) {
                arguments[i] = null;
            } else if (type.isEnum()) {
                arguments[i] = type.getEnumConstants()[0];
            } else {
                arguments[i] = ARGUMENTS.get(type);
            }
        }
        return arguments;
    }

    private static Map<Class<?>, Object> arguments() {
        PermissionResource table =
                new PermissionResource(ResourceType.TABLE, "db", "t", null, null);
        Map<Class<?>, Object> arguments = new HashMap<>();
        arguments.put(String.class, "SELECT");
        arguments.put(Integer.class, 1);
        arguments.put(int.class, 1);
        arguments.put(Long.class, 1L);
        arguments.put(long.class, 1L);
        arguments.put(boolean.class, false);
        arguments.put(Instant.class, Instant.EPOCH);
        arguments.put(List.class, Collections.emptyList());
        arguments.put(Map.class, Collections.emptyMap());
        arguments.put(Identifier.class, Identifier.create("db", "t"));
        arguments.put(
                Function.class, new FunctionImpl(Identifier.create("db", "f"), new HashMap<>()));
        arguments.put(SemanticViewDefinition.class, new SemanticViewDefinition("x1", "x1"));
        arguments.put(PermissionResource.class, table);
        arguments.put(
                PermissionAssignment.class, new PermissionAssignment(table, "SELECT", "x1", null));
        arguments.put(
                ListPermissionsRequest.class,
                new ListPermissionsRequest(
                        ResourceType.TABLE, "db", "t", null, null, null, null, null, null));
        arguments.put(
                ListPoliciesRequest.class,
                new ListPoliciesRequest(table, null, null, null, null, null));
        arguments.put(DataPolicy.class, DataPolicy.rowFilter(table, new RowFilter("x1"), "x1"));
        return arguments;
    }
}
