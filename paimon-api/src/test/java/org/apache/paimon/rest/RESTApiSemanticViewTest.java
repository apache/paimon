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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.RESTException;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.requests.UpsertSemanticViewRequest;
import org.apache.paimon.rest.responses.GetSemanticViewResponse;
import org.apache.paimon.rest.responses.ListSemanticViewsResponse;
import org.apache.paimon.view.SemanticViewDefinition;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** HTTP contract tests for semantic view definition management. */
class RESTApiSemanticViewTest {

    private static final String BASE_PATH = "/v1/catalog%2Fid/databases/sales/semantic-views";
    private static final Identifier IDENTIFIER = Identifier.create("sales", "revenue");
    private static final SemanticViewDefinition DEFINITION =
            new SemanticViewDefinition(
                    "provider-v2-yaml",
                    "# 指标\r\nversion: '1.1'\nsource: db.orders\nfuture: {expr: \"a + b\\c\"}\n");

    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private RESTApi api;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/v1/",
                exchange -> {
                    requests.add(new Request(exchange));
                    Reply reply = replies.poll();
                    if (reply == null) {
                        reply = new Reply(500, "{\"code\":500,\"message\":\"unexpected request\"}");
                    }
                    byte[] data = reply.body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(reply.code, data.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(data);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();

        Options options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(PREFIX, "catalog/id");
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "test-token");
        api = new RESTApi(options, false);
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testCompleteDefinitionUpsertAndReadPreserveText() throws Exception {
        for (String revision : Arrays.asList(null, "r17")) {
            enqueue(200, responseJson());
            GetSemanticViewResponse result =
                    api.upsertSemanticView(IDENTIFIER, DEFINITION, revision);
            assertThat(result.getDefinition()).isEqualTo(DEFINITION);
            assertThat(result.getRevision()).isEqualTo("r18");
            assertThat(result.getName()).isEqualTo("revenue");
            assertThat(result.getEntityName()).isEqualTo("opaque/view:123");
        }
        enqueue(200, responseJson());
        assertThat(api.getSemanticView(IDENTIFIER).getDefinition()).isEqualTo(DEFINITION);
        assertThat(requests).hasSize(3);
        for (int i = 0; i < 2; i++) {
            Request request = requests.get(i);
            assertThat(request.method).isEqualTo("POST");
            assertThat(request.path).isEqualTo(BASE_PATH + "/revenue");
            assertThat(request.query).isNull();
            assertThat(request.authorization).isEqualTo("Bearer test-token");
            Map<?, ?> body = RESTApi.fromJson(request.body, Map.class);
            assertThat(body.keySet()).hasSize(i == 0 ? 1 : 2);
            Map<String, String> definition = (Map<String, String>) body.get("definition");
            assertThat(definition)
                    .containsOnlyKeys("format", "content")
                    .containsEntry("format", "provider-v2-yaml")
                    .containsEntry("content", DEFINITION.getContent());
            assertThat(body.get("expectedRevision")).isEqualTo(i == 0 ? null : "r17");
        }
        assertThat(requests.get(2).method).isEqualTo("GET");
    }

    @Test
    void testIndependentPathEncodingIncludingDotSegments() throws Exception {
        Identifier id = Identifier.create("sales /华+%?#", "model.a/b+% ?#");
        enqueue(200, responseJson());
        api.upsertSemanticView(id, DEFINITION);
        enqueue(200, responseJson());
        api.getSemanticView(id);
        enqueue(200, "");
        api.deleteSemanticView(id);
        String path =
                "/v1/catalog%2Fid/databases/sales+%2F%E5%8D%8E%2B%25%3F%23/semantic-views/model.a%2Fb%2B%25+%3F%23";
        assertThat(requests).extracting(r -> r.path).containsExactly(path, path, path);
        enqueue(200, responseJson());
        api.getSemanticView(Identifier.create(".", ".."));
        assertThat(requests.get(3).path)
                .isEqualTo("/v1/catalog%2Fid/databases/%2E/semantic-views/%2E%2E");
    }

    @Test
    void testPagedAndAllNamesPreserveOpaqueTokens() throws Exception {
        enqueue(200, "{\"semanticViews\":[\"one\"],\"nextPageToken\":\"next +/%?&\"}");
        PagedList<String> page = api.listSemanticViewsPaged("sales", 1000, "start +/%");
        assertThat(page.getElements()).containsExactly("one");
        assertThat(page.getNextPageToken()).isEqualTo("next +/%?&");
        assertThat(queryParameters(requests.get(0).query))
                .containsEntry("maxResults", "1000")
                .containsEntry("pageToken", "start +/%");
        enqueue(200, "{\"semanticViews\":[\"one\"],\"nextPageToken\":\"next +/%?&\"}");
        enqueue(200, "{\"semanticViews\":[\"two\"]}");
        assertThat(api.listSemanticViews("sales")).containsExactly("one", "two");
        assertThat(requests.get(1).query).isNull();
        assertThat(queryParameters(requests.get(2).query))
                .containsOnlyKeys("pageToken")
                .containsEntry("pageToken", "next +/%?&");
        assertThat(requests)
                .allSatisfy(
                        r -> {
                            assertThat(r.method).isEqualTo("GET");
                            assertThat(r.path).isEqualTo(BASE_PATH);
                        });
        // Even a malformed empty page with a continuation token must not keep fetching.
        enqueue(200, "{\"semanticViews\":[],\"nextPageToken\":\"unused\"}");
        assertThat(api.listSemanticViews("sales")).isEmpty();
        assertThat(requests).hasSize(4);
    }

    @Test
    void testDeleteConditionIsAQueryParameterWithoutBody() throws Exception {
        enqueue(200, "");
        api.deleteSemanticView(IDENTIFIER, "r +/%?&");
        enqueue(200, "");
        api.deleteSemanticView(IDENTIFIER);
        assertThat(queryParameters(requests.get(0).query))
                .containsOnlyKeys("expectedRevision")
                .containsEntry("expectedRevision", "r +/%?&");
        assertThat(requests.get(1).query).isNull();
        assertThat(requests)
                .allSatisfy(
                        r -> {
                            assertThat(r.method).isEqualTo("DELETE");
                            assertThat(r.path).isEqualTo(BASE_PATH + "/revenue");
                            assertThat(r.body).isEmpty();
                        });
    }

    @Test
    void testDeleteQueryIsIncludedInAuthentication() throws Exception {
        enqueue(200, "");
        List<RESTAuthParameter> authInputs = new java.util.ArrayList<>();
        RESTAuthFunction auth =
                new RESTAuthFunction(Collections.emptyMap(), null) {
                    @Override
                    public Map<String, String> apply(RESTAuthParameter input) {
                        authInputs.add(input);
                        return Collections.singletonMap("Authorization", "signed");
                    }
                };
        new HttpClient("http://127.0.0.1:" + server.getAddress().getPort())
                .delete(
                        BASE_PATH + "/revenue",
                        Collections.singletonMap("expectedRevision", "r +/%?&"),
                        null,
                        auth);
        assertThat(authInputs).hasSize(1);
        RESTAuthParameter input = authInputs.get(0);
        assertThat(input.resourcePath()).isEqualTo(BASE_PATH + "/revenue");
        assertThat(input.method()).isEqualTo("DELETE");
        assertThat(input.data()).isNull();
        assertThat(input.parameters())
                .containsOnlyKeys("expectedRevision")
                .containsEntry("expectedRevision", "r+%2B%2F%25%3F%26");
        assertThat(requests.get(0).authorization).isEqualTo("signed");
        assertThat(queryParameters(requests.get(0).query).get("expectedRevision"))
                .isEqualTo("r +/%?&");
    }

    @Test
    void testMissingObjectsPropagateWithoutFallback() {
        List<Consumer<RESTApi>> operations =
                Arrays.asList(
                        client -> client.upsertSemanticView(IDENTIFIER, DEFINITION, "old"),
                        client -> client.getSemanticView(IDENTIFIER),
                        client -> client.listSemanticViews("sales"),
                        client -> client.listSemanticViewsPaged("sales", 1, null),
                        client -> client.deleteSemanticView(IDENTIFIER, "old"));
        for (Consumer<RESTApi> operation : operations) {
            enqueue(
                    404,
                    "{\"code\":404,\"resourceType\":\"SEMANTIC_VIEW\",\"message\":\"missing model\"}");
            assertThatThrownBy(() -> operation.accept(api))
                    .isInstanceOf(NoSuchResourceException.class)
                    .hasMessageContaining("missing model");
        }
        assertThat(requests).hasSize(operations.size());
    }

    @Test
    void testConflictsNeverBecomeUnconditionalWrites() throws Exception {
        enqueue(409, "{\"code\":409,\"message\":\"stale revision\"}");
        assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, DEFINITION, "old"))
                .isInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("stale revision");
        enqueue(409, "{\"code\":409,\"message\":\"stale revision\"}");
        assertThatThrownBy(() -> api.deleteSemanticView(IDENTIFIER, "old"))
                .isInstanceOf(AlreadyExistsException.class);
        assertThat(requests).hasSize(2);
        assertThat(
                        RESTApi.fromJson(requests.get(0).body, UpsertSemanticViewRequest.class)
                                .getExpectedRevision())
                .isEqualTo("old");
        assertThat(queryParameters(requests.get(1).query)).containsEntry("expectedRevision", "old");
    }

    @Test
    void testConditionalUpsertDoesNotAutomaticallyReplayAfterServiceFailure() {
        enqueue(503, "{\"code\":503,\"message\":\"temporarily unavailable\"}");
        assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, DEFINITION, "old"))
                .isInstanceOf(ServiceUnavailableException.class);
        assertThat(requests).hasSize(1);
    }

    @Test
    void testForbiddenUnsupportedAndOversizedResponsesHaveNoFallback() {
        enqueue(403, "{\"code\":403,\"message\":\"denied\"}");
        assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, DEFINITION))
                .isInstanceOf(ForbiddenException.class);
        enqueue(501, "{\"code\":501,\"message\":\"unsupported\"}");
        assertThatThrownBy(() -> api.getSemanticView(IDENTIFIER))
                .isInstanceOf(NotImplementedException.class);
        enqueue(413, "{\"code\":413,\"message\":\"too large\"}");
        assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, DEFINITION))
                .isInstanceOf(RESTException.class)
                .hasMessageContaining("too large");
        assertThat(requests).hasSize(3);
    }

    @Test
    void testInvalidInputsAndUtf8SizeBoundaryBeforeHttp() {
        for (String invalid : Arrays.asList(null, "", "  ")) {
            assertThatThrownBy(() -> new SemanticViewDefinition(invalid, "content"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new SemanticViewDefinition("provider-yaml", invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.listSemanticViews(invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        for (String invalid : Arrays.asList("", " ")) {
            assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, DEFINITION, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.deleteSemanticView(IDENTIFIER, invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.getSemanticView(Identifier.create("sales", invalid)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> api.getSemanticView(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.upsertSemanticView(null, DEFINITION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.deleteSemanticView(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> api.upsertSemanticView(IDENTIFIER, null))
                .isInstanceOf(IllegalArgumentException.class);
        for (int invalid : new int[] {-1, 0, 1001}) {
            assertThatThrownBy(() -> api.listSemanticViewsPaged("sales", invalid, null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        char[] chars = new char[SemanticViewDefinition.MAX_CONTENT_BYTES / 3];
        Arrays.fill(chars, '华');
        String limit = new String(chars) + "x";
        assertThat(limit.getBytes(StandardCharsets.UTF_8))
                .hasSize(SemanticViewDefinition.MAX_CONTENT_BYTES);
        assertThat(new SemanticViewDefinition("provider-yaml", limit).getContent())
                .isEqualTo(limit);
        assertThatThrownBy(() -> new SemanticViewDefinition("provider-yaml", limit + "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("1 MiB");
        assertThat(requests).isEmpty();
    }

    @Test
    void testIncompleteResponsesCannotRemoveARevisionCondition() throws Exception {
        for (String field : Arrays.asList("name", "entityName", "definition", "revision")) {
            Map<String, Object> response = RESTApi.fromJson(responseJson(), Map.class);
            response.remove(field);
            enqueue(200, RESTApi.toJson(response));
            assertThatThrownBy(() -> api.getSemanticView(IDENTIFIER))
                    .isInstanceOf(RESTException.class);
        }
        for (String revision : Arrays.asList(null, "", " ")) {
            Map<String, Object> response = RESTApi.fromJson(responseJson(), Map.class);
            response.put("revision", revision);
            enqueue(200, RESTApi.toJson(response));
            assertThatThrownBy(() -> api.getSemanticView(IDENTIFIER))
                    .isInstanceOf(RESTException.class);
        }
        assertThat(requests).hasSize(7);
    }

    @Test
    void testModelFormatsUseTwoFieldWireDefinitions() throws Exception {
        for (String format :
                Arrays.asList("databricks-yaml", "snowflake-yaml", "ossie-yaml", "provider-json")) {
            // The REST layer transports provider-defined text without interpreting its grammar.
            SemanticViewDefinition definition =
                    new SemanticViewDefinition(format, DEFINITION.getContent());
            enqueue(
                    200,
                    RESTApi.toJson(
                            new GetSemanticViewResponse(
                                    "revenue", "opaque/view:123", definition, "r18")));
            assertThat(api.upsertSemanticView(IDENTIFIER, definition).getDefinition())
                    .isEqualTo(definition);
            String body = requests.get(requests.size() - 1).body;
            Map<?, ?> request = RESTApi.fromJson(body, Map.class);
            Map<String, String> fields = (Map<String, String>) request.get("definition");
            assertThat(fields)
                    .containsOnlyKeys("format", "content")
                    .containsEntry("format", format)
                    .containsEntry("content", DEFINITION.getContent());
            UpsertSemanticViewRequest external =
                    new ObjectMapper().readValue(body, UpsertSemanticViewRequest.class);
            assertThat(external.getDefinition()).isEqualTo(definition);
        }
        assertThat(requests).hasSize(4);
    }

    @Test
    void testNestedJacksonCompatibilityAndRequiredFields() throws Exception {
        String json = RESTApi.toJson(new UpsertSemanticViewRequest(DEFINITION, "r17"));
        UpsertSemanticViewRequest external =
                new ObjectMapper().readValue(json, UpsertSemanticViewRequest.class);
        assertThat(external.getDefinition()).isEqualTo(DEFINITION);
        assertThat(external.getExpectedRevision()).isEqualTo("r17");
        assertThat(
                        RESTApi.fromJson(RESTApi.toJson(external), UpsertSemanticViewRequest.class)
                                .getDefinition())
                .isEqualTo(DEFINITION);
        assertThat(
                        RESTApi.fromJson(
                                RESTApi.toJson(new UpsertSemanticViewRequest(DEFINITION, null)),
                                Map.class))
                .containsOnlyKeys("definition");
        for (String invalid :
                Arrays.asList(
                        "{}",
                        "{\"definition\":null}",
                        "{\"definition\":{}}",
                        "{\"definition\":{\"format\":\"databricks-yaml\"}}",
                        "{\"definition\":{\"content\":\"source: orders\"}}")) {
            assertThatThrownBy(() -> RESTApi.fromJson(invalid, UpsertSemanticViewRequest.class))
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
        }
        GetSemanticViewResponse response =
                RESTApi.fromJson(
                        responseJson().replace("\"name\":", "\"future\":true,\"name\":"),
                        GetSemanticViewResponse.class);
        assertThat(response.getDefinition()).isEqualTo(DEFINITION);
        ListSemanticViewsResponse empty =
                RESTApi.fromJson("{\"future\":true}", ListSemanticViewsResponse.class);
        assertThat(empty.getSemanticViews()).isEmpty();
        assertThat(empty.getNextPageToken()).isNull();
        assertThat(RESTApi.fromJson(RESTApi.toJson(empty), Map.class))
                .containsOnlyKeys("semanticViews");
    }

    private static String responseJson() throws Exception {
        return RESTApi.toJson(
                new GetSemanticViewResponse("revenue", "opaque/view:123", DEFINITION, "r18"));
    }

    private void enqueue(int code, String body) {
        replies.add(new Reply(code, body));
    }

    private static Map<String, String> queryParameters(String query) throws Exception {
        Map<String, String> values = new HashMap<>();
        for (String parameter : query.split("&")) {
            String[] pair = parameter.split("=", 2);
            values.put(URLDecoder.decode(pair[0], "UTF-8"), URLDecoder.decode(pair[1], "UTF-8"));
        }
        return values;
    }

    private static class Reply {
        private final int code;
        private final String body;

        private Reply(int code, String body) {
            this.code = code;
            this.body = body;
        }
    }

    private static class Request {
        private final String method;
        private final String path;
        private final String query;
        private final String body;
        private final String authorization;

        private Request(HttpExchange exchange) throws IOException {
            method = exchange.getRequestMethod();
            path = exchange.getRequestURI().getRawPath();
            query = exchange.getRequestURI().getRawQuery();
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] data = new byte[1024];
            int read;
            while ((read = exchange.getRequestBody().read(data)) != -1) {
                buffer.write(data, 0, read);
            }
            body = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
