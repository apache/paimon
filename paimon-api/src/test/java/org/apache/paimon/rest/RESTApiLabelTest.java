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
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.requests.UpsertLabelRequest;
import org.apache.paimon.rest.responses.GetLabelResponse;
import org.apache.paimon.rest.responses.ListLabelsResponse;

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

/** HTTP contract tests for generic label operations. */
class RESTApiLabelTest {

    private static final String BASE_PATH = "/v1/catalog%2Fid/labels";
    private static final String LABEL_JSON =
            "{\"entityType\":\"TABLE\",\"entityName\":\"sales.orders\","
                    + "\"key\":\"domain\",\"value\":\"sales\"}";

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
    void testUpsertUsesOnePostWithoutExistenceCheck() throws Exception {
        for (String value : Arrays.asList("sales", "finance", "finance")) {
            enqueue(200, "");
            api.upsertLabel("TABLE", "sales.orders", "domain", value);
        }

        assertThat(requests).hasSize(3);
        for (int i = 0; i < requests.size(); i++) {
            Request request = requests.get(i);
            assertThat(request.method).isEqualTo("POST");
            assertThat(request.path).isEqualTo(BASE_PATH);
            assertThat(request.query).isNull();
            assertThat(request.authorization).isEqualTo("Bearer test-token");
            assertThat(RESTApi.fromJson(request.body, Map.class))
                    .isEqualTo(
                            labelFields(
                                    "TABLE",
                                    "sales.orders",
                                    "domain",
                                    i == 0 ? "sales" : "finance"));
        }
    }

    @Test
    void testColumnAndProviderDefinedTypesShareTheWriteEndpoint() throws Exception {
        for (String type : Arrays.asList("COLUMN", "MODEL_VERSION")) {
            enqueue(200, "");
            api.upsertLabel(type, "sales.orders.id", "classification", "");
        }
        assertThat(requests).hasSize(2);
        for (int i = 0; i < requests.size(); i++) {
            Request request = requests.get(i);
            assertThat(request.method).isEqualTo("POST");
            assertThat(request.path).isEqualTo(BASE_PATH);
            assertThat(RESTApi.fromJson(request.body, Map.class))
                    .isEqualTo(
                            labelFields(
                                    i == 0 ? "COLUMN" : "MODEL_VERSION",
                                    "sales.orders.id",
                                    "classification",
                                    ""));
        }
    }

    @Test
    void testEntityNameAndKeyRemainSingleEncodedSegments() throws Exception {
        String name = "sales db.orders/华+%?#";
        String key = "a/b+% ?#";
        enqueue(200, RESTApi.toJson(labelFields("COLUMN", name, key, "confidential")));
        GetLabelResponse label = api.getLabel("COLUMN", name, key);

        assertThat(requests.get(0).method).isEqualTo("GET");
        assertThat(requests.get(0).path)
                .isEqualTo(
                        BASE_PATH
                                + "/COLUMN/sales+db.orders%2F%E5%8D%8E%2B%25%3F%23/a%2Fb%2B%25+%3F%23");
        assertThat(label.getEntityType()).isEqualTo("COLUMN");
        assertThat(label.getEntityName()).isEqualTo(name);
        assertThat(label.getKey()).isEqualTo(key);
        assertThat(label.getValue()).isEqualTo("confidential");

        enqueue(200, "");
        api.deleteLabel("COLUMN", name, key);
        assertThat(requests.get(1).method).isEqualTo("DELETE");
        assertThat(requests.get(1).path).isEqualTo(requests.get(0).path);
    }

    @Test
    void testDotOnlyNamesRemainDataInThePath() {
        enqueue(200, LABEL_JSON);
        api.getLabel("TABLE", ".", "..");
        assertThat(requests.get(0).path).isEqualTo(BASE_PATH + "/TABLE/%2E/%2E%2E");
    }

    @Test
    void testListFollowsOpaqueContinuationTokens() throws Exception {
        enqueue(200, "{\"labels\":[" + LABEL_JSON + "],\"nextPageToken\":\"next +/%?&\"}");
        enqueue(200, "{\"labels\":[" + LABEL_JSON.replace("domain", "owner") + "]}");

        List<GetLabelResponse> labels = api.listLabels("TABLE", "sales.orders");

        assertThat(labels).extracting(GetLabelResponse::getKey).containsExactly("domain", "owner");
        assertThat(requests).hasSize(2);
        assertThat(requests.get(0).method).isEqualTo("GET");
        assertThat(requests.get(0).path).isEqualTo(BASE_PATH + "/TABLE/sales.orders");
        assertThat(requests.get(0).query).isNull();
        assertThat(requests.get(1).path).isEqualTo(requests.get(0).path);
        assertThat(queryParameters(requests.get(1).query)).containsEntry("pageToken", "next +/%?&");
    }

    @Test
    void testPagedListCarriesLimitsAndReturnsEmptyTerminalPage() throws Exception {
        enqueue(200, "{\"labels\":[" + LABEL_JSON + "],\"nextPageToken\":\"next\"}");
        PagedList<GetLabelResponse> first =
                api.listLabelsPaged("TABLE", "sales.orders", 1000, "start +/%");
        assertThat(first.getElements())
                .extracting(GetLabelResponse::getValue)
                .containsExactly("sales");
        assertThat(first.getNextPageToken()).isEqualTo("next");
        assertThat(queryParameters(requests.get(0).query))
                .containsEntry("maxResults", "1000")
                .containsEntry("pageToken", "start +/%");

        enqueue(200, "{\"labels\":[]}");
        PagedList<GetLabelResponse> last =
                api.listLabelsPaged("COLUMN", "sales.orders.id", 1, "next");
        assertThat(last.getElements()).isEmpty();
        assertThat(last.getNextPageToken()).isNull();
        assertThat(requests.get(1).path).isEqualTo(BASE_PATH + "/COLUMN/sales.orders.id");

        enqueue(200, "{\"labels\":[]}");
        assertThat(api.listLabels("TABLE", "sales.empty")).isEmpty();
    }

    @Test
    void testRepeatedDeleteDoesNotRequireARead() {
        enqueue(200, "");
        enqueue(200, "");
        api.deleteLabel("TABLE", "sales.orders", "domain");
        api.deleteLabel("TABLE", "sales.orders", "domain");

        assertThat(requests).hasSize(2);
        for (Request request : requests) {
            assertThat(request.method).isEqualTo("DELETE");
            assertThat(request.path).isEqualTo(BASE_PATH + "/TABLE/sales.orders/domain");
            assertThat(request.body).isEmpty();
        }
    }

    @Test
    void testMissingEntityIsPropagatedByEveryOperation() {
        List<Consumer<RESTApi>> operations =
                Arrays.asList(
                        client -> client.upsertLabel("TABLE", "sales.missing", "domain", "sales"),
                        client -> client.getLabel("TABLE", "sales.missing", "domain"),
                        client -> client.listLabels("TABLE", "sales.missing"),
                        client -> client.listLabelsPaged("TABLE", "sales.missing", null, null),
                        client -> client.deleteLabel("TABLE", "sales.missing", "domain"));
        for (Consumer<RESTApi> operation : operations) {
            enqueue(
                    404,
                    "{\"code\":404,\"message\":\"entity missing\",\"resourceType\":\"TABLE\",\"resourceName\":\"sales.missing\"}");
            assertThatThrownBy(() -> operation.accept(api))
                    .isInstanceOf(NoSuchResourceException.class)
                    .hasMessageContaining("entity missing");
        }
        assertThat(requests).hasSize(operations.size());
    }

    @Test
    void testMissingLabelForbiddenAndUnsupportedHaveNoFallback() {
        enqueue(
                404,
                "{\"code\":404,\"message\":\"label missing\",\"resourceType\":\"LABEL\",\"resourceName\":\"domain\"}");
        assertThatThrownBy(() -> api.getLabel("TABLE", "sales.orders", "domain"))
                .isInstanceOf(NoSuchResourceException.class)
                .hasMessageContaining("label missing");

        enqueue(403, "{\"code\":403,\"message\":\"label access denied\"}");
        assertThatThrownBy(() -> api.upsertLabel("TABLE", "sales.orders", "domain", "sales"))
                .isInstanceOf(ForbiddenException.class)
                .hasMessageContaining("label access denied");

        enqueue(501, "{\"code\":501,\"message\":\"labels unsupported\"}");
        assertThatThrownBy(() -> api.listLabels("TABLE", "sales.orders"))
                .isInstanceOf(NotImplementedException.class)
                .hasMessageContaining("labels unsupported");
        assertThat(requests).hasSize(3);
    }

    @Test
    void testInvalidInputsFailBeforeHttp() {
        for (String invalid : Arrays.asList(null, "", "  ")) {
            assertThatThrownBy(() -> api.upsertLabel(invalid, "sales.orders", "domain", "sales"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.upsertLabel("TABLE", invalid, "domain", "sales"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.upsertLabel("TABLE", "sales.orders", invalid, "sales"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.getLabel(invalid, "sales.orders", "domain"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.listLabels("TABLE", invalid))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> api.deleteLabel("TABLE", "sales.orders", invalid))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> api.upsertLabel("TABLE", "sales.orders", "domain", null))
                .isInstanceOf(IllegalArgumentException.class);
        for (int invalid : new int[] {-1, 0, 1001}) {
            assertThatThrownBy(() -> api.listLabelsPaged("TABLE", "sales.orders", invalid, null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(requests).isEmpty();
    }

    @Test
    void testWireModelsRequireWriteFieldsAndTolerateFutureResponseFields() throws Exception {
        UpsertLabelRequest request = RESTApi.fromJson(LABEL_JSON, UpsertLabelRequest.class);
        assertThat(RESTApi.fromJson(RESTApi.toJson(request), Map.class))
                .isEqualTo(labelFields("TABLE", "sales.orders", "domain", "sales"));
        for (String field : Arrays.asList("entityType", "entityName", "key", "value")) {
            Map<String, String> fields = labelFields("TABLE", "sales.orders", "domain", "sales");
            fields.remove(field);
            String json = RESTApi.toJson(fields);
            assertThatThrownBy(() -> RESTApi.fromJson(json, UpsertLabelRequest.class))
                    .hasRootCauseInstanceOf(IllegalArgumentException.class);
        }

        String futureLabel =
                LABEL_JSON.substring(0, LABEL_JSON.length() - 1) + ",\"source\":\"system\"}";
        ListLabelsResponse response =
                RESTApi.fromJson(
                        "{\"labels\":["
                                + futureLabel
                                + "],\"nextPageToken\":\"next\",\"future\":true}",
                        ListLabelsResponse.class);
        assertThat(response.getLabels()).hasSize(1);
        assertThat(response.getLabels().get(0).getKey()).isEqualTo("domain");
        assertThat(response.getNextPageToken()).isEqualTo("next");
        assertThat(RESTApi.fromJson("{\"labels\":[]}", ListLabelsResponse.class).getNextPageToken())
                .isNull();
        assertThat(RESTApi.fromJson("{}", ListLabelsResponse.class).getLabels()).isEmpty();
    }

    private void enqueue(int code, String body) {
        replies.add(new Reply(code, body));
    }

    private static Map<String, String> labelFields(
            String type, String name, String key, String value) {
        Map<String, String> fields = new HashMap<>();
        fields.put("entityType", type);
        fields.put("entityName", name);
        fields.put("key", key);
        fields.put("value", value);
        return fields;
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
