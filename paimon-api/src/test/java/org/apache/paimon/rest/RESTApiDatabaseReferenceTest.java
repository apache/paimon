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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.paimon.rest.RESTCatalogInternalOptions.PREFIX;
import static org.apache.paimon.rest.RESTCatalogOptions.DATABASE_REFERENCE;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;

/** HTTP contract tests for database-level branches and immutable tags. */
class RESTApiDatabaseReferenceTest {

    private static final String TREES_PATH = "/v1/catalog%2Fid/databases/training+db/trees";

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
    void testBranchAndImmutableTagHappyPath() throws Exception {
        enqueue(
                200,
                "{\"references\":[{\"type\":\"BRANCH\",\"name\":\"main\"},"
                        + "{\"type\":\"TAG\",\"name\":\"train-v1\"}],"
                        + "\"nextPageToken\":\"next\"}");
        PagedList<DatabaseReference> page =
                api.listDatabaseReferencesPaged(
                        "training db", DatabaseReferenceType.TAG, 100, "start token");
        assertThat(page.getElements())
                .containsExactly(
                        new DatabaseReference(DatabaseReferenceType.BRANCH, "main"),
                        new DatabaseReference(DatabaseReferenceType.TAG, "train-v1"));
        assertThat(page.getNextPageToken()).isEqualTo("next");
        assertRequest(0, "GET", TREES_PATH);
        assertThat(queryParameters(requests.get(0).query))
                .containsEntry("type", "tag")
                .containsEntry("maxResults", "100")
                .containsEntry("pageToken", "start token");

        enqueue(200, "{\"reference\":{\"type\":\"BRANCH\",\"name\":\"main\"}}");
        assertThat(api.getDatabaseReference("training db", "main"))
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "main"));
        assertRequest(1, "GET", TREES_PATH + "/main");

        enqueue(200, "{\"reference\":{\"type\":\"BRANCH\",\"name\":\"exp-1\"}}");
        DatabaseReference branch =
                api.createDatabaseReference(
                        "training db",
                        "exp-1",
                        DatabaseReferenceType.BRANCH,
                        new DatabaseReference(DatabaseReferenceType.BRANCH, "main"));
        assertThat(branch).isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "exp-1"));
        assertRequest(2, "POST", TREES_PATH);
        assertThat(queryParameters(requests.get(2).query))
                .containsEntry("name", "exp-1")
                .containsEntry("type", "branch");
        assertReferenceBody(requests.get(2), "BRANCH", "main");

        enqueue(200, "{\"reference\":{\"type\":\"TAG\",\"name\":\"train-v1\"}}");
        DatabaseReference tag =
                api.createDatabaseReference(
                        "training db",
                        "train-v1",
                        DatabaseReferenceType.TAG,
                        new DatabaseReference(DatabaseReferenceType.BRANCH, "exp-1"));
        assertThat(tag).isEqualTo(new DatabaseReference(DatabaseReferenceType.TAG, "train-v1"));
        assertRequest(3, "POST", TREES_PATH);
        assertThat(queryParameters(requests.get(3).query))
                .containsEntry("name", "train-v1")
                .containsEntry("type", "tag");
        assertReferenceBody(requests.get(3), "BRANCH", "exp-1");

        enqueue(200, "{\"reference\":{\"type\":\"BRANCH\",\"name\":\"main\"}}");
        assertThat(api.fastForwardDatabaseBranch("training db", "main", "train-v1"))
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "main"));
        assertRequest(4, "PUT", TREES_PATH + "/main");
        assertThat(queryParameters(requests.get(4).query))
                .containsEntry("mode", "FAST_FORWARD")
                .containsEntry("type", "branch");
        assertReferenceBody(requests.get(4), "TAG", "train-v1");

        enqueue(200, "{\"reference\":{\"type\":\"BRANCH\",\"name\":\"exp-1\"}}");
        assertThat(
                        api.deleteDatabaseReference(
                                "training db", "exp-1", DatabaseReferenceType.BRANCH))
                .isEqualTo(new DatabaseReference(DatabaseReferenceType.BRANCH, "exp-1"));
        assertRequest(5, "DELETE", TREES_PATH + "/exp-1");
        assertThat(queryParameters(requests.get(5).query)).containsEntry("type", "branch");
        assertThat(requests.get(5).body).isEmpty();
    }

    @Test
    void testListAllReferencesFollowsPages() {
        enqueue(
                200,
                "{\"references\":[{\"type\":\"BRANCH\",\"name\":\"main\"}],"
                        + "\"nextPageToken\":\"p2\"}");
        enqueue(200, "{\"references\":[{\"type\":\"BRANCH\",\"name\":\"exp-1\"}]}");

        assertThat(api.listDatabaseReferences("training db", DatabaseReferenceType.BRANCH))
                .extracting(DatabaseReference::getName)
                .containsExactly("main", "exp-1");
        assertThat(requests).hasSize(2);
        assertThat(queryParameters(requests.get(0).query)).containsEntry("type", "branch");
        assertThat(queryParameters(requests.get(1).query))
                .containsEntry("type", "branch")
                .containsEntry("pageToken", "p2");
    }

    @Test
    void testReferenceOptionIsSentToTableApis() {
        enqueue(200, "{\"tables\":[]}");
        Options options = new Options();
        options.set(URI, "http://127.0.0.1:" + server.getAddress().getPort());
        options.set(PREFIX, "catalog/id");
        options.set(TOKEN_PROVIDER, "bear");
        options.set(TOKEN, "test-token");
        options.set(DATABASE_REFERENCE, "train-v1");

        new RESTApi(options, false).listTables("training db");

        assertRequest(0, "GET", "/v1/catalog%2Fid/databases/training+db/tables");
        assertThat(requests.get(0).reference).isEqualTo("train-v1");
    }

    private void enqueue(int code, String body) {
        replies.add(new Reply(code, body));
    }

    private void assertRequest(int index, String method, String path) {
        Request request = requests.get(index);
        assertThat(request.method).isEqualTo(method);
        assertThat(request.path).isEqualTo(path);
        assertThat(request.authorization).isEqualTo("Bearer test-token");
    }

    private static void assertReferenceBody(Request request, String type, String name)
            throws Exception {
        assertThat(RESTApi.fromJson(request.body, Map.class))
                .containsEntry("type", type)
                .containsEntry("name", name)
                .hasSize(2);
    }

    private static Map<String, String> queryParameters(String query) {
        Map<String, String> values = new LinkedHashMap<>();
        if (query == null || query.isEmpty()) {
            return values;
        }
        for (String parameter : query.split("&")) {
            String[] pair = parameter.split("=", 2);
            values.put(decode(pair[0]), decode(pair[1]));
        }
        return values;
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        private final String reference;

        private Request(HttpExchange exchange) throws IOException {
            method = exchange.getRequestMethod();
            path = exchange.getRequestURI().getRawPath();
            query = exchange.getRequestURI().getRawQuery();
            body = read(exchange.getRequestBody());
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            reference = exchange.getRequestHeaders().getFirst("Paimon-Reference");
        }

        private static String read(InputStream input) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = input.read(buffer)) >= 0) {
                output.write(buffer, 0, length);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
