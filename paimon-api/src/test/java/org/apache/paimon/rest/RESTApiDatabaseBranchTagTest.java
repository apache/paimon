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
import org.apache.paimon.rest.responses.GetDatabaseTagResponse;

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
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** HTTP contract tests for database-level branches and immutable tags. */
class RESTApiDatabaseBranchTagTest {

    private static final String DATABASE_PATH = "/v1/catalog%2Fid/databases/training+db";

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
                    exchange.sendResponseHeaders(reply.code, data.length == 0 ? -1 : data.length);
                    if (data.length > 0) {
                        try (OutputStream output = exchange.getResponseBody()) {
                            output.write(data);
                        }
                    }
                    exchange.close();
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
    void testBranchRequestsMatchTableRequests() throws Exception {
        Identifier table = Identifier.create("training db", "features");
        enqueue(200, "");
        api.createDatabaseBranch("training db", "experiment", "baseline");
        enqueue(200, "");
        api.createBranch(table, "experiment", "baseline");
        assertRequest(0, "POST", DATABASE_PATH + "/branches");
        assertRequest(1, "POST", DATABASE_PATH + "/tables/features/branches");
        assertBody(requests.get(0), "{\"branch\":\"experiment\",\"fromTag\":\"baseline\"}");
        assertThat(requests.get(0).body).isEqualTo(requests.get(1).body);

        enqueue(200, "");
        api.createDatabaseBranch("training db", "empty", null);
        assertBody(requests.get(2), "{\"branch\":\"empty\",\"fromTag\":null}");

        enqueue(200, "");
        api.fastForwardDatabase("training db", "experiment");
        enqueue(200, "");
        api.fastForward(table, "experiment");
        assertRequest(3, "POST", DATABASE_PATH + "/branches/experiment/forward");
        assertRequest(4, "POST", DATABASE_PATH + "/tables/features/branches/experiment/forward");
        assertBody(requests.get(3), "{}");
        assertThat(requests.get(3).body).isEqualTo(requests.get(4).body);

        enqueue(200, "");
        api.dropDatabaseBranch("training db", "experiment");
        assertRequest(5, "DELETE", DATABASE_PATH + "/branches/experiment");
        assertThat(requests.get(5).body).isEmpty();
        assertThat(requests.get(5).query).isNull();
        assertThat(requests).hasSize(6);
    }

    @Test
    void testListBranchesUsesTableResponse() {
        enqueue(200, "{\"branches\":[\"main\",\"experiment\"]}");
        assertThat(api.listDatabaseBranches("training db")).containsExactly("main", "experiment");
        assertRequest(0, "GET", DATABASE_PATH + "/branches");
        assertThat(requests.get(0).query).isNull();
        enqueue(200, "{}");
        assertThat(api.listDatabaseBranches("training db")).isEmpty();
    }

    @Test
    void testDatabaseTagMetadataAndDefaults() throws Exception {
        enqueue(200, "");
        api.createDatabaseTag("training db", "train-v1", "experiment", "7d");
        assertRequest(0, "POST", DATABASE_PATH + "/tags");
        assertBody(
                requests.get(0),
                "{\"tagName\":\"train-v1\",\"fromBranch\":\"experiment\",\"timeRetained\":\"7d\"}");

        enqueue(200, "");
        api.createDatabaseTag("training db", "baseline", null, null);
        assertBody(
                requests.get(1),
                "{\"tagName\":\"baseline\",\"fromBranch\":null,\"timeRetained\":null}");

        enqueue(
                200,
                "{\"tagName\":\"train-v1\",\"fromBranch\":\"experiment\",\"tagCreateTime\":1720000000000,\"tagTimeRetained\":\"7d\",\"futureField\":true}");
        GetDatabaseTagResponse tag = api.getDatabaseTag("training db", "train-v1");
        assertThat(tag.tagName()).isEqualTo("train-v1");
        assertThat(tag.fromBranch()).isEqualTo("experiment");
        assertThat(tag.tagCreateTime()).isEqualTo(1720000000000L);
        assertThat(tag.tagTimeRetained()).isEqualTo("7d");
        assertRequest(2, "GET", DATABASE_PATH + "/tags/train-v1");

        enqueue(200, "");
        api.deleteDatabaseTag("training db", "train-v1");
        assertRequest(3, "DELETE", DATABASE_PATH + "/tags/train-v1");
        assertThat(requests.get(3).body).isEmpty();
        assertThat(requests.get(3).query).isNull();
        assertThat(requests).hasSize(4);
    }

    @Test
    void testTagPagesUseTableResponseAndPreserveTokens() {
        enqueue(200, "{\"tags\":[\"train-v1\"],\"nextPageToken\":\"next +/%?&\"}");
        PagedList<String> first =
                api.listDatabaseTagsPaged("training db", 10, "start +/%", "train-");
        assertThat(first.getElements()).containsExactly("train-v1");
        assertThat(first.getNextPageToken()).isEqualTo("next +/%?&");
        assertRequest(0, "GET", DATABASE_PATH + "/tags");
        Map<String, String> query = queryParameters(requests.get(0).query);
        assertThat(query)
                .hasSize(3)
                .containsEntry("maxResults", "10")
                .containsEntry("pageToken", "start +/%")
                .containsEntry("tagNamePrefix", "train-");

        enqueue(200, "{\"tags\":[\"train-v2\"]}");
        PagedList<String> second =
                api.listDatabaseTagsPaged("training db", null, first.getNextPageToken(), "train-");
        assertThat(second.getElements()).containsExactly("train-v2");
        assertThat(second.getNextPageToken()).isNull();
        assertThat(queryParameters(requests.get(1).query))
                .hasSize(2)
                .containsEntry("pageToken", "next +/%?&")
                .containsEntry("tagNamePrefix", "train-");

        enqueue(200, "{\"tags\":null,\"nextPageToken\":\"continue\"}");
        PagedList<String> empty = api.listDatabaseTagsPaged("training db", null, null, null);
        assertThat(empty.getElements()).isEmpty();
        assertThat(empty.getNextPageToken()).isEqualTo("continue");
        assertThat(requests.get(2).query).isNull();
    }

    @Test
    void testManagementRejectsVirtualDatabaseNamesBeforeSendingRequests() {
        for (String database :
                new String[] {"training db$branch_experiment", "training db$tag_train-v1"}) {
            assertThatThrownBy(() -> api.listDatabaseBranches(database))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.createDatabaseBranch(database, "new", null))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.dropDatabaseBranch(database, "experiment"))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.fastForwardDatabase(database, "experiment"))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.createDatabaseTag(database, "new", null, null))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.getDatabaseTag(database, "train-v1"))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.listDatabaseTagsPaged(database, null, null, null))
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> api.deleteDatabaseTag(database, "train-v1"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
        assertThat(requests).isEmpty();
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

    private static void assertBody(Request request, String expectedJson) throws Exception {
        assertThat(request.query).isNull();
        assertThat(RESTApi.fromJson(request.body, Map.class))
                .isEqualTo(RESTApi.fromJson(expectedJson, Map.class));
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

        private Request(HttpExchange exchange) throws IOException {
            method = exchange.getRequestMethod();
            path = exchange.getRequestURI().getRawPath();
            query = exchange.getRequestURI().getRawQuery();
            body = read(exchange.getRequestBody());
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
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
