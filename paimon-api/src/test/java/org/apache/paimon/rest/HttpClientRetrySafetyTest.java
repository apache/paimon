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

import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a POST declaring itself unsafe to replay is sent exactly once, and that every other
 * POST keeps the 429/503 retry it has always had.
 *
 * <p>The server here refuses only the first attempt, so a retried request succeeds on its second
 * one: the request count separates "sent once" from "sent again" without waiting out five backoffs.
 */
public class HttpClientRetrySafetyTest {

    private static final String PATH = "/databases";

    private HttpServer server;
    private HttpClient client;
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeEach
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                PATH,
                exchange -> {
                    if (requests.incrementAndGet() == 1) {
                        // A proxy answering 503 says nothing about whether the server applied the
                        // request; this is exactly the shape that applies a request twice.
                        respond(exchange, 503, "{\"message\":\"busy\",\"code\":503}");
                    } else {
                        respond(exchange, 200, "{\"databases\":[\"db\"]}");
                    }
                });
        server.start();
        client = new HttpClient("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testARequestThatDeclaresItselfUnsafeIsNotRetried() {
        assertThatThrownBy(() -> post(new UnsafeToRetry()))
                .isInstanceOf(ServiceUnavailableException.class);

        // Retrying would apply the same request a second time, and nothing downstream could see it.
        assertThat(requests.get()).isEqualTo(1);
    }

    @Test
    public void testARequestThatNeverHeardOfRetrySafetyKeepsItsRetry() {
        // The regression this guards against is the global one: every request type implements
        // RESTRequest and rides the interface default, so the case above would still pass if the
        // default flipped to false and silently took 429/503 retry away from commits, database
        // creation and every other POST in the catalog.
        assertThat(new DefaultRetrySafety().isRetrySafe()).isTrue();
        assertThat(post(new DefaultRetrySafety())).isNotNull();

        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    public void testRetrySafetyNeverReachesTheWire() {
        // isRetrySafe is how the client treats the request, not something the server is told. It is
        // a getter on a serialized type, so without @JsonIgnore it would show up in the body.
        assertThat(RESTUtil.encodedBody(new UnsafeToRetry())).doesNotContain("retrySafe");
        assertThat(RESTUtil.encodedBody(new DefaultRetrySafety())).doesNotContain("retrySafe");
    }

    /** A request that leaves {@link RESTRequest#isRetrySafe()} at its default, as all others do. */
    private static class DefaultRetrySafety implements RESTRequest {

        @JsonGetter("name")
        public String getName() {
            return "db";
        }
    }

    /** A request that must reach the server at most once. */
    private static class UnsafeToRetry implements RESTRequest {

        @JsonGetter("name")
        public String getName() {
            return "db";
        }

        @Override
        public boolean isRetrySafe() {
            return false;
        }
    }

    private ListDatabasesResponse post(RESTRequest request) {
        return client.post(PATH, request, ListDatabasesResponse.class, null);
    }

    private static void respond(HttpExchange exchange, int statusCode, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
