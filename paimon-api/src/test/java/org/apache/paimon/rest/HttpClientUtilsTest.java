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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HttpClientUtils}. */
public class HttpClientUtilsTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    public void testExistsReturnsTrueForAvailableResource() throws Exception {
        registerHandler(
                "/ok",
                exchange -> {
                    respond(exchange, 200, "abc".getBytes());
                });

        assertThat(HttpClientUtils.exists(url("/ok"))).isTrue();
    }

    @Test
    public void testExistsReturnsFalseForMissingResource() throws Exception {
        registerHandler(
                "/missing",
                exchange -> {
                    respond(exchange, 404, new byte[0]);
                });

        assertThat(HttpClientUtils.exists(url("/missing"))).isFalse();
    }

    @Test
    public void testExistsFallsBackToRangeGetWhenHeadNotAllowed() throws Exception {
        registerHandler(
                "/no-head",
                exchange -> {
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        respond(exchange, 405, new byte[0]);
                        return;
                    }
                    respond(exchange, 200, "abc".getBytes());
                });

        assertThat(HttpClientUtils.exists(url("/no-head"))).isTrue();
    }

    @Test
    public void testExistsFallsBackToRangeGetWhenHeadReturnsNotFound() throws Exception {
        registerHandler(
                "/head-404-get-ok",
                exchange -> {
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        respond(exchange, 404, new byte[0]);
                        return;
                    }
                    if ("GET".equals(exchange.getRequestMethod())
                            && exchange.getRequestHeaders().getFirst("Range") != null) {
                        respond(exchange, 206, "abc".getBytes());
                        return;
                    }
                    respond(exchange, 404, new byte[0]);
                });

        assertThat(HttpClientUtils.exists(url("/head-404-get-ok"))).isTrue();
    }

    @Test
    public void testExistsFallsBackToRangeGetWhenHeadReturnsForbidden() throws Exception {
        registerHandler(
                "/head-403-get-ok",
                exchange -> {
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        respond(exchange, 403, new byte[0]);
                        return;
                    }
                    if ("GET".equals(exchange.getRequestMethod())
                            && exchange.getRequestHeaders().getFirst("Range") != null) {
                        respond(exchange, 200, "abc".getBytes());
                        return;
                    }
                    respond(exchange, 403, new byte[0]);
                });

        assertThat(HttpClientUtils.exists(url("/head-403-get-ok"))).isTrue();
    }

    @Test
    public void testExistsTreatsEmptyResourceAsExistingWhenRangeReturns416() throws Exception {
        registerHandler(
                "/empty-no-head",
                exchange -> {
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        respond(exchange, 405, new byte[0]);
                        return;
                    }
                    if ("GET".equals(exchange.getRequestMethod())
                            && exchange.getRequestHeaders().getFirst("Range") != null) {
                        respond(exchange, 416, new byte[0]);
                        return;
                    }
                    respond(exchange, 200, new byte[0]);
                });

        assertThat(HttpClientUtils.exists(url("/empty-no-head"))).isTrue();
    }

    @Test
    public void testExistsReturnsFalseOnlyWhenRangeGetAlsoNotFound() throws Exception {
        registerHandler(
                "/head-404-get-404",
                exchange -> {
                    if ("HEAD".equals(exchange.getRequestMethod())) {
                        respond(exchange, 404, new byte[0]);
                        return;
                    }
                    respond(exchange, 404, new byte[0]);
                });

        assertThat(HttpClientUtils.exists(url("/head-404-get-404"))).isFalse();
    }

    private void registerHandler(String path, HttpHandler handler) {
        server.createContext(path, handler);
    }

    private String url(String path) {
        return "http://127.0.0.1:" + port + path;
    }

    private static void respond(HttpExchange exchange, int statusCode, byte[] body)
            throws IOException {
        boolean headRequest = "HEAD".equals(exchange.getRequestMethod());
        long responseLength = headRequest ? -1 : body.length;
        exchange.sendResponseHeaders(statusCode, responseLength);
        if (!headRequest && body.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(body);
            }
        } else {
            exchange.close();
        }
    }
}
