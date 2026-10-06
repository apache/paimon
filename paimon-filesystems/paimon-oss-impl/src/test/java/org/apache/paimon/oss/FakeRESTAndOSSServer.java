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

package org.apache.paimon.oss;

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.responses.GetTableTokenResponse;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** A local REST catalog token endpoint and OSS endpoint that record what they are asked. */
public class FakeRESTAndOSSServer implements AutoCloseable {

    public static final int OBJECT_SIZE = 4096;

    private final HttpServer server;
    private final List<GetTableTokenResponse> tokens = new ArrayList<>();
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final List<String> tokenRequestPaths = Collections.synchronizedList(new ArrayList<>());
    private final List<String> ossGetAuthorizations =
            Collections.synchronizedList(new ArrayList<>());

    public FakeRESTAndOSSServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/", this::handleToken);
        server.createContext("/bucket/", this::handleObject);
        server.start();
    }

    /** Tokens returned by successive token requests; the last one repeats. */
    public void addToken(String accessKeyId, long expiresAtMillis) {
        tokens.add(new GetTableTokenResponse(ossToken(accessKeyId), expiresAtMillis));
    }

    /** Like {@link #addToken}, but with an access key pair and no security token. */
    public void addTokenWithoutSecurityToken(String accessKeyId, long expiresAtMillis) {
        Map<String, String> token = ossToken(accessKeyId);
        token.remove("fs.oss.securityToken");
        tokens.add(new GetTableTokenResponse(token, expiresAtMillis));
    }

    public Map<String, String> ossToken(String accessKeyId) {
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.endpoint", endpoint());
        token.put("fs.oss.accessKeyId", accessKeyId);
        token.put("fs.oss.accessKeySecret", "secret-" + accessKeyId);
        token.put("fs.oss.securityToken", "token-" + accessKeyId);
        return token;
    }

    /** Options a REST catalog client needs to reach this server. */
    public Options catalogOptions() {
        Options options = new Options();
        options.set("uri", endpoint());
        options.set("token.provider", "bear");
        options.set("token", "catalog-token");
        options.set("prefix", "catalog");
        return options;
    }

    public int tokenRequests() {
        return tokenRequests.get();
    }

    public List<String> tokenRequestPaths() {
        return tokenRequestPaths;
    }

    public List<String> ossGetAuthorizations() {
        return ossGetAuthorizations;
    }

    public String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handleToken(HttpExchange exchange) throws IOException {
        tokenRequestPaths.add(exchange.getRequestURI().getPath());
        int index = tokenRequests.getAndIncrement();
        GetTableTokenResponse token = tokens.get(Math.min(index, tokens.size() - 1));
        byte[] body = RESTApi.toJson(token).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
        exchange.close();
    }

    private void handleObject(HttpExchange exchange) throws IOException {
        exchange.getResponseHeaders().add("Last-Modified", "Thu, 24 Sep 2026 00:00:00 GMT");
        if ("HEAD".equals(exchange.getRequestMethod())) {
            exchange.getResponseHeaders().add("Content-Length", String.valueOf(OBJECT_SIZE));
            exchange.sendResponseHeaders(200, -1);
        } else {
            ossGetAuthorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            String[] range =
                    exchange.getRequestHeaders()
                            .getFirst("Range")
                            .substring("bytes=".length())
                            .split("-");
            int start = Integer.parseInt(range[0]);
            int end = Math.min(Integer.parseInt(range[1]), OBJECT_SIZE - 1);
            exchange.getResponseHeaders()
                    .add("Content-Range", "bytes " + start + "-" + end + "/" + OBJECT_SIZE);
            exchange.sendResponseHeaders(206, end - start + 1);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(new byte[end - start + 1]);
            }
        }
        exchange.close();
    }
}
