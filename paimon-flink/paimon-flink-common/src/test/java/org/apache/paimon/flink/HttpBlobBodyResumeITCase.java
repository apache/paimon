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

package org.apache.paimon.flink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for resuming truncated HTTP response bodies while writing BLOBs. */
public class HttpBlobBodyResumeITCase extends CatalogITCaseBase {

    @Test
    public void testHttpBlobWriteResumesWithStrongEtag() throws Exception {
        byte[] payload = payload(128 * 1024);
        int truncatedLength = 89_075;
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifRange = new AtomicReference<>();

        HttpServer server = newServer();
        server.createContext(
                "/strong-etag",
                exchange -> {
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, truncatedLength);
                        return;
                    }

                    range.set(exchange.getRequestHeaders().getFirst("Range"));
                    ifRange.set(exchange.getRequestHeaders().getFirst("If-Range"));
                    byte[] remaining = Arrays.copyOfRange(payload, truncatedLength, payload.length);
                    exchange.getResponseHeaders()
                            .add(
                                    "Content-Range",
                                    String.format(
                                            "bytes %d-%d/%d",
                                            truncatedLength, payload.length - 1, payload.length));
                    respond(exchange, 206, remaining);
                });
        server.start();

        try {
            String url = url(server, "/strong-etag");
            // A successful recovery must preserve the BLOB even when terminal fetch failures are
            // configured to fall back to NULL.
            createBlobTable("strong_etag_blob_table", true);
            batchSql(
                    "INSERT INTO strong_etag_blob_table VALUES"
                            + " (1, sys.path_to_descriptor('"
                            + url
                            + "'))");

            assertBlobEquals("strong_etag_blob_table", payload);
            assertThat(requestCount).hasValue(2);
            assertThat(range).hasValue("bytes=" + truncatedLength + "-");
            assertThat(ifRange).hasValue("\"image-v1\"");
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testHttpBlobWriteReplaysWithoutStrongEtag() throws Exception {
        byte[] payload = payload(32 * 1024);
        int truncatedLength = 7_321;
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifRange = new AtomicReference<>();

        HttpServer server = newServer();
        server.createContext(
                "/no-strong-etag",
                exchange -> {
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, truncatedLength);
                        return;
                    }

                    range.set(exchange.getRequestHeaders().getFirst("Range"));
                    ifRange.set(exchange.getRequestHeaders().getFirst("If-Range"));
                    respond(exchange, 200, payload);
                });
        server.start();

        try {
            String url = url(server, "/no-strong-etag");
            createBlobTable("replayed_blob_table", false);
            batchSql(
                    "INSERT INTO replayed_blob_table VALUES"
                            + " (1, sys.path_to_descriptor('"
                            + url
                            + "'))");

            assertBlobEquals("replayed_blob_table", payload);
            assertThat(requestCount).hasValue(2);
            assertThat(range.get()).isNull();
            assertThat(ifRange.get()).isNull();
        } finally {
            server.stop(0);
        }
    }

    private void createBlobTable(String tableName, boolean writeNullOnFetchFailure) {
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE %s (id INT, picture BYTES) WITH ("
                                + "'row-tracking.enabled'='true',"
                                + "'data-evolution.enabled'='true',"
                                + "'blob-field'='picture',"
                                + "'blob-as-descriptor'='true'%s)",
                        tableName,
                        writeNullOnFetchFailure
                                ? ",'blob-write-null-on-fetch-failure'='true'"
                                : ""));
    }

    private void assertBlobEquals(String tableName, byte[] expected) {
        batchSql("ALTER TABLE %s SET ('blob-as-descriptor'='false')", tableName);
        List<Row> rows = batchSql("SELECT id, picture FROM %s", tableName);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(1);
        assertThat((byte[]) rows.get(0).getField(1)).isEqualTo(expected);
        assertThat(findLatestSnapshot(tableName)).isNotNull();
    }

    private static HttpServer newServer() throws IOException {
        return HttpServer.create(new InetSocketAddress(0), 0);
    }

    private static String url(HttpServer server, String path) {
        return String.format("http://localhost:%d%s", server.getAddress().getPort(), path);
    }

    private static void respondTruncated(HttpExchange exchange, byte[] payload, int truncatedLength)
            throws IOException {
        exchange.sendResponseHeaders(200, payload.length);
        OutputStream out = exchange.getResponseBody();
        out.write(payload, 0, truncatedLength);
        out.flush();
        exchange.close();
    }

    private static void respond(HttpExchange exchange, int statusCode, byte[] payload)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, payload.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(payload);
        }
    }

    private static byte[] payload(int length) {
        byte[] payload = new byte[length];
        byte[] seed = "paimon-http-blob-resume".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (seed[i % seed.length] ^ (i * 31) ^ (i >>> 8));
        }
        return payload;
    }
}
