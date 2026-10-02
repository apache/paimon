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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.Options;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.ResponseMessage;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OSSRetryStrategy}, end to end against a fake OSS endpoint. */
public class OSSRetryStrategyTest {

    private static final int PART_SIZE = 100 * 1024;

    private HttpServer server;
    private final AtomicInteger completeCalls = new AtomicInteger();
    private final AtomicInteger deleteObjectsCalls = new AtomicInteger();
    private volatile int throttledCompletes;
    private volatile int throttledDeletes;

    @BeforeEach
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void testThrottledCompleteMultipartUploadIsRetried() throws IOException {
        throttledCompletes = 2;

        writeMultipartFile(fileIO(10));

        assertThat(completeCalls).hasValue(3);
    }

    @Test
    public void testCompleteMultipartUploadGivesUpAfterMaxAttempts() {
        throttledCompletes = Integer.MAX_VALUE;

        assertThatThrownBy(() -> writeMultipartFile(fileIO(2)))
                .hasStackTraceContaining("QpsLimitExceeded");
        assertThat(completeCalls).hasValue(3);
    }

    @Test
    public void testDisabledFallsBackToSdkRetry() {
        throttledCompletes = 1;
        OSSFileIO fileIO = fileIO(10, false);

        assertThatThrownBy(() -> writeMultipartFile(fileIO))
                .hasStackTraceContaining("QpsLimitExceeded");
        assertThat(completeCalls).hasValue(1);
    }

    @Test
    public void testThrottledDeleteObjectsIsRetried() throws Exception {
        throttledDeletes = 1;
        Path path = new Path("oss://bucket/dir/file");

        fileIO(10)
                .ossClient(path)
                .deleteObjects(
                        new DeleteObjectsRequest("bucket")
                                .withKeys(Collections.singletonList("dir/file")));

        assertThat(deleteObjectsCalls).hasValue(2);
    }

    @Test
    public void testRetriesThrottlingAndServerErrorsOnly() {
        OSSRetryStrategy strategy = new OSSRetryStrategy();
        for (int status : new int[] {429, 500, 502, 503, 504}) {
            assertThat(
                            strategy.shouldRetry(
                                    serverError("QpsLimitExceeded"), null, status(status), 0))
                    .as("status %s", status)
                    .isTrue();
        }
        for (int status : new int[] {400, 403, 404, 409, 501}) {
            assertThat(strategy.shouldRetry(serverError("NoSuchUpload"), null, status(status), 0))
                    .as("status %s", status)
                    .isFalse();
        }
        assertThat(strategy.shouldRetry(serverError("InvalidResponse"), null, status(503), 0))
                .isFalse();
    }

    @Test
    public void testRetriesNetworkErrorsOnly() {
        OSSRetryStrategy strategy = new OSSRetryStrategy();
        assertThat(strategy.shouldRetry(clientError("SocketTimeout"), null, null, 0)).isTrue();
        assertThat(strategy.shouldRetry(clientError("ConnectionTimeout"), null, null, 0)).isTrue();
        assertThat(strategy.shouldRetry(clientError("SocketException"), null, null, 0)).isTrue();
        assertThat(strategy.shouldRetry(clientError("NonRepeatableRequest"), null, null, 0))
                .isFalse();
        assertThat(strategy.shouldRetry(clientError("Unknown"), null, null, 0)).isFalse();
    }

    @Test
    public void testPauseDelayIsJitteredAndCapped() {
        OSSRetryStrategy strategy = new OSSRetryStrategy();
        for (int retries = 1; retries <= 100; retries++) {
            long ceiling = retries < 6 ? 300L << retries : 10_000;
            for (int i = 0; i < 100; i++) {
                assertThat(strategy.getPauseDelay(retries)).isBetween(ceiling / 2, ceiling);
            }
        }
    }

    private static OSSException serverError(String code) {
        return new OSSException("failed", code, "request-id", "host", null, null, "POST");
    }

    private static ClientException clientError(String code) {
        return new ClientException("failed", code, "request-id");
    }

    private static ResponseMessage status(int status) {
        ResponseMessage response = new ResponseMessage(null);
        response.setStatusCode(status);
        return response;
    }

    private void writeMultipartFile(OSSFileIO fileIO) throws IOException {
        try (PositionOutputStream out =
                fileIO.newOutputStream(new Path("oss://bucket/dir/file"), false)) {
            out.write(new byte[PART_SIZE + PART_SIZE / 2]);
        }
    }

    private OSSFileIO fileIO(int maxAttempts) {
        return fileIO(maxAttempts, true);
    }

    private OSSFileIO fileIO(int maxAttempts, boolean enhancedRetry) {
        Options options = new Options();
        options.set("fs.oss.enhanced-retry.enabled", String.valueOf(enhancedRetry));
        options.set("fs.oss.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        options.set("fs.oss.accessKeyId", "ak");
        options.set("fs.oss.accessKeySecret", "sk");
        options.set("fs.oss.attempts.maximum", String.valueOf(maxAttempts));
        options.set("fs.oss.multipart.upload.size", String.valueOf(PART_SIZE));
        options.set("file-io.allow-cache", "false");
        OSSFileIO fileIO = new OSSFileIO();
        fileIO.configure(CatalogContext.create(options));
        return fileIO;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (InputStream in = exchange.getRequestBody()) {
            while (in.read() >= 0) {
                // drain the request body
            }
        }
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getRawQuery();
        query = query == null ? "" : query;
        exchange.getResponseHeaders().add("x-oss-request-id", "fake-request-id");
        if ("HEAD".equals(method)) {
            exchange.sendResponseHeaders(404, -1);
        } else if ("GET".equals(method)) {
            respond(
                    exchange,
                    200,
                    "<ListBucketResult><Name>bucket</Name><MaxKeys>1</MaxKeys>"
                            + "<IsTruncated>false</IsTruncated><KeyCount>0</KeyCount>"
                            + "</ListBucketResult>");
        } else if ("POST".equals(method) && query.contains("uploads")) {
            respond(
                    exchange,
                    200,
                    "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>dir/file</Key>"
                            + "<UploadId>upload-id</UploadId></InitiateMultipartUploadResult>");
        } else if ("PUT".equals(method)) {
            exchange.getResponseHeaders().add("ETag", "\"etag\"");
            exchange.sendResponseHeaders(200, -1);
        } else if ("POST".equals(method) && query.contains("uploadId")) {
            if (completeCalls.incrementAndGet() <= throttledCompletes) {
                respondThrottled(exchange);
            } else {
                respond(
                        exchange,
                        200,
                        "<CompleteMultipartUploadResult><Bucket>bucket</Bucket>"
                                + "<Key>dir/file</Key><ETag>\"etag\"</ETag>"
                                + "</CompleteMultipartUploadResult>");
            }
        } else if ("POST".equals(method) && query.contains("delete")) {
            if (deleteObjectsCalls.incrementAndGet() <= throttledDeletes) {
                respondThrottled(exchange);
            } else {
                respond(exchange, 200, "<DeleteResult></DeleteResult>");
            }
        } else {
            exchange.sendResponseHeaders(400, -1);
        }
        exchange.close();
    }

    private static void respondThrottled(HttpExchange exchange) throws IOException {
        respond(
                exchange,
                503,
                "<Error><Code>QpsLimitExceeded</Code>"
                        + "<Message>Please reduce your request rate.</Message>"
                        + "<RequestId>fake-request-id</RequestId><HostId>fake</HostId></Error>");
    }

    private static void respond(HttpExchange exchange, int status, String xml) throws IOException {
        byte[] body =
                ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xml)
                        .getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }
}
