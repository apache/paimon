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

import org.apache.paimon.utils.SensitiveConfigUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.util.TimeValue;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void testConfiguredKeepAliveTimeoutCapsServerTimeout() {
        assertThat(keepAliveDuration(Duration.ofSeconds(60), "timeout=75").toSeconds())
                .isEqualTo(60);
    }

    @Test
    public void testConfiguredKeepAliveTimeoutKeepsShorterServerTimeout() {
        assertThat(keepAliveDuration(Duration.ofSeconds(60), "timeout=30").toSeconds())
                .isEqualTo(30);
    }

    @Test
    public void testConfiguredKeepAliveTimeoutIsFallbackWithoutServerHeader() {
        assertThat(keepAliveDuration(Duration.ofSeconds(60), null).toSeconds()).isEqualTo(60);
    }

    @Test
    public void testConfiguredKeepAliveTimeoutCapsIndefiniteServerTimeout() {
        assertThat(keepAliveDuration(Duration.ofSeconds(60), "timeout=-1").toSeconds())
                .isEqualTo(60);
    }

    @Test
    public void testUnconfiguredKeepAlivePreservesHttpClientDefault() {
        BasicClassicHttpResponse response = new BasicClassicHttpResponse(200);
        HttpClientContext context = HttpClientContext.create();
        context.setRequestConfig(RequestConfig.custom().build());

        assertThat(
                        HttpClientUtils.KEEP_ALIVE_STRATEGY
                                .getKeepAliveDuration(response, context)
                                .toMinutes())
                .isEqualTo(3);
    }

    @Test
    public void testRejectNonPositiveKeepAliveTimeout() {
        assertThatThrownBy(() -> HttpClientUtils.getAsInputStream(url("/ok"), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than 0");
        assertThatThrownBy(() -> HttpClientUtils.exists(url("/ok"), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than 0");
    }

    @Test
    public void testConfiguredKeepAliveUsesDedicatedPoolAndClosesOlderIdleConnection()
            throws Exception {
        List<Integer> remotePorts = new CopyOnWriteArrayList<>();
        registerHandler(
                "/pool-state",
                exchange -> {
                    remotePorts.add(exchange.getRemoteAddress().getPort());
                    respond(exchange, 200, "ok".getBytes());
                });

        readAllAndClose(url("/pool-state"), null);
        readAllAndClose(url("/pool-state"), Duration.ofSeconds(10));
        readAllAndClose(url("/pool-state"), Duration.ofSeconds(10));
        Thread.sleep(150);
        readAllAndClose(url("/pool-state"), Duration.ofMillis(50));

        assertThat(remotePorts).hasSize(4);
        assertThat(remotePorts.get(1)).isNotEqualTo(remotePorts.get(0));
        assertThat(remotePorts.get(2)).isEqualTo(remotePorts.get(1));
        assertThat(remotePorts.get(3)).isNotEqualTo(remotePorts.get(2));
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

        assertThat(HttpClientUtils.exists(url("/no-head"), Duration.ofSeconds(60))).isTrue();
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

    @Test
    public void testGetAsInputStreamThrowsForNotFound() {
        registerHandler(
                "/get-missing",
                exchange -> {
                    respond(exchange, 404, new byte[0]);
                });

        assertThatThrownBy(() -> HttpClientUtils.getAsInputStream(url("/get-missing")))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("HTTP error code: 404");
    }

    @Test
    public void testInvalidUriExceptionDoesNotLeakCredentials() {
        String uri = "https://alice:secret@host/bad path?sig=QUERY_SECRET";
        for (ThrowableAssert.ThrowingCallable call :
                new ThrowableAssert.ThrowingCallable[] {
                    () -> HttpClientUtils.exists(uri), () -> HttpClientUtils.getAsInputStream(uri)
                }) {
            assertThatThrownBy(call)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasNoCause()
                    .matches(e -> HttpClientUtils.isInvalidUriException(e))
                    .satisfies(
                            e -> {
                                assertThat(String.valueOf(e)).doesNotContain("secret");
                                assertThat(String.valueOf(e)).doesNotContain("QUERY_SECRET");
                            });
        }
    }

    @Test
    public void testExecuteFailureDoesNotLeakRedirectLocation() {
        String secretLocation = url("/redirect") + "?sig=REDIRECT_SECRET";
        registerHandler(
                "/redirect",
                exchange -> {
                    exchange.getResponseHeaders().add("Location", secretLocation);
                    respond(exchange, 302, new byte[0]);
                });

        for (ThrowableAssert.ThrowingCallable call :
                new ThrowableAssert.ThrowingCallable[] {
                    () -> HttpClientUtils.getAsInputStream(url("/redirect")),
                    () -> HttpClientUtils.exists(url("/redirect"))
                }) {
            assertThatThrownBy(call)
                    .isInstanceOf(IOException.class)
                    .hasNoCause()
                    .satisfies(
                            e -> {
                                assertThat(String.valueOf(e)).doesNotContain("REDIRECT_SECRET");
                                assertThat(e.getMessage()).doesNotContain("sig=");
                            });
        }
    }

    @Test
    public void testGetAsInputStreamDoesNotLeakConnectionsOnRepeatedNotFound() throws Exception {
        registerHandler(
                "/missing",
                exchange -> {
                    respond(exchange, 404, new byte[0]);
                });
        registerHandler(
                "/ok",
                exchange -> {
                    respond(exchange, 200, "x".getBytes());
                });

        for (int i = 0; i < 120; i++) {
            assertThatThrownBy(() -> HttpClientUtils.getAsInputStream(url("/missing")))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("HTTP error code: 404");
        }

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/ok"))) {
            assertThat(in.read()).isEqualTo('x');
        }
    }

    @Test
    public void testGetAsInputStreamPreservesContentDecoding() throws Exception {
        byte[] payload = payload(4096);
        byte[] compressed = gzip(payload);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/encoded",
                exchange -> {
                    if (requestCount.incrementAndGet() > 1) {
                        respond(exchange, 410, new byte[0]);
                        return;
                    }
                    exchange.getResponseHeaders().add("Content-Encoding", "gzip");
                    respond(exchange, 200, compressed);
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/encoded"))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(1);
    }

    @Test
    public void testGetAsInputStreamFallsBackWhenIdentityEncodingIsRejected() throws Exception {
        byte[] payload = payload(4096);
        byte[] compressed = gzip(payload);
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> firstAcceptEncoding = new AtomicReference<>();
        AtomicReference<String> secondAcceptEncoding = new AtomicReference<>();
        registerHandler(
                "/encoded-only",
                exchange -> {
                    int currentRequest = requestCount.incrementAndGet();
                    String acceptEncoding =
                            exchange.getRequestHeaders().getFirst("Accept-Encoding");
                    if (currentRequest == 1) {
                        firstAcceptEncoding.set(acceptEncoding);
                        respond(exchange, 406, new byte[0]);
                        return;
                    }

                    secondAcceptEncoding.set(acceptEncoding);
                    exchange.getResponseHeaders().add("Content-Encoding", "gzip");
                    respond(exchange, 200, compressed);
                });

        try (InputStream in =
                HttpClientUtils.getAsInputStream(url("/encoded-only"), Duration.ofSeconds(60))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(2);
        assertThat(firstAcceptEncoding).hasValue("identity");
        assertThat(secondAcceptEncoding.get()).contains("gzip");
    }

    @Test
    public void testGetAsInputStreamDoesNotResumeTruncatedEncodedResponse() throws Exception {
        byte[] compressed = gzip(payload(4096));
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/truncated-encoded",
                exchange -> {
                    requestCount.incrementAndGet();
                    exchange.getResponseHeaders().add("Content-Encoding", "gzip");
                    respondTruncated(
                            exchange, 200, compressed.length, compressed, compressed.length / 2);
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/truncated-encoded"))) {
            assertThatThrownBy(() -> readAll(in))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("encoded response bodies cannot be resumed safely");
        }
        assertThat(requestCount).hasValue(1);
    }

    @Test
    public void testGetAsInputStreamResumesTruncatedResponseBody() throws Exception {
        byte[] payload = payload(128 * 1024);
        int truncatedLength = 89_075;
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifRange = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        registerHandler(
                "/truncated",
                exchange -> {
                    int currentRequest = requestCount.incrementAndGet();
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (currentRequest == 1) {
                        respondTruncated(exchange, payload, truncatedLength);
                        return;
                    }

                    range.set(exchange.getRequestHeaders().getFirst("Range"));
                    ifRange.set(exchange.getRequestHeaders().getFirst("If-Range"));
                    acceptEncoding.set(exchange.getRequestHeaders().getFirst("Accept-Encoding"));
                    byte[] remaining = Arrays.copyOfRange(payload, truncatedLength, payload.length);
                    exchange.getResponseHeaders()
                            .add(
                                    "Content-Range",
                                    String.format(
                                            "bytes %d-%d/%d",
                                            truncatedLength, payload.length - 1, payload.length));
                    respond(exchange, 206, remaining);
                });

        try (InputStream in =
                HttpClientUtils.getAsInputStream(url("/truncated"), Duration.ofSeconds(60))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(2);
        assertThat(range).hasValue("bytes=" + truncatedLength + "-");
        assertThat(ifRange).hasValue("\"image-v1\"");
        assertThat(acceptEncoding).hasValue("identity");
    }

    @Test
    public void testGetAsInputStreamReplaysAndVerifiesWithoutResourceValidator() throws Exception {
        byte[] payload = payload(4096);
        int truncatedLength = 1024;
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> acceptEncoding = new AtomicReference<>();
        registerHandler(
                "/truncated-no-validator",
                exchange -> {
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, truncatedLength);
                        return;
                    }

                    range.set(exchange.getRequestHeaders().getFirst("Range"));
                    acceptEncoding.set(exchange.getRequestHeaders().getFirst("Accept-Encoding"));
                    respond(exchange, 200, payload);
                });

        try (InputStream in =
                HttpClientUtils.getAsInputStream(
                        url("/truncated-no-validator"), Duration.ofSeconds(60))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(2);
        assertThat(range.get()).isNull();
        assertThat(acceptEncoding).hasValue("identity");
    }

    @Test
    public void testGetAsInputStreamReadsChunkedReplayPastInitialContentLength() throws Exception {
        byte[] replayedPayload = payload(120);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/longer-chunked-replay",
                exchange -> {
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, 200, 100, replayedPayload, 40);
                    } else {
                        respondChunked(exchange, 200, replayedPayload);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/longer-chunked-replay"))) {
            assertThat(readAll(in)).isEqualTo(replayedPayload);
        }
        assertThat(requestCount).hasValue(2);
    }

    @Test
    public void testGetAsInputStreamReplaysWhenOnlyLastModifiedIsAvailable() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifRange = new AtomicReference<>();
        registerHandler(
                "/last-modified-only",
                exchange -> {
                    exchange.getResponseHeaders()
                            .add("Last-Modified", "Mon, 17 Aug 2026 00:00:00 GMT");
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 1024);
                    } else {
                        range.set(exchange.getRequestHeaders().getFirst("Range"));
                        ifRange.set(exchange.getRequestHeaders().getFirst("If-Range"));
                        respond(exchange, 200, payload);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/last-modified-only"))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(2);
        assertThat(range.get()).isNull();
        assertThat(ifRange.get()).isNull();
    }

    @Test
    public void testGetAsInputStreamReplaysWhenOnlyWeakEtagIsAvailable() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> range = new AtomicReference<>();
        AtomicReference<String> ifRange = new AtomicReference<>();
        registerHandler(
                "/weak-etag",
                exchange -> {
                    exchange.getResponseHeaders().add("ETag", "W/\"image-v1\"");
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 1024);
                    } else {
                        range.set(exchange.getRequestHeaders().getFirst("Range"));
                        ifRange.set(exchange.getRequestHeaders().getFirst("If-Range"));
                        respond(exchange, 200, payload);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/weak-etag"))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(2);
        assertThat(range.get()).isNull();
        assertThat(ifRange.get()).isNull();
    }

    @Test
    public void testGetAsInputStreamRejectsChangedReplayWithoutResourceValidator()
            throws Exception {
        byte[] payload = payload(4096);
        byte[] changedPayload = payload.clone();
        changedPayload[17] ^= 1;
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/changed-replay",
                exchange -> {
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 1024);
                    } else {
                        respond(exchange, 200, changedPayload);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/changed-replay"))) {
            assertThatThrownBy(() -> readAll(in))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("resource content changed");
        }
        assertThat(requestCount).hasValue(2);
    }

    @Test
    public void testGetAsInputStreamResumesMultipleTruncatedBodies() throws Exception {
        byte[] payload = payload(16 * 1024);
        int firstEnd = 2048;
        int secondEnd = 3072;
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> firstRange = new AtomicReference<>();
        AtomicReference<String> secondRange = new AtomicReference<>();
        registerHandler(
                "/multiple-truncations",
                exchange -> {
                    int currentRequest = requestCount.incrementAndGet();
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (currentRequest == 1) {
                        respondTruncated(exchange, payload, firstEnd);
                        return;
                    }

                    int start = currentRequest == 2 ? firstEnd : secondEnd;
                    if (currentRequest == 2) {
                        firstRange.set(exchange.getRequestHeaders().getFirst("Range"));
                    } else {
                        secondRange.set(exchange.getRequestHeaders().getFirst("Range"));
                    }
                    exchange.getResponseHeaders()
                            .add(
                                    "Content-Range",
                                    String.format(
                                            "bytes %d-%d/%d",
                                            start, payload.length - 1, payload.length));
                    byte[] remaining = Arrays.copyOfRange(payload, start, payload.length);
                    if (currentRequest == 2) {
                        respondTruncated(exchange, 206, remaining.length, remaining, 1024);
                    } else {
                        respond(exchange, 206, remaining);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/multiple-truncations"))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(3);
        assertThat(firstRange).hasValue("bytes=" + firstEnd + "-");
        assertThat(secondRange).hasValue("bytes=" + secondEnd + "-");
    }

    @Test
    public void testGetAsInputStreamContinuesAfterBoundedRangeResponse() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        AtomicReference<String> lastRange = new AtomicReference<>();
        registerHandler(
                "/bounded-range",
                exchange -> {
                    int currentRequest = requestCount.incrementAndGet();
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (currentRequest == 1) {
                        respondTruncated(exchange, payload, 1024);
                    } else if (currentRequest == 2) {
                        exchange.getResponseHeaders().add("Content-Range", "bytes 1024-2047/4096");
                        respond(exchange, 206, Arrays.copyOfRange(payload, 1024, 2048));
                    } else {
                        lastRange.set(exchange.getRequestHeaders().getFirst("Range"));
                        exchange.getResponseHeaders().add("Content-Range", "bytes 2048-4095/4096");
                        respond(exchange, 206, Arrays.copyOfRange(payload, 2048, 4096));
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/bounded-range"))) {
            assertThat(readAll(in)).isEqualTo(payload);
        }
        assertThat(requestCount).hasValue(3);
        assertThat(lastRange).hasValue("bytes=2048-");
    }

    @Test
    public void testGetAsInputStreamFailsWhenServerIgnoresRange() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/ignore-range",
                exchange -> {
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 1024);
                    } else {
                        respond(exchange, 200, payload);
                    }
                });

        String uri = url("/ignore-range") + "?sig=BODY_RESUME_SECRET";
        try (InputStream in = HttpClientUtils.getAsInputStream(uri)) {
            assertThatThrownBy(() -> readAll(in))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("did not honor the range request")
                    .satisfies(
                            e -> {
                                assertThat(String.valueOf(e)).doesNotContain("BODY_RESUME_SECRET");
                                assertThat(e.getMessage()).doesNotContain("sig=");
                            });
        }
        assertThat(requestCount).hasValue(2);
    }

    @Test
    public void testGetAsInputStreamFailsForMismatchedContentRange() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/wrong-content-range",
                exchange -> {
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 1024);
                        return;
                    }

                    exchange.getResponseHeaders().add("Content-Range", "bytes 1023-4095/4096");
                    respond(exchange, 206, Arrays.copyOfRange(payload, 1023, payload.length));
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/wrong-content-range"))) {
            assertThatThrownBy(() -> readAll(in))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("resumed at byte 1023 instead of 1024");
        }
        assertThat(requestCount).hasValue(2);
    }

    @Test
    public void testGetAsInputStreamFailsAfterBoundedResumeAttempts() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/resume-exhausted",
                exchange -> {
                    int currentRequest = requestCount.incrementAndGet();
                    exchange.getResponseHeaders().add("ETag", "\"image-v1\"");
                    if (currentRequest == 1) {
                        respondTruncated(exchange, payload, 1024);
                        return;
                    }

                    int start = 1024 + currentRequest - 2;
                    exchange.getResponseHeaders()
                            .add(
                                    "Content-Range",
                                    String.format(
                                            "bytes %d-%d/%d",
                                            start, payload.length - 1, payload.length));
                    byte[] remaining = Arrays.copyOfRange(payload, start, payload.length);
                    respondTruncated(exchange, 206, remaining.length, remaining, 1);
                });

        String uri = url("/resume-exhausted") + "?sig=BODY_RESUME_SECRET";
        try (InputStream in = HttpClientUtils.getAsInputStream(uri)) {
            assertThatThrownBy(() -> readAll(in))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("after 5 resume attempts")
                    .hasMessageContaining(url("/resume-exhausted"))
                    .hasMessageContaining("position=1029")
                    .hasMessageContaining("contentLength=4096")
                    .hasMessageContaining("recoveryAttempts=5")
                    .satisfies(
                            e -> {
                                assertThat(String.valueOf(e)).doesNotContain("BODY_RESUME_SECRET");
                                assertThat(e.getMessage()).doesNotContain("sig=");
                            });
            assertThatThrownBy(in::read)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("after 5 resume attempts");
        }
        assertThat(requestCount).hasValue(6);
    }

    @Test
    public void testGetAsInputStreamDoesNotResumeAfterClose() throws Exception {
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/closed-stream",
                exchange -> {
                    requestCount.incrementAndGet();
                    respond(exchange, 200, payload(4096));
                });

        InputStream in = HttpClientUtils.getAsInputStream(url("/closed-stream"));
        assertThat(in.read()).isNotNegative();
        in.close();
        assertThatThrownBy(in::read)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stream is closed");
        assertThat(requestCount).hasValue(1);
    }

    @Test
    public void testGetAsInputStreamKeepsRestartStatusFailureTerminal() throws Exception {
        byte[] payload = payload(4096);
        AtomicInteger requestCount = new AtomicInteger();
        registerHandler(
                "/restart-status",
                exchange -> {
                    if (requestCount.incrementAndGet() == 1) {
                        respondTruncated(exchange, payload, 0);
                    } else {
                        respond(exchange, 404, new byte[0]);
                    }
                });

        try (InputStream in = HttpClientUtils.getAsInputStream(url("/restart-status"))) {
            assertThatThrownBy(in::read)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("server returned HTTP 404");
            assertThatThrownBy(in::read)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("server returned HTTP 404");
        }
        assertThat(requestCount).hasValue(2);
    }

    @Test
    public void testIsNotFoundError() {
        RuntimeException exception =
                new RuntimeException("wrapper", new RuntimeException("HTTP error code: 404"));
        assertThat(HttpClientUtils.isNotFoundError(exception)).isTrue();
        assertThat(HttpClientUtils.isNotFoundError(new RuntimeException("HTTP error code: 500")))
                .isFalse();
    }

    @Test
    public void testGetHttpStatusCodeFromUnexpectedStatusIOException() {
        IOException exception =
                new IOException("Unexpected HTTP status code: 400 for uri: http://127.0.0.1/test");
        assertThat(HttpClientUtils.getHttpStatusCode(exception)).isEqualTo(400);
    }

    @Test
    public void testIsInvalidUriException() {
        assertThat(
                        HttpClientUtils.isInvalidUriException(
                                new IllegalArgumentException("Illegal character in path")))
                .isTrue();
        // The shared invalid-URI exception must classify uniformly across modules.
        assertThat(
                        HttpClientUtils.isInvalidUriException(
                                SensitiveConfigUtils.invalidUri("https://host/bad path")))
                .isTrue();
        assertThat(
                        HttpClientUtils.isInvalidUriException(
                                new RuntimeException("HTTP error code: 404")))
                .isFalse();
    }

    @Test
    public void testExistsThrowsForBadRequest() {
        registerHandler(
                "/bad-request",
                exchange -> {
                    respond(exchange, 400, new byte[0]);
                });

        assertThatThrownBy(() -> HttpClientUtils.exists(url("/bad-request")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unexpected HTTP status code: 400");
    }

    @Test
    public void testExistsThrowsForRateLimitOnHead() {
        registerHandler(
                "/rate-limit",
                exchange -> {
                    respond(exchange, 420, new byte[0]);
                });

        assertThatThrownBy(() -> HttpClientUtils.exists(url("/rate-limit")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unexpected HTTP status code: 420");
    }

    private void registerHandler(String path, HttpHandler handler) {
        server.createContext(path, handler);
    }

    private static TimeValue keepAliveDuration(Duration cap, String serverKeepAlive) {
        TimeValue timeout = TimeValue.of(cap);
        BasicClassicHttpResponse response = new BasicClassicHttpResponse(200);
        if (serverKeepAlive != null) {
            response.addHeader(HttpHeaders.KEEP_ALIVE, serverKeepAlive);
        }
        HttpClientContext context = HttpClientContext.create();
        context.setRequestConfig(RequestConfig.custom().setConnectionKeepAlive(timeout).build());
        context.setAttribute(HttpClientUtils.KEEP_ALIVE_TIMEOUT_ATTRIBUTE, timeout);
        return HttpClientUtils.KEEP_ALIVE_STRATEGY.getKeepAliveDuration(response, context);
    }

    private static void readAllAndClose(String uri, Duration keepAliveTimeout) throws IOException {
        try (InputStream inputStream =
                keepAliveTimeout == null
                        ? HttpClientUtils.getAsInputStream(uri)
                        : HttpClientUtils.getAsInputStream(uri, keepAliveTimeout)) {
            readAll(inputStream);
        }
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

    private static void respondTruncated(HttpExchange exchange, byte[] body, int truncatedLength)
            throws IOException {
        respondTruncated(exchange, 200, body.length, body, truncatedLength);
    }

    private static void respondTruncated(
            HttpExchange exchange,
            int statusCode,
            long declaredLength,
            byte[] body,
            int truncatedLength)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, declaredLength);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(body, 0, truncatedLength);
        outputStream.flush();
        exchange.close();
    }

    private static void respondChunked(HttpExchange exchange, int statusCode, byte[] body)
            throws IOException {
        exchange.sendResponseHeaders(statusCode, 0);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(body);
        }
    }

    private static byte[] payload(int length) {
        byte[] payload = new byte[length];
        new Random(20260817L).nextBytes(payload);
        return payload;
    }

    private static byte[] gzip(byte[] payload) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            gzipOutputStream.write(payload);
        }
        return outputStream.toByteArray();
    }

    private static byte[] readAll(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) >= 0) {
            outputStream.write(buffer, 0, bytesRead);
        }
        return outputStream.toByteArray();
    }
}
