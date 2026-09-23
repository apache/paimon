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

import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.exceptions.ServiceUnavailableException;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.responses.CreatePartitionsResponse;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a POST the server cannot absorb twice is sent exactly once, and that every other POST
 * keeps the 429/503 retry it has always had.
 *
 * <p>The server here refuses only the first attempt, so a retried request succeeds on its second
 * one: the request count separates "sent once" from "sent again" without waiting out five backoffs.
 */
public class HttpClientRetrySafetyTest {

    private static final String PATH = "/partitions";

    private static final Map<String, String> SPEC = Collections.singletonMap("dt", "20260728");

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
                        // request; this is exactly the shape that double counts an ADD report.
                        respond(exchange, 503, "{\"message\":\"busy\",\"code\":503}");
                    } else {
                        respond(exchange, 200, "{\"created\":[],\"existed\":[]}");
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
    public void testAnAddingReportIsNotRetried() {
        assertThatThrownBy(() -> post(request(false, statistics())))
                .isInstanceOf(ServiceUnavailableException.class);

        // Retrying would add the same increment a second time.
        assertThat(requests.get()).isEqualTo(1);
    }

    @Test
    public void testAReplacingReportIsRetried() {
        assertThat(post(request(true, statistics()))).isNotNull();

        // Replacing lands on the same value, so a second delivery changes nothing.
        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    public void testARequestCarryingNoReportIsRetried() {
        assertThat(post(new CreatePartitionsRequest(Collections.singletonList(SPEC)))).isNotNull();

        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    public void testARequestThatNeverHeardOfReportsKeepsItsRetry() {
        // Nearly every request type rides the interface default, so every case above would still
        // pass if that default flipped to false and took 429/503 retry away from every other POST
        // in the catalog. This is the only case that rides it.
        assertThat(new DefaultRetrySafety().isRetrySafe()).isTrue();
        assertThat(post(new DefaultRetrySafety())).isNotNull();

        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    public void testRetrySafetyNeverReachesTheWire() {
        // isRetrySafe is how the client treats the request, not something the server is told.
        // Without the @JsonIgnore on the interface default it would show up in every body.
        assertThat(RESTUtil.encodedBody(new DefaultRetrySafety())).doesNotContain("retrySafe");
    }

    @Test
    public void testAReportOfNoStatisticsIsRetried() {
        // Splitting a large create leaves batches with an empty statistics list; they increment
        // nothing, so they keep their retry.
        assertThat(post(request(false, Collections.emptyList()))).isNotNull();

        assertThat(requests.get()).isEqualTo(2);
    }

    @Test
    public void testOnlyANonEmptyAddingReportDeclaresItselfUnsafeToRetry() {
        assertThat(request(false, statistics()).isRetrySafe()).isFalse();
        assertThat(request(false, Collections.emptyList()).isRetrySafe()).isTrue();
        assertThat(request(true, statistics()).isRetrySafe()).isTrue();
        assertThat(new CreatePartitionsRequest(Collections.singletonList(SPEC)).isRetrySafe())
                .isTrue();
        // A request that leaves the flag out reports nothing, so it is replayable whatever the
        // flag would have said.
        assertThat(
                        new CreatePartitionsRequest(
                                        Collections.singletonList(SPEC), true, null, null)
                                .isRetrySafe())
                .isTrue();
    }

    /** A request that leaves {@link RESTRequest#isRetrySafe()} at its default, as nearly all do. */
    private static class DefaultRetrySafety implements RESTRequest {

        @JsonGetter("partitionSpecs")
        public List<Map<String, String>> getPartitionSpecs() {
            return Collections.singletonList(SPEC);
        }
    }

    private CreatePartitionsResponse post(RESTRequest request) {
        return client.post(PATH, request, CreatePartitionsResponse.class, null);
    }

    private static CreatePartitionsRequest request(
            boolean replaceStatistics, List<PartitionStatistics> statistics) {
        return new CreatePartitionsRequest(
                Collections.singletonList(SPEC), true, statistics, replaceStatistics);
    }

    private static List<PartitionStatistics> statistics() {
        return Collections.singletonList(new PartitionStatistics(SPEC, 3L, 300L, 1L, 1000L, -1));
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
