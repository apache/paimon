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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobIngestObservations;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.metrics.Counter;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricGroupImpl;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real HTTP, local Blob file, native batch commit, and snapshot-pinned readback. */
class BlobIngestNativeWorkloadTest extends TableTestBase {

    private static final String OPTION = "blob-ingest-observation.enabled";
    private static final String GROUP = "blobIngest";
    private static final int PREFIX = 41_007;

    @Test
    void testNativeSuccessAndOptionComparison() throws Exception {
        boolean baseline =
                Boolean.parseBoolean(System.getProperties().getProperty("io13.baseline"));
        int samples = Integer.parseInt(System.getProperties().getProperty("io13.samples", "4"));
        for (int sample = 0; sample < samples; sample++) {
            if (baseline) {
                verifyNativeSuccess("old-" + sample, "false", true, false);
            } else if (sample % 2 == 0) {
                verifyNativeSuccess("off-" + sample, "false", true, false);
                verifyNativeSuccess("on-" + sample, "true", true, true);
            } else {
                verifyNativeSuccess("on-" + sample, "true", true, true);
                verifyNativeSuccess("off-" + sample, "false", true, false);
            }
        }
        if (!baseline) {
            verifyNativeSuccess("invalid", "invalid", true, false);
            verifyNativeSuccess("no-reporter", "true", false, false);
        }
    }

    @Test
    void testHttpStatusAndPermanentPartial() throws Exception {
        byte[] data = payload();
        AtomicInteger missing = new AtomicInteger();
        AtomicInteger busy = new AtomicInteger();
        AtomicInteger partial = new AtomicInteger();
        AtomicInteger noResponse = new AtomicInteger();
        HttpServer server = server();
        serveFailure(server, "/missing", missing, 404);
        serveFailure(server, "/busy", busy, 429);
        server.createContext(
                "/disconnect",
                exchange -> {
                    noResponse.incrementAndGet();
                    exchange.close();
                });
        server.createContext(
                "/partial",
                exchange -> {
                    partial.incrementAndGet();
                    if (exchange.getRequestHeaders().getFirst("Range") == null) {
                        exchange.getResponseHeaders().add("ETag", "\"io13-fixture\"");
                        truncate(exchange, data);
                    } else {
                        exchange.sendResponseHeaders(404, -1);
                        exchange.close();
                    }
                });
        server.start();
        try {
            assertFailure(server, "/missing", "native-missing", "http_response_initial_get_404", 0);
            assertFailure(server, "/busy", "native-busy", "http_response_initial_get_429", 0);
            assertFailure(
                    server, "/partial", "native-partial", "http_response_range_resume_404", PREFIX);
            assertFailure(
                    server,
                    "/disconnect",
                    "native-no-response",
                    "http_response_initial_get_no_response",
                    0);
            assertThat(missing).hasValue(1);
            assertThat(busy.get()).isGreaterThan(1);
            assertThat(partial).hasValue(2);
            assertThat(noResponse.get()).isPositive();
            printSourceRequests("native-missing", missing);
            printSourceRequests("native-busy", busy);
            printSourceRequests("native-partial", partial);
            printSourceRequests("native-no-response", noResponse);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void testDisabledInnerTableDoesNotReportToOuterTable() throws Exception {
        for (String mode : Arrays.asList("append", "overwrite", "filter")) {
            List<String> outerEvents = new ArrayList<>();
            FileStoreTable table = table("disabled_inner_" + mode, "false");
            Registry registry = new Registry();
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BlobIngestObservations.Scope outer =
                    BlobIngestObservations.observe(
                            (stage, outcome, value) -> outerEvents.add(stage + "/" + outcome))) {
                BlobIngestObservations.record("outer", "before", 1);
                try (BatchTableWrite write = builder.newWrite();
                        TableCommitImpl commit = (TableCommitImpl) builder.newCommit()) {
                    write.withMetricRegistry(registry);
                    commit.withMetricRegistry(registry);
                    write.write(GenericRow.of(1, Blob.fromData(new byte[] {1, 2, 3})));
                    List<CommitMessage> messages = write.prepareCommit();
                    if ("overwrite".equals(mode)) {
                        commit.withOverwrite(Collections.emptyMap()).commit(messages);
                    } else if ("filter".equals(mode)) {
                        Map<Long, List<CommitMessage>> commits =
                                Collections.singletonMap(
                                        BatchWriteBuilder.COMMIT_IDENTIFIER, messages);
                        assertThat(commit.filterAndCommit(commits)).isEqualTo(1);
                        assertThat(commit.filterAndCommit(commits)).isZero();
                    } else {
                        commit.commit(messages);
                    }
                    assertThat(table.snapshotManager().latestSnapshot()).isNotNull();
                }
                BlobIngestObservations.record("outer", "after", 1);
            }
            assertThat(outerEvents).containsExactly("outer/before", "outer/after");
            assertThat(registry.groups).isEmpty();
            assertThat(BlobIngestObservations.enabled()).isFalse();
        }
    }

    @Test
    void testObservationRegistryFailuresPreserveNativeWriteAndCommit() throws Exception {
        byte[] data = payload();
        for (String failure : Arrays.asList("create", "record", "close")) {
            FileStoreTable table = table("registry_" + failure, "true");
            MetricRegistry registry =
                    (name, variables) -> {
                        if (!GROUP.equals(name)) {
                            return new MetricGroupImpl(name, variables);
                        }
                        if ("create".equals(failure)) {
                            throw new IllegalStateException("Cannot register observations");
                        }
                        return new MetricGroupImpl(name, variables) {
                            @Override
                            public Counter counter(String metricName) {
                                if ("record".equals(failure)) {
                                    throw new IllegalStateException("Cannot record observations");
                                }
                                return super.counter(metricName);
                            }

                            @Override
                            public void close() {
                                if ("close".equals(failure)) {
                                    throw new IllegalStateException("Cannot close observations");
                                }
                            }
                        };
                    };
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                write.withMetricRegistry(registry);
                commit.withMetricRegistry(registry);
                write.write(GenericRow.of(1, Blob.fromData(data)));
                commit.commit(write.prepareCommit());
            }
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            assertThat(snapshot).isNotNull();
            ReadBuilder read =
                    table.copy(
                                    Collections.singletonMap(
                                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                            Long.toString(snapshot.id())))
                            .newReadBuilder();
            List<byte[]> contents = new ArrayList<>();
            try (RecordReader<InternalRow> reader =
                    read.newRead().createReader(read.newScan().plan())) {
                reader.forEachRemaining(row -> contents.add(row.getBlob(1).toData()));
            }
            assertThat(contents).singleElement().isEqualTo(data);
            assertThat(BlobIngestObservations.enabled()).isFalse();
        }
    }

    @Test
    void testAsyncDestinationPreservesNativeObservation() throws Exception {
        verifyNativeSuccess("async", "true", true, true, true);
    }

    private void verifyNativeSuccess(String suffix, String option, boolean report, boolean enabled)
            throws Exception {
        verifyNativeSuccess(suffix, option, report, enabled, false);
    }

    private void verifyNativeSuccess(
            String suffix, String option, boolean report, boolean enabled, boolean async)
            throws Exception {
        String run = "native-" + suffix;
        byte[] data = payload();
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = server();
        server.createContext(
                "/plain",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 200, data);
                });
        server.createContext(
                "/resume",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.getResponseHeaders().add("ETag", "\"io13-fixture\"");
                    String range = exchange.getRequestHeaders().getFirst("Range");
                    if (range == null) {
                        truncate(exchange, data);
                        return;
                    }
                    if (!("bytes=" + PREFIX + "-").equals(range)
                            || !"\"io13-fixture\""
                                    .equals(exchange.getRequestHeaders().getFirst("If-Range"))) {
                        exchange.sendResponseHeaders(400, -1);
                        exchange.close();
                        return;
                    }
                    exchange.getResponseHeaders()
                            .add(
                                    "Content-Range",
                                    "bytes "
                                            + PREFIX
                                            + "-"
                                            + (data.length - 1)
                                            + "/"
                                            + data.length);
                    respond(exchange, 206, Arrays.copyOfRange(data, PREFIX, data.length));
                });
        server.start();
        try {
            FileStoreTable table = table(run, option, async);
            Registry registry = new Registry();
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            long startMillis = System.currentTimeMillis();
            Snapshot snapshot;
            long elapsed;
            long heapDelta;
            long readbackNanos;
            try (BatchTableWrite write = builder.newWrite();
                    BatchTableCommit commit = builder.newCommit()) {
                if (report) {
                    write.withMetricRegistry(registry);
                    commit.withMetricRegistry(registry);
                }
                long heapStart = usedHeap();
                long start = System.nanoTime();
                write.write(GenericRow.of(1, Blob.fromHttp(url(server, "/plain"))));
                write.write(GenericRow.of(2, Blob.fromHttp(url(server, "/resume"))));
                write.write(GenericRow.of(3, null));
                List<CommitMessage> messages = write.prepareCommit();
                Snapshot beforeCommit = table.snapshotManager().latestSnapshot();
                commit.commit(messages);
                elapsed = System.nanoTime() - start;
                heapDelta = usedHeap() - heapStart;
                assertThat(messages).isNotEmpty();
                assertThat(beforeCommit).isNull();
                snapshot = table.snapshotManager().latestSnapshot();
                assertThat(snapshot.commitIdentifier())
                        .isEqualTo(BatchWriteBuilder.COMMIT_IDENTIFIER);
                assertThat(snapshot.totalRecordCount()).isPositive();
                ReadBuilder read =
                        table.copy(
                                        Collections.singletonMap(
                                                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                                Long.toString(snapshot.id())))
                                .newReadBuilder();
                long readbackStart = System.nanoTime();
                Map<Integer, byte[]> contents = new HashMap<>();
                try (RecordReader<InternalRow> reader =
                        read.newRead().createReader(read.newScan().plan())) {
                    reader.forEachRemaining(
                            row ->
                                    contents.put(
                                            row.getInt(0),
                                            row.isNullAt(1) ? null : row.getBlob(1).toData()));
                }
                readbackNanos = System.nanoTime() - readbackStart;
                assertThat(contents).containsOnlyKeys(1, 2, 3);
                assertThat(contents.get(1)).isEqualTo(data);
                assertThat(contents.get(2)).isEqualTo(data);
                assertThat(contents.get(3)).isNull();
                assertThat(requests).hasValue(3);
                if (enabled) {
                    assertThat(registry.get("file_close_succeeded")).isEqualTo(1);
                    assertThat(registry.get("file_prepared_succeeded")).isEqualTo(1);
                    assertThat(registry.get("blob_read_started")).isEqualTo(2);
                    assertThat(registry.get("blob_read_completed")).isEqualTo(2);
                    assertThat(registry.get("blob_source_http")).isEqualTo(2);
                    assertThat(registry.get("blob_skipped_null")).isEqualTo(1);
                    assertThat(registry.get("http_attempt_initial_get")).isEqualTo(2);
                    assertThat(registry.get("http_response_initial_get_200")).isEqualTo(2);
                    assertThat(registry.get("http_response_range_resume_206")).isEqualTo(1);
                    assertThat(registry.get("http_retry_range_resume")).isEqualTo(1);
                    assertThat(registry.get("payload_read_observed")).isEqualTo(2L * data.length);
                    assertThat(registry.get("payload_written_observed"))
                            .isEqualTo(2L * data.length);
                    assertThat(registry.get("commit_publish_attempt")).isEqualTo(1);
                    assertThat(registry.get("commit_committed")).isEqualTo(1);
                    registry.dump(run);
                } else {
                    assertThat(registry.groups).isEmpty();
                }
            }
            System.out.printf(
                    "IO13_RESULT|%s|%d|%d|%d|%d|%d|%d|%d|%s|%d%n",
                    run,
                    startMillis,
                    elapsed,
                    heapDelta,
                    requests.get(),
                    snapshot.id(),
                    3,
                    2L * data.length,
                    digest(data),
                    readbackNanos);
        } finally {
            server.stop(0);
        }
    }

    private void assertFailure(
            HttpServer server, String path, String run, String response, int bytes)
            throws Exception {
        FileStoreTable table = table(run, "true");
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        Registry registry = new Registry();
        try (BatchTableWrite write = builder.newWrite()) {
            write.withMetricRegistry(registry);
            assertThatThrownBy(
                            () -> write.write(GenericRow.of(1, Blob.fromHttp(url(server, path)))))
                    .isInstanceOf(Exception.class);
            assertThat(registry.get(response)).isEqualTo(1);
            assertThat(registry.get("blob_read_started")).isEqualTo(1);
            assertThat(registry.get("blob_read_failed")).isEqualTo(1);
            assertThat(registry.get("payload_read_observed")).isEqualTo(bytes);
            assertThat(registry.get("payload_read_incomplete")).isEqualTo(bytes);
            assertThat(registry.get("file_close_succeeded")).isZero();
            assertThat(registry.get("file_prepared_succeeded")).isZero();
            assertThat(table.snapshotManager().latestSnapshot()).isNull();
            registry.dump(run);
        }
    }

    private FileStoreTable table(String run, String option) throws Exception {
        return table(run, option, false);
    }

    private FileStoreTable table(String run, String option, boolean async) throws Exception {
        String name = run.replace('-', '_');
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.BLOB())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.ASYNC_FILE_WRITE.key(), Boolean.toString(async))
                        .option(OPTION, option)
                        .build();
        catalog.createTable(identifier(name), schema, false);
        return (FileStoreTable) catalog.getTable(identifier(name));
    }

    private static HttpServer server() throws IOException {
        return HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    }

    private static String url(HttpServer server, String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }

    private static void serveFailure(
            HttpServer server, String path, AtomicInteger requests, int status) {
        server.createContext(
                path,
                exchange -> {
                    requests.incrementAndGet();
                    if (status == 429) {
                        exchange.getResponseHeaders().add("Retry-After", "1");
                    }
                    exchange.sendResponseHeaders(status, -1);
                    exchange.close();
                });
    }

    private static void truncate(HttpExchange exchange, byte[] data) throws IOException {
        exchange.sendResponseHeaders(200, data.length);
        OutputStream out = exchange.getResponseBody();
        out.write(data, 0, PREFIX);
        out.flush();
        exchange.close();
    }

    private static void respond(HttpExchange exchange, int status, byte[] data) throws IOException {
        exchange.sendResponseHeaders(status, data.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(data);
        }
    }

    private static byte[] payload() {
        byte[] data = new byte[64 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (31 * i ^ (i >>> 8));
        }
        return data;
    }

    private static String digest(byte[] bytes) throws Exception {
        StringBuilder value = new StringBuilder();
        for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
            value.append(Character.forDigit((b & 0xff) >>> 4, 16));
            value.append(Character.forDigit(b & 15, 16));
        }
        return value.toString();
    }

    private static void printSourceRequests(String run, AtomicInteger requests) {
        System.out.printf("IO13_SOURCE|%s|%d%n", run, requests.get());
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static final class Registry implements MetricRegistry {

        private final List<MetricGroup> groups = new ArrayList<>();

        @Override
        public MetricGroup createMetricGroup(String name, Map<String, String> variables) {
            MetricGroupImpl group = new MetricGroupImpl(name, variables);
            if (GROUP.equals(name)) {
                groups.add(group);
            }
            return group;
        }

        private long get(String key) {
            return groups.stream()
                    .map(group -> group.getMetrics().get(key))
                    .filter(Counter.class::isInstance)
                    .mapToLong(metric -> ((Counter) metric).getCount())
                    .sum();
        }

        private void dump(String run) {
            long now = System.currentTimeMillis();
            groups.stream()
                    .flatMap(group -> group.getMetrics().keySet().stream())
                    .distinct()
                    .sorted()
                    .forEach(
                            key ->
                                    System.out.printf(
                                            "IO13_METRIC|%s|%d|%s|%d%n", run, now, key, get(key)));
        }
    }
}
