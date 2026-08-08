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

package org.apache.paimon.service.server;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.LegacyFailureFramePolicy;
import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.NetworkServer;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.query.DataEvolutionGlobalIndexTableQuery;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.netty4.io.netty.buffer.Unpooled;
import org.apache.paimon.shade.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.TooLongFrameException;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INTERNAL_ERROR;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.NOT_READY;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.OVERLOADED;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.STALE_GENERATION;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNKNOWN_KEY_SHARD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Network-level tests for the dedicated global-index server protocol and fences. */
class GlobalIndexQueryServerTest extends TableTestBase {

    private static final String SERVER_EPOCH = "server-epoch";
    private static final int SERVER_ID = 0;
    private static final int NUM_SERVERS = 2;

    @Test
    void testLookupAndStructuredFenceFailures() throws Throwable {
        FileStoreTable table = createAppendTable();
        DataEvolutionGlobalIndexTableQuery query =
                new DataEvolutionGlobalIndexTableQuery(
                        table,
                        "url",
                        Collections.singletonList("descriptor"),
                        new File(tempPath.toFile(), "server-query-state"));
        GlobalIndexQueryServer server =
                new GlobalIndexQueryServer(
                        SERVER_ID,
                        NUM_SERVERS,
                        SERVER_EPOCH,
                        "127.0.0.1",
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        NetworkClient<GlobalIndexRequest, GlobalIndexResponse> client =
                new NetworkClient<>(
                        "Global Index Query Server Test Client",
                        1,
                        new MessageSerializer<>(
                                new GlobalIndexRequest.Deserializer(),
                                new GlobalIndexResponse.Deserializer()),
                        new DisabledServiceRequestStats(),
                        GlobalIndexResponse.MAX_NETWORK_FRAME_BYTES);
        try {
            assertRequestFrameLimit(server);
            server.start();
            BinaryRow ownedKey = keyForShard(SERVER_ID);
            BinaryRow foreignKey = keyForShard(1);

            GlobalIndexRequest unsupportedRequest =
                    new GlobalIndexRequest(
                            SERVER_EPOCH, Long.MIN_VALUE, new BinaryRow[] {ownedKey}) {
                        @Override
                        public byte[] serialize() {
                            byte[] bytes = super.serialize();
                            ByteBuffer.wrap(bytes).putInt(GlobalIndexRequest.PROTOCOL_VERSION + 1);
                            return bytes;
                        }
                    };
            assertMalformedFrameClosesConnection(
                    sendMalformedRequest(server.getServerAddress(), unsupportedRequest),
                    "Unsupported global-index");

            GlobalIndexRequest trailingBytesRequest =
                    new GlobalIndexRequest(
                            SERVER_EPOCH, Long.MIN_VALUE, new BinaryRow[] {ownedKey}) {
                        @Override
                        public byte[] serialize() {
                            byte[] bytes = super.serialize();
                            return Arrays.copyOf(bytes, bytes.length + 1);
                        }
                    };
            assertMalformedFrameClosesConnection(
                    sendMalformedRequest(server.getServerAddress(), trailingBytesRequest),
                    "trailing bytes");

            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest(
                                    SERVER_EPOCH, Long.MIN_VALUE, new BinaryRow[] {ownedKey})),
                    NOT_READY,
                    false);

            query.beginRefresh(5L, 100L);
            query.put(5L, ownedKey, value("descriptor"));
            query.finishRefresh(5L);

            GlobalIndexResponse response =
                    client.sendRequest(
                                    server.getServerAddress(),
                                    new GlobalIndexRequest(
                                            SERVER_EPOCH, 5L, new BinaryRow[] {ownedKey}))
                            .get(10L, TimeUnit.SECONDS);
            assertThat(response.serverEpoch()).isEqualTo(SERVER_EPOCH);
            assertThat(response.servedGeneration()).isEqualTo(5L);
            assertThat(response.servedSnapshotId()).isEqualTo(100L);
            assertThat(response.values()).hasSize(1);
            assertThat(response.values()[0].getString(0).toString()).isEqualTo("descriptor");

            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest("old-epoch", 5L, new BinaryRow[] {ownedKey})),
                    STALE_GENERATION,
                    true);
            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest(SERVER_EPOCH, 4L, new BinaryRow[] {ownedKey})),
                    STALE_GENERATION,
                    true);
            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest(SERVER_EPOCH, 5L, new BinaryRow[] {foreignKey})),
                    UNKNOWN_KEY_SHARD,
                    true);

            // Observing a newer generation immediately fences a cached generation-5 descriptor.
            // The old state stays allocated for shadow refresh, but it must not answer a key from
            // an uncovered append tail with null.
            query.beginRefresh(6L, 101L);
            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest(SERVER_EPOCH, 5L, new BinaryRow[] {ownedKey})),
                    STALE_GENERATION,
                    true);
            assertFailureResponse(
                    client.sendRequest(
                            server.getServerAddress(),
                            new GlobalIndexRequest(SERVER_EPOCH, 6L, new BinaryRow[] {ownedKey})),
                    NOT_READY,
                    false);

        } finally {
            client.shutdown().get(10L, TimeUnit.SECONDS);
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
            query.close();
        }
    }

    @Test
    void testBoundedQueryQueueReturnsStructuredOverloadedResponse() throws Throwable {
        FileStoreTable table = createAppendTable();
        BinaryRow ownedKey = keyForShard(SERVER_ID);
        BlockingQuery query =
                new BlockingQuery(table, new File(tempPath.toFile(), "bounded-query-state"));
        query.beginRefresh(5L, 100L);
        query.put(5L, ownedKey, value("descriptor"));
        query.finishRefresh(5L);

        GlobalIndexQueryServer server =
                new GlobalIndexQueryServer(
                        SERVER_ID,
                        NUM_SERVERS,
                        SERVER_EPOCH,
                        "127.0.0.1",
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        NetworkClient<GlobalIndexRequest, GlobalIndexResponse> client = boundedClient();
        try {
            server.start();
            GlobalIndexRequest request =
                    new GlobalIndexRequest(SERVER_EPOCH, 5L, new BinaryRow[] {ownedKey});
            CompletableFuture<GlobalIndexResponse> running =
                    client.sendRequest(server.getServerAddress(), request);
            assertThat(query.awaitBlocked()).isTrue();

            CompletableFuture<GlobalIndexResponse> queued =
                    client.sendRequest(server.getServerAddress(), request);
            awaitQueuedRequest(server);
            GlobalIndexResponse overloaded =
                    client.sendRequest(server.getServerAddress(), request)
                            .get(10L, TimeUnit.SECONDS);

            assertFailureResponse(overloaded, OVERLOADED, true);
            query.release();
            assertThat(running.get(10L, TimeUnit.SECONDS).isSuccess()).isTrue();
            assertThat(queued.get(10L, TimeUnit.SECONDS).isSuccess()).isTrue();
        } finally {
            query.release();
            client.shutdown().get(10L, TimeUnit.SECONDS);
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
            query.close();
        }
    }

    @Test
    void testRepeatedLargeValueFailsBeforeResponseConstructionAndDeduplicatesLookup()
            throws Exception {
        FileStoreTable table = createAppendTable(DataTypes.BYTES());
        BinaryRow ownedKey = keyForShard(SERVER_ID);
        BinaryRow largeValue = row(new byte[8 * 1024 * 1024], DataTypes.BYTES());
        ConstantQuery query =
                new ConstantQuery(
                        table, new File(tempPath.toFile(), "large-value-query-state"), largeValue);
        GlobalIndexQueryServer server =
                new GlobalIndexQueryServer(
                        SERVER_ID,
                        NUM_SERVERS,
                        SERVER_EPOCH,
                        "127.0.0.1",
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        GlobalIndexServerHandler handler =
                new GlobalIndexServerHandler(
                        server,
                        SERVER_ID,
                        NUM_SERVERS,
                        SERVER_EPOCH,
                        query,
                        serializer(),
                        new DisabledServiceRequestStats());
        BinaryRow[] duplicateKeys = new BinaryRow[9];
        Arrays.fill(duplicateKeys, ownedKey);

        GlobalIndexResponse response =
                handler.handleRequest(42L, new GlobalIndexRequest(SERVER_EPOCH, 5L, duplicateKeys))
                        .join();

        assertFailureResponse(response, REQUEST_TOO_LARGE, false);
        assertThat(query.lookupCount()).isOne();
        query.close();
    }

    @Test
    void testUnexpectedLookupFailureUsesStructuredInternalErrorResponse() throws Throwable {
        FileStoreTable table = createAppendTable();
        BinaryRow ownedKey = keyForShard(SERVER_ID);
        FailingQuery query =
                new FailingQuery(table, new File(tempPath.toFile(), "failing-query-state"));
        GlobalIndexQueryServer server =
                new GlobalIndexQueryServer(
                        SERVER_ID,
                        NUM_SERVERS,
                        SERVER_EPOCH,
                        "127.0.0.1",
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        NetworkClient<GlobalIndexRequest, GlobalIndexResponse> client = boundedClient();
        try {
            server.start();
            GlobalIndexResponse response =
                    client.sendRequest(
                                    server.getServerAddress(),
                                    new GlobalIndexRequest(
                                            SERVER_EPOCH, 5L, new BinaryRow[] {ownedKey}))
                            .get(10L, TimeUnit.SECONDS);

            assertFailureResponse(response, INTERNAL_ERROR, false);
            assertThat(response.errorMessage()).doesNotContain("secret lookup failure");
        } finally {
            client.shutdown().get(10L, TimeUnit.SECONDS);
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
            query.close();
        }
    }

    @Test
    void testBoundedClientRejectsOversizedResponseFrame() throws Throwable {
        OversizedResponseServer server = new OversizedResponseServer();
        NetworkClient<GlobalIndexRequest, GlobalIndexResponse> client = boundedClient();
        try {
            server.start();
            Throwable failure =
                    failure(
                            client.sendRequest(
                                    server.getServerAddress(),
                                    new GlobalIndexRequest(
                                            SERVER_EPOCH,
                                            1L,
                                            new BinaryRow[] {keyForShard(SERVER_ID)})));
            assertThat(containsCause(failure, TooLongFrameException.class)).isTrue();
        } finally {
            client.shutdown().get(10L, TimeUnit.SECONDS);
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
        }
    }

    private FileStoreTable createAppendTable() throws Exception {
        return createAppendTable(DataTypes.STRING());
    }

    private FileStoreTable createAppendTable(org.apache.paimon.types.DataType valueType)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING().notNull())
                        .column("descriptor", valueType)
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private static BinaryRow keyForShard(int shard) {
        for (int i = 0; i < 10_000; i++) {
            BinaryRow key = row("key-" + i, DataTypes.STRING().notNull());
            if (GlobalIndexQueryServiceUtils.route(key, NUM_SERVERS) == shard) {
                return key;
            }
        }
        throw new AssertionError("Could not find a key for shard " + shard);
    }

    private static BinaryRow value(String value) {
        return row(value, DataTypes.STRING());
    }

    private static BinaryRow row(Object value, org.apache.paimon.types.DataType type) {
        InternalRowSerializer serializer = InternalSerializers.create(RowType.of(type));
        Object internalValue =
                value instanceof String ? BinaryString.fromString((String) value) : value;
        return serializer.toBinaryRow(GenericRow.of(internalValue)).copy();
    }

    private static MessageSerializer<GlobalIndexRequest, GlobalIndexResponse> serializer() {
        return new MessageSerializer<>(
                new GlobalIndexRequest.Deserializer(), new GlobalIndexResponse.Deserializer());
    }

    private static NetworkClient<GlobalIndexRequest, GlobalIndexResponse> boundedClient() {
        return new NetworkClient<>(
                "Bounded Global Index Test Client",
                1,
                serializer(),
                new DisabledServiceRequestStats(),
                GlobalIndexResponse.MAX_NETWORK_FRAME_BYTES,
                LegacyFailureFramePolicy.REJECT_SERIALIZED_THROWABLE);
    }

    private static Throwable sendMalformedRequest(
            java.net.InetSocketAddress serverAddress, GlobalIndexRequest request) throws Exception {
        NetworkClient<GlobalIndexRequest, GlobalIndexResponse> client = boundedClient();
        try {
            return failure(client.sendRequest(serverAddress, request));
        } finally {
            client.shutdown().get(10L, TimeUnit.SECONDS);
        }
    }

    private static void assertFailureResponse(
            CompletableFuture<GlobalIndexResponse> future,
            GlobalIndexQueryErrorCode errorCode,
            boolean retryable)
            throws Exception {
        assertFailureResponse(future.get(10L, TimeUnit.SECONDS), errorCode, retryable);
    }

    private static void assertFailureResponse(
            GlobalIndexResponse response, GlobalIndexQueryErrorCode errorCode, boolean retryable) {
        assertThat(response.isSuccess()).isFalse();
        assertThat(response.errorCode()).isEqualTo(errorCode);
        assertThat(response.retryable()).isEqualTo(retryable);
        assertThat(response.values()).isEmpty();
    }

    private static void assertMalformedFrameClosesConnection(
            Throwable failure, String privateServerDetail) {
        assertThat(containsCause(failure, ClosedChannelException.class)).isTrue();
        assertThat(allMessages(failure)).doesNotContain(privateServerDetail);
    }

    private static String allMessages(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            messages.append(current.getMessage()).append('\n');
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        return messages.toString();
    }

    private static boolean containsCause(
            Throwable throwable, Class<? extends Throwable> expectedType) {
        Throwable current = throwable;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            Throwable next = current.getCause();
            if (next == current) {
                return false;
            }
            current = next;
        }
        return false;
    }

    private static void awaitQueuedRequest(GlobalIndexQueryServer server) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (server.numQueuedRequests() == 1) {
                return;
            }
            Thread.sleep(10L);
        }
        assertThat(server.numQueuedRequests()).isOne();
    }

    private static void assertRequestFrameLimit(GlobalIndexQueryServer server) {
        int maxFrameLength = server.maxRequestFrameLength();
        assertThat(maxFrameLength).isEqualTo(GlobalIndexRequest.MAX_NETWORK_FRAME_BYTES);
        EmbeddedChannel channel =
                new EmbeddedChannel(new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4));
        try {
            assertThatThrownBy(
                            () ->
                                    channel.writeInbound(
                                            Unpooled.buffer(Integer.BYTES)
                                                    .writeInt(maxFrameLength - Integer.BYTES + 1)))
                    .isInstanceOf(TooLongFrameException.class);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static class BlockingQuery extends DataEvolutionGlobalIndexTableQuery {
        private final AtomicBoolean blockFirst = new AtomicBoolean(true);
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private BlockingQuery(FileStoreTable table, File stateRoot) {
            super(table, "url", Collections.singletonList("descriptor"), stateRoot);
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key)
                throws IOException {
            if (blockFirst.compareAndSet(true, false)) {
                blocked.countDown();
                try {
                    if (!release.await(10L, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting to release blocked query.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            return super.lookup(partition, bucket, key);
        }

        private boolean awaitBlocked() throws InterruptedException {
            return blocked.await(10L, TimeUnit.SECONDS);
        }

        private void release() {
            release.countDown();
        }
    }

    private static class ConstantQuery extends DataEvolutionGlobalIndexTableQuery {
        private final BinaryRow value;
        private final AtomicInteger lookupCount = new AtomicInteger();

        private ConstantQuery(FileStoreTable table, File stateRoot, BinaryRow value) {
            super(table, "url", Collections.singletonList("descriptor"), stateRoot);
            this.value = value;
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) {
            lookupCount.incrementAndGet();
            return value;
        }

        @Override
        public long latestGeneration() {
            return 5L;
        }

        @Override
        public long servedGeneration() {
            return 5L;
        }

        @Override
        public long servedSnapshotId() {
            return 100L;
        }

        private int lookupCount() {
            return lookupCount.get();
        }
    }

    private static class FailingQuery extends DataEvolutionGlobalIndexTableQuery {

        private FailingQuery(FileStoreTable table, File stateRoot) {
            super(table, "url", Collections.singletonList("descriptor"), stateRoot);
        }

        @Override
        public InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) {
            throw new AssertionError("secret lookup failure");
        }

        @Override
        public long latestGeneration() {
            return 5L;
        }

        @Override
        public long servedGeneration() {
            return 5L;
        }

        @Override
        public long servedSnapshotId() {
            return 100L;
        }
    }

    private static class OversizedResponseServer
            extends NetworkServer<GlobalIndexRequest, GlobalIndexResponse> {

        private OversizedResponseServer() {
            super(
                    "Oversized Global Index Response Server",
                    "127.0.0.1",
                    Collections.singletonList(0).iterator(),
                    1,
                    1,
                    GlobalIndexRequest.MAX_NETWORK_FRAME_BYTES);
        }

        @Override
        public AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse> initializeHandler() {
            return new AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse>(
                    this, serializer(), new DisabledServiceRequestStats()) {
                @Override
                public CompletableFuture<GlobalIndexResponse> handleRequest(
                        long requestId, GlobalIndexRequest request) {
                    return CompletableFuture.completedFuture(
                            new GlobalIndexResponse(
                                    request.serverEpoch(),
                                    request.servedGeneration(),
                                    1L,
                                    new BinaryRow[] {value("ignored")}) {
                                @Override
                                public byte[] serialize() {
                                    return new byte
                                            [GlobalIndexResponse.MAX_SERIALIZED_PAYLOAD_BYTES + 1];
                                }
                            });
                }

                @Override
                public CompletableFuture<Void> shutdown() {
                    return CompletableFuture.completedFuture(null);
                }
            };
        }
    }

    private static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected global-index request to fail.");
        } catch (CompletionException e) {
            return e.getCause();
        }
    }
}
