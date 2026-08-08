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

package org.apache.paimon.service.client;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.query.GlobalIndexQueryEndpoint;
import org.apache.paimon.query.GlobalIndexQueryLocation;
import org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode;
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.NetworkServer;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.messages.MessageType;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.paimon.shade.netty4.io.netty.util.ReferenceCountUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.DUPLICATE_KEY;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INTERNAL_ERROR;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.NOT_READY;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.OVERLOADED;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TIMEOUT;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.STALE_GENERATION;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNKNOWN_KEY_SHARD;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNSUPPORTED_PROTOCOL;
import static org.apache.paimon.service.messages.KvRequestTest.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests routing, fencing, and bounded retries of {@link GlobalIndexQueryClient}. */
class GlobalIndexQueryClientTest {

    private static final InetSocketAddress ADDRESS_0 =
            InetSocketAddress.createUnresolved("server-0", 10000);
    private static final InetSocketAddress ADDRESS_1 =
            InetSocketAddress.createUnresolved("server-1", 10001);

    @Test
    void testGroupsByEndpointAndRestoresOriginalOrder() throws Exception {
        List<SentRequest> sentRequests = new ArrayList<>();
        GlobalIndexQueryLocation location =
                (key, forceUpdate) ->
                        key.getInt(0) % 2 == 0
                                ? endpoint(0, ADDRESS_0, "epoch-0", 7L, 55L)
                                : endpoint(1, ADDRESS_1, "epoch-1", 7L, 55L);
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        location,
                        (address, request) -> {
                            sentRequests.add(new SentRequest(address, request));
                            BinaryRow[] values = new BinaryRow[request.keys().length];
                            for (int i = 0; i < values.length; i++) {
                                values[i] = row(request.keys()[i].getInt(0) + 100);
                            }
                            return CompletableFuture.completedFuture(
                                    new GlobalIndexResponse(
                                            request.serverEpoch(),
                                            request.servedGeneration(),
                                            55L,
                                            values));
                        });

        BinaryRow callerKey = row(3);
        callerKey.setRowKind(RowKind.DELETE);
        GlobalIndexQueryClient.LookupResult lookupResult =
                client.getValuesWithMetadata(new BinaryRow[] {callerKey, row(2), row(1), row(4)})
                        .get(10, TimeUnit.SECONDS);
        BinaryRow[] values = lookupResult.values();

        assertThat(intValues(values)).containsExactly(103, 102, 101, 104);
        assertThat(lookupResult.servedGeneration()).isEqualTo(7L);
        assertThat(lookupResult.servedSnapshotId()).isEqualTo(55L);
        assertThat(lookupResult.snapshotUuid()).isEqualTo("snapshot-55");
        assertThat(sentRequests).hasSize(2);
        assertThat(sentRequests)
                .extracting(request -> request.address)
                .containsExactly(ADDRESS_1, ADDRESS_0);
        assertThat(sentRequests.get(0).request.serverEpoch()).isEqualTo("epoch-1");
        assertThat(sentRequests.get(0).request.servedGeneration()).isEqualTo(7L);
        assertThat(intValues(sentRequests.get(0).request.keys())).containsExactly(3, 1);
        assertThat(intValues(sentRequests.get(1).request.keys())).containsExactly(2, 4);
        assertThat(sentRequests.get(0).request.keys()[0]).isNotSameAs(callerKey);
        assertThat(sentRequests.get(0).request.keys()[0].getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(callerKey.getRowKind()).isEqualTo(RowKind.DELETE);
    }

    @Test
    void testSynchronousNestedConnectFailureRefreshesOnce() throws Exception {
        List<Boolean> forceUpdates = new ArrayList<>();
        GlobalIndexQueryLocation location =
                (key, forceUpdate) -> {
                    forceUpdates.add(forceUpdate);
                    if (!forceUpdate) {
                        throw new IOException(new ConnectException("not listening"));
                    }
                    return endpoint(0, ADDRESS_0, "new-epoch", 2L, 20L);
                };
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        location,
                        (address, request) ->
                                CompletableFuture.completedFuture(
                                        new GlobalIndexResponse(
                                                "new-epoch", 2L, 20L, new BinaryRow[] {row(9)})));

        assertThat(intValues(client.getValues(new BinaryRow[] {row(1)}).get())).containsExactly(9);
        assertThat(forceUpdates).containsExactly(false, true);
    }

    @Test
    void testAsynchronousNestedConnectFailureRefreshesOnce() throws Exception {
        AtomicBoolean refreshed = new AtomicBoolean();
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            if (forceUpdate) {
                                refreshed.set(true);
                            }
                            return refreshed.get()
                                    ? endpoint(0, ADDRESS_0, "epoch-2", 2L, 20L)
                                    : endpoint(0, ADDRESS_0, "epoch-1", 1L, 10L);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            if (request.servedGeneration() == 1L) {
                                return failedFuture(
                                        new RuntimeException(
                                                new IOException(
                                                        new ConnectException("not listening"))));
                            }
                            return CompletableFuture.completedFuture(
                                    new GlobalIndexResponse(
                                            "epoch-2", 2L, 20L, new BinaryRow[] {row(9)}));
                        });

        assertThat(intValues(client.getValues(new BinaryRow[] {row(1)}).get())).containsExactly(9);
        assertThat(sends).hasValue(2);
        assertThat(forceUpdates).containsExactly(false, true);
    }

    @Test
    void testClosedChannelFailureRefreshesOnce() throws Exception {
        AtomicBoolean refreshed = new AtomicBoolean();
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            if (forceUpdate) {
                                refreshed.set(true);
                            }
                            return refreshed.get()
                                    ? endpoint(0, ADDRESS_0, "epoch-2", 2L, 20L)
                                    : endpoint(0, ADDRESS_0, "epoch-1", 1L, 10L);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            if (request.servedGeneration() == 1L) {
                                return failedFuture(new IOException(new ClosedChannelException()));
                            }
                            return CompletableFuture.completedFuture(
                                    new GlobalIndexResponse(
                                            "epoch-2", 2L, 20L, new BinaryRow[] {row(9)}));
                        });

        assertThat(intValues(client.getValues(new BinaryRow[] {row(1)}).get())).containsExactly(9);
        assertThat(sends).hasValue(2);
        assertThat(forceUpdates).containsExactly(false, true);
    }

    @Test
    void testUnknownShardRetriesOnlyOnce() {
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            return endpoint(0, ADDRESS_0, "epoch", 1L, 10L);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    GlobalIndexResponse.failure(
                                            request.serverEpoch(),
                                            request.servedGeneration(),
                                            10L,
                                            UNKNOWN_KEY_SHARD,
                                            "wrong shard"));
                        });

        Throwable failure = failure(client.getValues(new BinaryRow[] {row(1)}));

        assertThat(failure)
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> {
                            assertThat(e.errorCode()).isEqualTo(UNKNOWN_KEY_SHARD);
                            assertThat(e.retryable()).isTrue();
                        });
        assertThat(sends).hasValue(2);
        assertThat(forceUpdates).containsExactly(false, true);
    }

    @Test
    void testMismatchedResponseFenceRefreshesWholeBatch() throws Exception {
        AtomicBoolean refreshed = new AtomicBoolean();
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            if (forceUpdate) {
                                refreshed.set(true);
                            }
                            return refreshed.get()
                                    ? endpoint(0, ADDRESS_0, "epoch-2", 2L, 20L)
                                    : endpoint(0, ADDRESS_0, "epoch-1", 1L, 10L);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            long snapshot = request.servedGeneration() == 1L ? 99L : 20L;
                            return CompletableFuture.completedFuture(
                                    new GlobalIndexResponse(
                                            request.serverEpoch(),
                                            request.servedGeneration(),
                                            snapshot,
                                            new BinaryRow[] {row(11), row(12)}));
                        });

        BinaryRow[] values = client.getValues(new BinaryRow[] {row(1), row(2)}).get();

        assertThat(intValues(values)).containsExactly(11, 12);
        assertThat(sends).hasValue(2);
        assertThat(forceUpdates).containsExactly(false, false, true, false);
    }

    @Test
    void testMismatchedFailureResponseFenceTakesPrecedenceOverBusinessError() {
        List<GlobalIndexResponse> mismatchedResponses =
                Arrays.asList(
                        GlobalIndexResponse.failure(
                                "other-epoch", 1L, 10L, DUPLICATE_KEY, "duplicate key"),
                        GlobalIndexResponse.failure(
                                "epoch", 2L, 10L, DUPLICATE_KEY, "duplicate key"),
                        GlobalIndexResponse.failure(
                                "epoch", 1L, 11L, DUPLICATE_KEY, "duplicate key"));

        for (GlobalIndexResponse response : mismatchedResponses) {
            List<Boolean> forceUpdates = new ArrayList<>();
            AtomicInteger sends = new AtomicInteger();
            GlobalIndexQueryClient client =
                    new GlobalIndexQueryClient(
                            (key, forceUpdate) -> {
                                forceUpdates.add(forceUpdate);
                                return endpoint(0, ADDRESS_0, "epoch", 1L, 10L);
                            },
                            (address, request) -> {
                                sends.incrementAndGet();
                                return CompletableFuture.completedFuture(response);
                            });

            assertThat(failure(client.getValues(new BinaryRow[] {row(1)})))
                    .isInstanceOfSatisfying(
                            GlobalIndexQueryException.class,
                            e -> {
                                assertThat(e.errorCode()).isEqualTo(STALE_GENERATION);
                                assertThat(e.retryable()).isTrue();
                                assertThat(e).hasMessageContaining("does not match expected");
                            });
            assertThat(sends).hasValue(2);
            assertThat(forceUpdates).containsExactly(false, true);
        }
    }

    @Test
    void testMatchingFailureResponseFencePreservesBusinessError() {
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> endpoint(0, ADDRESS_0, "epoch", 1L, 10L),
                        (address, request) -> {
                            sends.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    GlobalIndexResponse.failure(
                                            "epoch",
                                            1L,
                                            10L,
                                            DUPLICATE_KEY,
                                            "duplicate from server"));
                        });

        assertThat(failure(client.getValues(new BinaryRow[] {row(1)})))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> {
                            assertThat(e.errorCode()).isEqualTo(DUPLICATE_KEY);
                            assertThat(e.retryable()).isFalse();
                            assertThat(e).hasMessage("duplicate from server");
                        });
        assertThat(sends).hasValue(1);
    }

    @Test
    void testMixedDiscoveryFenceIsNeverSentAndRefreshesWholeBatch() throws Exception {
        AtomicBoolean refreshed = new AtomicBoolean();
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            if (forceUpdate) {
                                refreshed.set(true);
                            }
                            long snapshot = refreshed.get() ? 20L : 10L + key.getInt(0);
                            return endpoint(
                                    0,
                                    ADDRESS_0,
                                    refreshed.get() ? "epoch-2" : "epoch-1",
                                    refreshed.get() ? 2L : 1L,
                                    snapshot);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    new GlobalIndexResponse(
                                            request.serverEpoch(),
                                            request.servedGeneration(),
                                            20L,
                                            new BinaryRow[] {row(21), row(22)}));
                        });

        BinaryRow[] values = client.getValues(new BinaryRow[] {row(1), row(2)}).get();

        assertThat(intValues(values)).containsExactly(21, 22);
        assertThat(sends).hasValue(1);
        assertThat(forceUpdates).containsExactly(false, false, true, false);
    }

    @Test
    void testSemanticFailuresDoNotRetry() {
        for (GlobalIndexQueryErrorCode errorCode :
                Arrays.asList(
                        NOT_READY,
                        DUPLICATE_KEY,
                        INTERNAL_ERROR,
                        UNSUPPORTED_PROTOCOL,
                        REQUEST_TOO_LARGE,
                        INVALID_REQUEST)) {
            AtomicInteger locations = new AtomicInteger();
            AtomicInteger sends = new AtomicInteger();
            GlobalIndexQueryClient client =
                    new GlobalIndexQueryClient(
                            (key, forceUpdate) -> {
                                locations.incrementAndGet();
                                return endpoint(0, ADDRESS_0, "epoch", 1L, 10L);
                            },
                            (address, request) -> {
                                sends.incrementAndGet();
                                return CompletableFuture.completedFuture(
                                        GlobalIndexResponse.failure(
                                                request.serverEpoch(),
                                                request.servedGeneration(),
                                                10L,
                                                errorCode,
                                                "failure"));
                            });

            assertThat(failure(client.getValues(new BinaryRow[] {row(1)})))
                    .isInstanceOfSatisfying(
                            GlobalIndexQueryException.class,
                            e -> assertThat(e.errorCode()).isEqualTo(errorCode));
            assertThat(locations).hasValue(1);
            assertThat(sends).hasValue(1);
        }
    }

    @Test
    void testOverloadedResponseRefreshesOnlyOnce() {
        List<Boolean> forceUpdates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            return endpoint(0, ADDRESS_0, "epoch", 1L, 10L);
                        },
                        (address, request) -> {
                            sends.incrementAndGet();
                            return CompletableFuture.completedFuture(
                                    GlobalIndexResponse.failure(
                                            request.serverEpoch(),
                                            request.servedGeneration(),
                                            10L,
                                            OVERLOADED,
                                            "busy"));
                        });

        assertThat(failure(client.getValues(new BinaryRow[] {row(1)})))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> {
                            assertThat(e.errorCode()).isEqualTo(OVERLOADED);
                            assertThat(e.retryable()).isTrue();
                        });
        assertThat(sends).hasValue(2);
        assertThat(forceUpdates).containsExactly(false, true);
    }

    @Test
    void testBlackHoleRequestTimesOutCancelsAndRefreshesOnlyOnce() throws Exception {
        List<Boolean> forceUpdates = new ArrayList<>();
        List<CompletableFuture<GlobalIndexResponse>> blackHoles = new ArrayList<>();
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> {
                            forceUpdates.add(forceUpdate);
                            return endpoint(0, ADDRESS_0, "epoch", 1L, 10L);
                        },
                        (address, request) -> {
                            CompletableFuture<GlobalIndexResponse> blackHole =
                                    new CompletableFuture<>();
                            blackHoles.add(blackHole);
                            return blackHole;
                        },
                        Duration.ofMillis(20));

        assertThat(failure(client.getValues(new BinaryRow[] {row(1)})))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> {
                            assertThat(e.errorCode()).isEqualTo(REQUEST_TIMEOUT);
                            assertThat(e.retryable()).isTrue();
                        });
        assertThat(forceUpdates).containsExactly(false, true);
        assertThat(blackHoles).hasSize(2);
        awaitAllCancelled(blackHoles);
    }

    @Test
    void testRepeatedNetworkTimeoutsReleasePendingRequests() throws Throwable {
        BlackHoleServer server = new BlackHoleServer();
        GlobalIndexQueryClient client = null;
        try {
            server.start();
            InetSocketAddress address = server.getServerAddress();
            AtomicInteger queryLocationCalls = new AtomicInteger();
            client =
                    new GlobalIndexQueryClient(
                            (key, forceUpdate) -> {
                                queryLocationCalls.incrementAndGet();
                                return endpoint(0, address, "epoch", 1L, 10L);
                            },
                            1,
                            Duration.ofMillis(100));

            boolean connectionReady = false;
            for (int attempt = 0; attempt < 100; attempt++) {
                try {
                    assertThat(client.getValues(new BinaryRow[] {row(-1)}).join())
                            .containsExactly((BinaryRow) null);
                    connectionReady = true;
                } catch (CompletionException e) {
                    assertThat(e.getCause())
                            .isInstanceOfSatisfying(
                                    GlobalIndexQueryException.class,
                                    failure ->
                                            assertThat(failure.errorCode())
                                                    .isEqualTo(REQUEST_TIMEOUT));
                }
                awaitNoPendingRequests(client);
                if (connectionReady) {
                    break;
                }
            }
            assertThat(connectionReady).isTrue();

            server.enableBlackHole();
            queryLocationCalls.set(0);

            for (int i = 0; i < 5; i++) {
                assertThat(failure(client.getValues(new BinaryRow[] {row(i)})))
                        .isInstanceOfSatisfying(
                                GlobalIndexQueryException.class,
                                e -> assertThat(e.errorCode()).isEqualTo(REQUEST_TIMEOUT));
                awaitNoPendingRequests(client);
                if (i == 0) {
                    assertThat(server.awaitFirstBlackHoleRequest()).isTrue();
                }
            }

            assertThat(queryLocationCalls).hasValue(10);
        } finally {
            if (client != null) {
                client.shutdownFuture().get(10L, TimeUnit.SECONDS);
            }
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
        }
    }

    @Test
    void testRejectsMaliciousLegacyFailureFrameWithoutDeserializingThrowable() throws Throwable {
        MaliciousFailureServer server = new MaliciousFailureServer();
        GlobalIndexQueryClient client = null;
        DeserializationProbeException.reset();
        try {
            server.start();
            InetSocketAddress address = server.getServerAddress();
            client =
                    new GlobalIndexQueryClient(
                            (key, forceUpdate) -> endpoint(0, address, "epoch", 1L, 10L),
                            1,
                            Duration.ofSeconds(5));

            Throwable failure = failure(client.getValues(new BinaryRow[] {row(1)}));

            assertThat(failure)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Rejected prohibited legacy serialized Throwable");
            assertThat(DeserializationProbeException.deserializationCount()).isZero();
            awaitNoPendingRequests(client);
        } finally {
            if (client != null) {
                client.shutdownFuture().get(10L, TimeUnit.SECONDS);
            }
            server.shutdownServer().get(10L, TimeUnit.SECONDS);
        }
    }

    @Test
    void testLogicalBatchLimitIsAppliedBeforeSharding() {
        GlobalIndexQueryClient client =
                new GlobalIndexQueryClient(
                        (key, forceUpdate) -> endpoint(0, ADDRESS_0, "epoch", 1L, 10L),
                        (address, request) -> {
                            throw new AssertionError("Oversized logical batch must not be sent.");
                        });
        BinaryRow[] keys = new BinaryRow[GlobalIndexRequest.MAX_KEYS + 1];
        Arrays.fill(keys, row(1));

        assertThatThrownBy(() -> client.getValues(keys))
                .isInstanceOfSatisfying(
                        GlobalIndexQueryException.class,
                        e -> assertThat(e.errorCode()).isEqualTo(REQUEST_TOO_LARGE));
    }

    private static GlobalIndexQueryEndpoint endpoint(
            int shard, InetSocketAddress address, String epoch, long generation, long snapshot) {
        return new GlobalIndexQueryEndpoint(
                shard, address, epoch, generation, snapshot, "snapshot-" + snapshot);
    }

    private static CompletableFuture<GlobalIndexResponse> failedFuture(Throwable throwable) {
        CompletableFuture<GlobalIndexResponse> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }

    private static Throwable failure(CompletableFuture<?> future) {
        try {
            future.join();
            throw new AssertionError("Expected global-index query to fail.");
        } catch (CompletionException e) {
            return e.getCause();
        }
    }

    private static List<Integer> intValues(BinaryRow[] rows) {
        List<Integer> values = new ArrayList<>(rows.length);
        for (BinaryRow row : rows) {
            values.add(row == null ? null : row.getInt(0));
        }
        return values;
    }

    private static void awaitAllCancelled(List<CompletableFuture<GlobalIndexResponse>> futures)
            throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (futures.stream().allMatch(CompletableFuture::isCancelled)) {
                return;
            }
            Thread.sleep(10L);
        }
        assertThat(futures).allMatch(CompletableFuture::isCancelled);
    }

    private static void awaitNoPendingRequests(GlobalIndexQueryClient client) throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            if (client.numPendingNetworkRequests() == 0) {
                return;
            }
            Thread.sleep(10L);
        }
        assertThat(client.numPendingNetworkRequests()).isZero();
    }

    private static class BlackHoleServer
            extends NetworkServer<GlobalIndexRequest, GlobalIndexResponse> {

        private final AtomicBoolean blackHoleEnabled = new AtomicBoolean();
        private final CountDownLatch firstBlackHoleRequestReceived = new CountDownLatch(1);

        private BlackHoleServer() {
            super(
                    "Global Index Black Hole Test Server",
                    "127.0.0.1",
                    Collections.singletonList(0).iterator(),
                    1,
                    1,
                    GlobalIndexRequest.MAX_NETWORK_FRAME_BYTES);
        }

        @Override
        public AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse> initializeHandler() {
            return new AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse>(
                    this,
                    new MessageSerializer<>(
                            new GlobalIndexRequest.Deserializer(),
                            new GlobalIndexResponse.Deserializer()),
                    new DisabledServiceRequestStats()) {
                @Override
                public CompletableFuture<GlobalIndexResponse> handleRequest(
                        long requestId, GlobalIndexRequest request) {
                    if (!blackHoleEnabled.get()) {
                        return CompletableFuture.completedFuture(
                                new GlobalIndexResponse(
                                        request.serverEpoch(),
                                        request.servedGeneration(),
                                        10L,
                                        new BinaryRow[request.keys().length]));
                    }
                    firstBlackHoleRequestReceived.countDown();
                    return new CompletableFuture<>();
                }

                @Override
                public CompletableFuture<Void> shutdown() {
                    return CompletableFuture.completedFuture(null);
                }
            };
        }

        private void enableBlackHole() {
            blackHoleEnabled.set(true);
        }

        private boolean awaitFirstBlackHoleRequest() throws InterruptedException {
            return firstBlackHoleRequestReceived.await(10L, TimeUnit.SECONDS);
        }
    }

    private static class MaliciousFailureServer
            extends NetworkServer<GlobalIndexRequest, GlobalIndexResponse> {

        private MaliciousFailureServer() {
            super(
                    "Malicious Legacy Failure Test Server",
                    "127.0.0.1",
                    Collections.singletonList(0).iterator(),
                    1,
                    1,
                    GlobalIndexRequest.MAX_NETWORK_FRAME_BYTES);
        }

        @Override
        public AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse> initializeHandler() {
            return new AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse>(
                    this,
                    new MessageSerializer<>(
                            new GlobalIndexRequest.Deserializer(),
                            new GlobalIndexResponse.Deserializer()),
                    new DisabledServiceRequestStats()) {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    try {
                        ByteBuf frame = (ByteBuf) msg;
                        MessageType messageType = MessageSerializer.deserializeHeader(frame);
                        if (messageType != MessageType.REQUEST) {
                            throw new IOException("Expected a request frame.");
                        }
                        long requestId = MessageSerializer.getRequestId(frame);
                        ctx.writeAndFlush(
                                MessageSerializer.serializeRequestFailure(
                                        ctx.alloc(),
                                        requestId,
                                        new DeserializationProbeException()));
                    } finally {
                        ReferenceCountUtil.release(msg);
                    }
                }

                @Override
                public CompletableFuture<GlobalIndexResponse> handleRequest(
                        long requestId, GlobalIndexRequest request) {
                    throw new AssertionError("Raw malicious handler must bypass request handling.");
                }

                @Override
                public CompletableFuture<Void> shutdown() {
                    return CompletableFuture.completedFuture(null);
                }
            };
        }
    }

    private static class DeserializationProbeException extends RuntimeException {

        private static final long serialVersionUID = 1L;
        private static final AtomicInteger DESERIALIZATION_COUNT = new AtomicInteger();

        private void readObject(ObjectInputStream input)
                throws IOException, ClassNotFoundException {
            input.defaultReadObject();
            DESERIALIZATION_COUNT.incrementAndGet();
        }

        private static void reset() {
            DESERIALIZATION_COUNT.set(0);
        }

        private static int deserializationCount() {
            return DESERIALIZATION_COUNT.get();
        }
    }

    private static class SentRequest {
        private final InetSocketAddress address;
        private final GlobalIndexRequest request;

        private SentRequest(InetSocketAddress address, GlobalIndexRequest request) {
            this.address = address;
            this.request = request;
        }
    }
}
