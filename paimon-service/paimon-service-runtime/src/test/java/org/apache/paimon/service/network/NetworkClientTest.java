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

package org.apache.paimon.service.network;

import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvRequestTest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.messages.KvResponseTest;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.messages.MessageType;
import org.apache.paimon.service.network.stats.AtomicServiceRequestStats;
import org.apache.paimon.testutils.assertj.PaimonAssertions;
import org.apache.paimon.utils.ExceptionUtils;

import org.apache.paimon.shade.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBuf;
import org.apache.paimon.shade.netty4.io.netty.channel.Channel;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInitializer;
import org.apache.paimon.shade.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NetworkClient}. */
class NetworkClientTest {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkClientTest.class);

    // Thread pool for client bootstrap (shared between tests)
    private NioEventLoopGroup nioGroup;

    @BeforeEach
    void setUp() {
        nioGroup = new NioEventLoopGroup();
    }

    @AfterEach
    void tearDown() {
        if (nioGroup != null) {
            // note: no "quiet period" to not trigger Netty#4357
            nioGroup.shutdownGracefully();
        }
    }

    /** Tests simple queries, of which half succeed and half fail. */
    @Test
    void testSimpleRequests() throws Exception {
        AtomicServiceRequestStats stats = new AtomicServiceRequestStats();

        MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        NetworkClient<KvRequest, KvResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new NetworkClient<>("Test Client", 1, serializer, stats);

            // Random result
            final KvResponse expected = KvResponseTest.random();

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getServerAddress(serverChannel);

            long numQueries = 1024L;

            List<CompletableFuture<KvResponse>> futures = new ArrayList<>();
            for (long i = 0L; i < numQueries; i++) {
                KvRequest request = KvRequestTest.random();
                futures.add(client.sendRequest(serverAddress, request));
            }

            // Respond to messages
            Exception testException = new RuntimeException("Expected test Exception");

            for (long i = 0L; i < numQueries; i++) {
                ByteBuf buf = received.take();
                assertThat(buf).withFailMessage("Receive timed out").isNotNull();

                Channel ch = channel.get();
                assertThat(ch).withFailMessage("Channel not active").isNotNull();

                assertThat(MessageType.REQUEST).isEqualTo(MessageSerializer.deserializeHeader(buf));
                long requestId = MessageSerializer.getRequestId(buf);

                buf.release();

                ByteBuf response;
                if (i % 2L == 0L) {
                    response =
                            MessageSerializer.serializeResponse(
                                    serverChannel.alloc(), requestId, expected);

                } else {
                    response =
                            MessageSerializer.serializeRequestFailure(
                                    serverChannel.alloc(), requestId, testException);
                }
                ch.writeAndFlush(response);
            }

            for (long i = 0L; i < numQueries; i++) {

                if (i % 2L == 0L) {
                    KvResponse serializedResult = futures.get((int) i).get();
                    assertThat(serializedResult).isEqualTo(expected);
                } else {
                    CompletableFuture<KvResponse> future = futures.get((int) i);
                    PaimonAssertions.assertThatFuture(future)
                            .eventuallyFailsWith(ExecutionException.class)
                            .satisfies(PaimonAssertions.anyCauseMatches(RuntimeException.class));
                }
            }

            assertThat(numQueries).isEqualTo(stats.getNumRequests());
            long expectedRequests = numQueries / 2L;

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != expectedRequests
                    || stats.getNumFailed() != expectedRequests) {
                //noinspection BusyWait
                Thread.sleep(100L);
            }

            assertThat(expectedRequests).isEqualTo(stats.getNumSuccessful());
            assertThat(expectedRequests).isEqualTo(stats.getNumFailed());
        } finally {
            if (client != null) {
                Exception exc = null;
                try {

                    // todo here we were seeing this problem:
                    // https://github.com/netty/netty/issues/4357 if we do a get().
                    // this is why we now simply wait a bit so that everything is
                    // shut down and then we check

                    client.shutdown().get();
                } catch (Exception e) {
                    exc = e;
                    LOG.error("An exception occurred while shutting down netty.", e);
                }

                assertThat(client.isEventGroupShutdown())
                        .withFailMessage(ExceptionUtils.stringifyException(exc))
                        .isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /** Tests that a request to an unavailable host is failed with ConnectException. */
    @Test
    void testRequestUnavailableHost() {
        AtomicServiceRequestStats stats = new AtomicServiceRequestStats();

        MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        NetworkClient<KvRequest, KvResponse> client = null;

        try {
            client = new NetworkClient<>("Test Client", 1, serializer, stats);

            // Since no real servers are created based on the server address, the given fixed port
            // is enough.
            InetSocketAddress serverAddress =
                    new InetSocketAddress("flink-qs-client-test-unavailable-host", 12345);

            KvRequest request = KvRequestTest.random();
            CompletableFuture<KvResponse> future = client.sendRequest(serverAddress, request);

            assertThat(future).isNotNull();
            assertThatThrownBy(future::get).hasRootCauseInstanceOf(ConnectException.class);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /** Multiple threads concurrently fire queries. */
    @Test
    void testConcurrentQueries() throws Exception {
        AtomicServiceRequestStats stats = new AtomicServiceRequestStats();

        final MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        ExecutorService executor = null;
        NetworkClient<KvRequest, KvResponse> client = null;
        Channel serverChannel = null;

        final KvResponse expected = KvResponseTest.random();

        try {
            int numQueryTasks = 4;
            final int numQueriesPerTask = 1024;

            executor = Executors.newFixedThreadPool(numQueryTasks);

            client = new NetworkClient<>("Test Client", 1, serializer, stats);

            serverChannel = createServerChannel(new RespondingChannelHandler(serializer, expected));

            final InetSocketAddress serverAddress = getServerAddress(serverChannel);

            final NetworkClient<KvRequest, KvResponse> finalClient = client;
            Callable<List<CompletableFuture<KvResponse>>> queryTask =
                    () -> {
                        List<CompletableFuture<KvResponse>> results =
                                new ArrayList<>(numQueriesPerTask);

                        for (int i = 0; i < numQueriesPerTask; i++) {
                            KvRequest request = KvRequestTest.random();
                            results.add(finalClient.sendRequest(serverAddress, request));
                        }

                        return results;
                    };

            // Submit query tasks
            List<Future<List<CompletableFuture<KvResponse>>>> futures = new ArrayList<>();
            for (int i = 0; i < numQueryTasks; i++) {
                futures.add(executor.submit(queryTask));
            }

            // Verify results
            for (Future<List<CompletableFuture<KvResponse>>> future : futures) {
                List<CompletableFuture<KvResponse>> results = future.get();
                for (CompletableFuture<KvResponse> result : results) {
                    KvResponse actual = result.get();
                    assertThat(actual).isEqualTo(expected);
                }
            }

            int totalQueries = numQueryTasks * numQueriesPerTask;

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != totalQueries) {
                //noinspection BusyWait
                Thread.sleep(100L);
            }

            assertThat(totalQueries).isEqualTo(stats.getNumRequests());
            assertThat(totalQueries).isEqualTo(stats.getNumSuccessful());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /**
     * Tests that a server failure closes the connection and removes it from the established
     * connections.
     */
    @Test
    void testFailureClosesChannel() throws Exception {
        AtomicServiceRequestStats stats = new AtomicServiceRequestStats();

        final MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        NetworkClient<KvRequest, KvResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new NetworkClient<>("Test Client", 1, serializer, stats);

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getServerAddress(serverChannel);

            // Requests
            List<CompletableFuture<KvResponse>> futures = new ArrayList<>();
            KvRequest request = KvRequestTest.random();

            futures.add(client.sendRequest(serverAddress, request));
            futures.add(client.sendRequest(serverAddress, request));

            ByteBuf buf = received.take();
            assertThat(buf).withFailMessage("Receive timed out").isNotNull();
            buf.release();

            buf = received.take();
            assertThat(buf).withFailMessage("Receive timed out").isNotNull();
            buf.release();

            assertThat(stats.getNumConnections()).isEqualTo(1L);

            Channel ch = channel.get();
            assertThat(ch).withFailMessage("Channel not active").isNotNull();

            // Respond with failure
            ch.writeAndFlush(
                    MessageSerializer.serializeServerFailure(
                            serverChannel.alloc(),
                            new RuntimeException("Expected test server failure")));

            CompletableFuture<KvResponse> removedFuture = futures.remove(0);
            PaimonAssertions.assertThatFuture(removedFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(PaimonAssertions.anyCauseMatches(RuntimeException.class));

            removedFuture = futures.remove(0);
            PaimonAssertions.assertThatFuture(removedFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(PaimonAssertions.anyCauseMatches(RuntimeException.class));

            assertThat(stats.getNumConnections()).isZero();

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != 0L || stats.getNumFailed() != 2L) {
                //noinspection BusyWait
                Thread.sleep(100L);
            }

            assertThat(stats.getNumRequests()).isEqualTo(2L);
            assertThat(stats.getNumSuccessful()).isZero();
            assertThat(stats.getNumFailed()).isEqualTo(2L);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    /**
     * Tests that a server channel close, closes the connection and removes it from the established
     * connections.
     */
    @Test
    void testServerClosesChannel() throws Exception {
        AtomicServiceRequestStats stats = new AtomicServiceRequestStats();

        final MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());

        NetworkClient<KvRequest, KvResponse> client = null;
        Channel serverChannel = null;

        try {
            client = new NetworkClient<>("Test Client", 1, serializer, stats);

            final LinkedBlockingQueue<ByteBuf> received = new LinkedBlockingQueue<>();
            final AtomicReference<Channel> channel = new AtomicReference<>();

            serverChannel =
                    createServerChannel(new ChannelDataCollectingHandler(channel, received));

            InetSocketAddress serverAddress = getServerAddress(serverChannel);

            // Requests
            KvRequest request = KvRequestTest.random();
            CompletableFuture<KvResponse> future = client.sendRequest(serverAddress, request);

            received.take();

            assertThat(stats.getNumConnections()).isEqualTo(1);

            channel.get().close().await();

            PaimonAssertions.assertThatFuture(future)
                    .eventuallyFailsWith(ExecutionException.class)
                    .satisfies(PaimonAssertions.anyCauseMatches(ClosedChannelException.class));

            assertThat(stats.getNumConnections()).isZero();

            // Counts can take some time to propagate
            while (stats.getNumSuccessful() != 0L || stats.getNumFailed() != 1L) {
                //noinspection BusyWait
                Thread.sleep(100L);
            }

            assertThat(stats.getNumRequests()).isEqualTo(1L);
            assertThat(stats.getNumSuccessful()).isZero();
            assertThat(stats.getNumFailed()).isEqualTo(1L);
        } finally {
            if (client != null) {
                try {
                    client.shutdown().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assertThat(client.isEventGroupShutdown()).isTrue();
            }

            if (serverChannel != null) {
                serverChannel.close();
            }

            assertThat(stats.getNumConnections()).withFailMessage("Channel leak").isZero();
        }
    }

    // ------------------------------------------------------------------------

    private Channel createServerChannel(final ChannelHandler... handlers)
            throws UnknownHostException, InterruptedException {
        ServerBootstrap bootstrap =
                new ServerBootstrap()
                        // Bind address and port
                        .localAddress(InetAddress.getLocalHost(), 0)
                        // NIO server channels
                        .group(nioGroup)
                        .channel(NioServerSocketChannel.class)
                        // See initializer for pipeline details
                        .childHandler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel ch) {
                                        ch.pipeline()
                                                .addLast(
                                                        new LengthFieldBasedFrameDecoder(
                                                                Integer.MAX_VALUE, 0, 4, 0, 4))
                                                .addLast(handlers);
                                    }
                                });

        return bootstrap.bind().sync().channel();
    }

    private InetSocketAddress getServerAddress(Channel serverChannel) {
        return (InetSocketAddress) serverChannel.localAddress();
    }

    private static class ChannelDataCollectingHandler extends ChannelInboundHandlerAdapter {
        private final AtomicReference<Channel> channel;
        private final LinkedBlockingQueue<ByteBuf> received;

        private ChannelDataCollectingHandler(
                AtomicReference<Channel> channel, LinkedBlockingQueue<ByteBuf> received) {
            this.channel = channel;
            this.received = received;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            channel.set(ctx.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            received.add((ByteBuf) msg);
        }
    }

    @ChannelHandler.Sharable
    private static final class RespondingChannelHandler extends ChannelInboundHandlerAdapter {
        private final MessageSerializer<KvRequest, KvResponse> serializer;
        private final KvResponse response;

        private RespondingChannelHandler(
                MessageSerializer<KvRequest, KvResponse> serializer, KvResponse response) {
            this.serializer = serializer;
            this.response = response;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST);
            long requestId = MessageSerializer.getRequestId(buf);
            serializer.deserializeRequest(buf);

            buf.release();

            ByteBuf serResponse =
                    MessageSerializer.serializeResponse(ctx.alloc(), requestId, response);

            ctx.channel().writeAndFlush(serResponse);
        }
    }
}
