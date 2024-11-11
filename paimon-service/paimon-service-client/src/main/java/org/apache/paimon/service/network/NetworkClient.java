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

import org.apache.paimon.service.network.messages.MessageBody;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.utils.FutureUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.paimon.shade.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.paimon.shade.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelInitializer;
import org.apache.paimon.shade.netty4.io.netty.channel.ChannelOption;
import org.apache.paimon.shade.netty4.io.netty.channel.EventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.paimon.shade.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.paimon.shade.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.paimon.shade.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The base class for every client. It is using pure netty to send and receive messages of type
 * {@link MessageBody}.
 *
 * @param <REQ> the type of request the client will send.
 * @param <RESP> the type of response the client expects to receive.
 */
public class NetworkClient<REQ extends MessageBody, RESP extends MessageBody> {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkClient.class);

    /** The name of the client. Used for logging and stack traces. */
    private final String clientName;

    /** Netty's Bootstrap. */
    private final Bootstrap bootstrap;

    /** The serializer to be used for (de-)serializing messages. */
    private final MessageSerializer<REQ, RESP> messageSerializer;

    /** Statistics tracker. */
    private final ServiceRequestStats stats;

    private final Map<InetSocketAddress, ServerConnection<REQ, RESP>> connections =
            new ConcurrentHashMap<>();

    /** Atomic shut down future. */
    private final AtomicReference<CompletableFuture<Void>> clientShutdownFuture =
            new AtomicReference<>(null);

    /**
     * Creates a client with the specified number of event loop threads.
     *
     * @param clientName the name of the client.
     * @param numEventLoopThreads number of event loop threads (minimum 1).
     * @param serializer the serializer used to (de-)serialize messages.
     * @param stats the statistics collector.
     */
    public NetworkClient(
            final String clientName,
            final int numEventLoopThreads,
            final MessageSerializer<REQ, RESP> serializer,
            final ServiceRequestStats stats) {

        Preconditions.checkArgument(
                numEventLoopThreads >= 1, "Non-positive number of event loop threads.");

        this.clientName = Preconditions.checkNotNull(clientName);
        this.messageSerializer = Preconditions.checkNotNull(serializer);
        this.stats = Preconditions.checkNotNull(stats);

        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("Paimon " + clientName + " Event Loop Thread %d")
                        .build();

        final EventLoopGroup nioGroup = new NioEventLoopGroup(numEventLoopThreads, threadFactory);
        final ByteBufAllocator bufferPool = new NettyBufferPool(numEventLoopThreads);

        this.bootstrap =
                new Bootstrap()
                        .group(nioGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.ALLOCATOR, bufferPool)
                        .handler(
                                new ChannelInitializer<SocketChannel>() {
                                    @Override
                                    protected void initChannel(SocketChannel channel) {
                                        channel.pipeline()
                                                .addLast(
                                                        new LengthFieldBasedFrameDecoder(
                                                                Integer.MAX_VALUE, 0, 4, 0, 4))
                                                .addLast(new ChunkedWriteHandler());
                                    }
                                });
    }

    public String getClientName() {
        return clientName;
    }

    public CompletableFuture<RESP> sendRequest(
            final InetSocketAddress serverAddress, final REQ request) {
        if (clientShutdownFuture.get() != null) {
            return FutureUtils.completedExceptionally(
                    new IllegalStateException(clientName + " is already shut down."));
        }

        ServerConnection<REQ, RESP> serverConnection = connections.get(serverAddress);
        if (serverConnection == null) {
            final ServerConnection<REQ, RESP> newConnection =
                    ServerConnection.createPendingConnection(clientName, messageSerializer, stats);
            serverConnection = newConnection;
            connections.put(serverAddress, newConnection);
            bootstrap
                    .connect(serverAddress.getAddress(), serverAddress.getPort())
                    .addListener((ChannelFutureListener) newConnection::establishConnection);

            newConnection
                    .getCloseFuture()
                    .handle(
                            (ignoredA, ignoredB) ->
                                    connections.remove(serverAddress, newConnection));
        }
        return serverConnection.sendRequest(request);
    }

    /**
     * Shuts down the client and closes all connections.
     *
     * <p>After a call to this method, all returned futures will be failed.
     *
     * @return A {@link CompletableFuture} that will be completed when the shutdown process is done.
     */
    public CompletableFuture<Void> shutdown() {
        final CompletableFuture<Void> newShutdownFuture = new CompletableFuture<>();
        if (clientShutdownFuture.compareAndSet(null, newShutdownFuture)) {

            final List<CompletableFuture<Void>> connectionFutures = new ArrayList<>();

            for (Map.Entry<InetSocketAddress, ServerConnection<REQ, RESP>> conn :
                    connections.entrySet()) {
                if (connections.remove(conn.getKey(), conn.getValue())) {
                    connectionFutures.add(conn.getValue().close());
                }
            }

            CompletableFuture.allOf(connectionFutures.toArray(new CompletableFuture<?>[0]))
                    .whenComplete(
                            (result, throwable) -> {
                                if (throwable != null) {
                                    LOG.warn(
                                            "Problem while shutting down the connections at the {}: {}",
                                            clientName,
                                            throwable);
                                }

                                if (bootstrap != null) {
                                    EventLoopGroup group = bootstrap.config().group();
                                    if (group != null && !group.isShutdown()) {
                                        group.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
                                                .addListener(
                                                        finished -> {
                                                            if (finished.isSuccess()) {
                                                                newShutdownFuture.complete(null);
                                                            } else {
                                                                newShutdownFuture
                                                                        .completeExceptionally(
                                                                                finished.cause());
                                                            }
                                                        });
                                    } else {
                                        newShutdownFuture.complete(null);
                                    }
                                } else {
                                    newShutdownFuture.complete(null);
                                }
                            });

            return newShutdownFuture;
        }
        return clientShutdownFuture.get();
    }

    public boolean isEventGroupShutdown() {
        return bootstrap == null || bootstrap.config().group().isTerminated();
    }
}
