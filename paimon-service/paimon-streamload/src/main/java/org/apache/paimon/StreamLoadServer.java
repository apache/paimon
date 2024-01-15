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

package org.apache.paimon;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.flink.shaded.netty4.io.netty.buffer.PooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LoggingHandler;
import org.apache.paimon.config.NettyServerConfig;
import org.apache.paimon.exception.RemoteException;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.WriteTimeoutHandler;
import org.apache.paimon.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.paimon.server.LoadHttpHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** dsds. */
public class StreamLoadServer {

    private static final Logger LOG = LoggerFactory.getLogger(StreamLoadServer.class);

    private final ServerBootstrap serverBootstrap = new ServerBootstrap();

    private final ExecutorService defaultExecutor =
            Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final EventLoopGroup bossGroup;

    private final EventLoopGroup workGroup;

    private final NettyServerConfig serverConfig;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);

    public StreamLoadServer(final NettyServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        ThreadFactory bossThreadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverConfig.getServerName() + "BossThread_%s")
                        .build();
        ThreadFactory workerThreadFactory =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverConfig.getServerName() + "WorkerThread_%s")
                        .build();
        if (Epoll.isAvailable()) {
            this.bossGroup = new EpollEventLoopGroup(1, bossThreadFactory);
            this.workGroup =
                    new EpollEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        } else {
            this.bossGroup = new NioEventLoopGroup(1, bossThreadFactory);
            this.workGroup =
                    new NioEventLoopGroup(serverConfig.getWorkerThread(), workerThreadFactory);
        }
    }

    public void start() {
        if (isStarted.compareAndSet(false, true)) {
            this.serverBootstrap
                    .group(this.bossGroup, this.workGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog())
                    .childOption(ChannelOption.SO_KEEPALIVE, serverConfig.isSoKeepalive())
                    .childOption(ChannelOption.TCP_NODELAY, serverConfig.isTcpNoDelay())
                    .childOption(ChannelOption.SO_SNDBUF, serverConfig.getSendBufferSize())
                    .childOption(ChannelOption.SO_RCVBUF, serverConfig.getReceiveBufferSize())
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(
                            new ChannelInitializer<Channel>() {

                                @Override
                                protected void initChannel(Channel ch) {
                                    initNettyChannel(ch);
                                }
                            });

            ChannelFuture future;
            try {
                future = serverBootstrap.bind(serverConfig.getListenPort()).sync();
            } catch (Exception e) {
                LOG.error("{} bind fail {}, exit", serverConfig.getServerName(), e.getMessage(), e);
                throw new RemoteException(
                        String.format(
                                "%s bind %s fail",
                                serverConfig.getServerName(), serverConfig.getListenPort()));
            }

            if (future.isSuccess()) {
                LOG.info(
                        "{} bind success at port: {}",
                        serverConfig.getServerName(),
                        serverConfig.getListenPort());
                return;
            }

            if (future.cause() != null) {
                throw new RemoteException(
                        String.format(
                                "%s bind %s fail",
                                serverConfig.getServerName(), serverConfig.getListenPort()),
                        future.cause());
            } else {
                throw new RemoteException(
                        String.format(
                                "%s bind %s fail",
                                serverConfig.getServerName(), serverConfig.getListenPort()));
            }
        }
    }

    /**
     * init netty channel
     *
     * @param ch socket channel
     */
    private void initNettyChannel(Channel ch) {

        ch.pipeline()
                .addLast("idleStateHandler", (ChannelHandler) new IdleStateHandler(0, 0, Constants.NETTY_SERVER_HEART_BEAT_TIME, TimeUnit.MILLISECONDS))
                .addLast("encoder-decoder", new HttpServerCodec())
                .addLast(new LoggingHandler(LogLevel.INFO))
                .addLast("chunkedWriteHandler", new ChunkedWriteHandler())
                .addLast("yourBusinessLogicHandler", new LoadHttpHandler());

    }

    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                if (bossGroup != null) {
                    this.bossGroup.shutdownGracefully();
                }
                if (workGroup != null) {
                    this.workGroup.shutdownGracefully();
                }
                defaultExecutor.shutdown();
            } catch (Exception ex) {
                LOG.error("netty org.apache.paimon.server close exception", ex);
            }
            LOG.info("netty org.apache.paimon.server closed");
        }
    }

    //    public static void main(String[] args) throws Exception {
    //        int port = 8888;
    //        Options options = new Options();
    //        options.set("warehouse",
    // "file:///Users/zhuoyuchen/Documents/GitHub/incubator-paimon/data");
    //        CatalogContext context = CatalogContext.create(options);
    //        Catalog catalog = CatalogFactory.createCatalog(context);
    //        new org.apache.paimon.StreamLoadServer(port, catalog).run();
    //    }
}
