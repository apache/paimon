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

import org.apache.paimon.query.QueryServer;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.NetworkServer;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.table.query.DataEvolutionGlobalIndexTableQuery;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Network server for shard-routed global-index lookups. */
public class GlobalIndexQueryServer extends NetworkServer<GlobalIndexRequest, GlobalIndexResponse>
        implements QueryServer {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexQueryServer.class);
    public static final int DEFAULT_MAX_QUEUED_REQUESTS = 16;

    private final int serverId;
    private final int numServers;
    private final String serverEpoch;
    private final DataEvolutionGlobalIndexTableQuery lookup;
    private final ServiceRequestStats stats;

    public GlobalIndexQueryServer(
            int serverId,
            int numServers,
            String serverEpoch,
            String bindAddress,
            Iterator<Integer> bindPortIterator,
            int numEventLoopThreads,
            int numQueryThreads,
            DataEvolutionGlobalIndexTableQuery lookup,
            ServiceRequestStats stats) {
        this(
                serverId,
                numServers,
                serverEpoch,
                bindAddress,
                bindPortIterator,
                numEventLoopThreads,
                numQueryThreads,
                DEFAULT_MAX_QUEUED_REQUESTS,
                lookup,
                stats);
    }

    public GlobalIndexQueryServer(
            int serverId,
            int numServers,
            String serverEpoch,
            String bindAddress,
            Iterator<Integer> bindPortIterator,
            int numEventLoopThreads,
            int numQueryThreads,
            int maxQueuedRequests,
            DataEvolutionGlobalIndexTableQuery lookup,
            ServiceRequestStats stats) {
        super(
                "Global Index Query Server",
                bindAddress,
                bindPortIterator,
                numEventLoopThreads,
                numQueryThreads,
                GlobalIndexRequest.MAX_NETWORK_FRAME_BYTES,
                maxQueuedRequests);
        Preconditions.checkArgument(
                serverId >= 0 && serverId < numServers,
                "Server id %s is outside [0, %s).",
                serverId,
                numServers);
        this.serverId = serverId;
        this.numServers = numServers;
        this.serverEpoch = Preconditions.checkNotNull(serverEpoch);
        this.lookup = Preconditions.checkNotNull(lookup);
        this.stats = Preconditions.checkNotNull(stats);
    }

    @Override
    public AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse> initializeHandler() {
        MessageSerializer<GlobalIndexRequest, GlobalIndexResponse> serializer =
                new MessageSerializer<>(
                        new GlobalIndexRequest.Deserializer(),
                        new GlobalIndexResponse.Deserializer());
        return new GlobalIndexServerHandler(
                this, serverId, numServers, serverEpoch, lookup, serializer, stats);
    }

    @Override
    public void start() throws Throwable {
        super.start();
    }

    @Override
    public InetSocketAddress getServerAddress() {
        return super.getServerAddress();
    }

    int numQueuedRequests() {
        return ((ThreadPoolExecutor) getQueryExecutor()).getQueue().size();
    }

    int maxRequestFrameLength() {
        return getMaxFrameLength();
    }

    @Override
    public void shutdown() {
        try {
            shutdownServer().get(10L, TimeUnit.SECONDS);
            LOG.info("{} was shutdown successfully.", getServerName());
        } catch (Exception e) {
            LOG.warn("{} shutdown failed.", getServerName(), e);
        }
    }
}
