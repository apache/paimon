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
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.NetworkServer;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/** The default implementation of the {@link QueryServer}. */
public class KvQueryServer extends NetworkServer<KvRequest, KvResponse> implements QueryServer {

    private static final Logger LOG = LoggerFactory.getLogger(KvQueryServer.class);

    private final int serverId;
    private final int numServers;
    private final TableQuery lookup;
    private final ServiceRequestStats stats;

    public KvQueryServer(
            final int serverId,
            final int numServers,
            final String bindAddress,
            final Iterator<Integer> bindPortIterator,
            final Integer numEventLoopThreads,
            final Integer numQueryThreads,
            final TableQuery lookup,
            final ServiceRequestStats stats) {
        super(
                "Kv Query Server",
                bindAddress,
                bindPortIterator,
                numEventLoopThreads,
                numQueryThreads);
        this.serverId = serverId;
        this.numServers = numServers;
        this.stats = Preconditions.checkNotNull(stats);
        this.lookup = Preconditions.checkNotNull(lookup);
    }

    @Override
    public AbstractServerHandler<KvRequest, KvResponse> initializeHandler() {
        MessageSerializer<KvRequest, KvResponse> serializer =
                new MessageSerializer<>(
                        new KvRequest.KvRequestDeserializer(),
                        new KvResponse.KvResponseDeserializer());
        return new KvServerHandler(this, serverId, numServers, lookup, serializer, stats);
    }

    @Override
    public void start() throws Throwable {
        super.start();
    }

    @Override
    public InetSocketAddress getServerAddress() {
        return super.getServerAddress();
    }

    @Override
    public void shutdown() {
        try {
            shutdownServer().get(10L, TimeUnit.SECONDS);
            LOG.info("{} was shutdown successfully.", getServerName());
        } catch (Exception e) {
            LOG.warn("{} shutdown failed: {}", getServerName(), e);
        }
    }
}
