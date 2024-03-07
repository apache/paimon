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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.service.exceptions.UnknownPartitionBucketException;
import org.apache.paimon.service.messages.KvRequest;
import org.apache.paimon.service.messages.KvResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;

import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.table.sink.ChannelComputer.select;

/**
 * This handler dispatches asynchronous tasks, which query values and write the result to the
 * channel.
 *
 * <p>The network threads receive the message, deserialize it and dispatch the query task. The
 * actual query is handled in a separate thread as it might otherwise block the network threads
 * (file I/O etc.).
 */
@ChannelHandler.Sharable
public class KvServerHandler extends AbstractServerHandler<KvRequest, KvResponse> {

    private final int serverId;
    private final int numServers;
    private final TableQuery lookup;
    private final InternalRowSerializer valueSerializer;

    /**
     * Create the handler used by the {@link KvQueryServer}.
     *
     * @param server the {@link KvQueryServer} using the handler.
     * @param lookup to be queried.
     * @param serializer the {@link MessageSerializer} used to (de-) serialize the different
     *     messages.
     * @param stats server statistics collector.
     */
    public KvServerHandler(
            final KvQueryServer server,
            final int serverId,
            final int numServers,
            final TableQuery lookup,
            final MessageSerializer<KvRequest, KvResponse> serializer,
            final ServiceRequestStats stats) {
        super(server, serializer, stats);
        this.serverId = serverId;
        this.numServers = numServers;
        this.lookup = Preconditions.checkNotNull(lookup);
        this.valueSerializer = lookup.createValueSerializer();
    }

    @Override
    public CompletableFuture<KvResponse> handleRequest(
            final long requestId, final KvRequest request) {
        final CompletableFuture<KvResponse> responseFuture = new CompletableFuture<>();

        int selectServerId = select(request.partition(), request.bucket(), numServers);
        if (selectServerId != serverId) {
            responseFuture.completeExceptionally(
                    new UnknownPartitionBucketException(getServerName()));
            return responseFuture;
        }

        try {
            BinaryRow[] keys = request.keys();
            BinaryRow[] values = new BinaryRow[keys.length];
            for (int i = 0; i < values.length; i++) {
                InternalRow value =
                        this.lookup.lookup(request.partition(), request.bucket(), keys[i]);
                if (value != null) {
                    values[i] = valueSerializer.toBinaryRow(value).copy();
                }
            }
            responseFuture.complete(new KvResponse(values));
            return responseFuture;
        } catch (Throwable t) {
            String errMsg =
                    "Error while processing request with ID "
                            + requestId
                            + ". Caused by: "
                            + ExceptionUtils.stringifyException(t);
            responseFuture.completeExceptionally(new RuntimeException(errMsg));
            return responseFuture;
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }
}
