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
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.AbstractServerHandler;
import org.apache.paimon.service.network.LegacyFailureFramePolicy;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.ServiceRequestStats;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.query.DataEvolutionGlobalIndexTableQuery;
import org.apache.paimon.table.query.DuplicateLookupKeyException;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.QueryServiceNotReadyException;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.netty4.io.netty.channel.ChannelHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.DUPLICATE_KEY;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INTERNAL_ERROR;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.NOT_READY;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.OVERLOADED;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.STALE_GENERATION;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.UNKNOWN_KEY_SHARD;

/** Handles requests for one global-index key-hash shard. */
@ChannelHandler.Sharable
public class GlobalIndexServerHandler
        extends AbstractServerHandler<GlobalIndexRequest, GlobalIndexResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexServerHandler.class);

    private final int serverId;
    private final int numServers;
    private final String serverEpoch;
    private final DataEvolutionGlobalIndexTableQuery lookup;
    private final InternalRowSerializer valueSerializer;

    public GlobalIndexServerHandler(
            GlobalIndexQueryServer server,
            int serverId,
            int numServers,
            String serverEpoch,
            DataEvolutionGlobalIndexTableQuery lookup,
            MessageSerializer<GlobalIndexRequest, GlobalIndexResponse> serializer,
            ServiceRequestStats stats) {
        super(server, serializer, stats, LegacyFailureFramePolicy.REJECT_SERIALIZED_THROWABLE);
        this.serverId = serverId;
        this.numServers = numServers;
        this.serverEpoch = Preconditions.checkNotNull(serverEpoch);
        this.lookup = Preconditions.checkNotNull(lookup);
        this.valueSerializer = lookup.createValueSerializer();
    }

    @Override
    public CompletableFuture<GlobalIndexResponse> handleRequest(
            long requestId, GlobalIndexRequest request) {
        CompletableFuture<GlobalIndexResponse> responseFuture = new CompletableFuture<>();

        try {
            if (!serverEpoch.equals(request.serverEpoch())) {
                responseFuture.complete(
                        failureResponse(
                                STALE_GENERATION,
                                String.format(
                                        "%s : Requested server epoch '%s', but this server uses '%s'.",
                                        getServerName(), request.serverEpoch(), serverEpoch)));
                return responseFuture;
            }

            // Fence cached descriptors against the newest observed generation, not just the last
            // published generation. The old state is retained during a shadow rebuild, but
            // accepting it after an append tail was observed could turn a not-yet-indexed key into
            // a false MISS.
            long acceptedGeneration = lookup.latestGeneration();
            if (request.servedGeneration() != acceptedGeneration) {
                responseFuture.complete(
                        failureResponse(
                                STALE_GENERATION,
                                String.format(
                                        "%s : Requested global-index generation %s, but this server serves %s.",
                                        getServerName(),
                                        request.servedGeneration(),
                                        acceptedGeneration)));
                return responseFuture;
            }

            BinaryRow[] keys = request.keys();
            for (BinaryRow key : keys) {
                if (GlobalIndexQueryServiceUtils.route(key, numServers) != serverId) {
                    responseFuture.complete(
                            failureResponse(
                                    UNKNOWN_KEY_SHARD,
                                    getServerName()
                                            + " : The server does not own the requested global-index key shard."));
                    return responseFuture;
                }
            }

            BinaryRow[] values = new BinaryRow[keys.length];
            Map<BinaryRow, BinaryRow> cachedValues = new HashMap<>();
            long totalValueBytes = 0L;
            for (int i = 0; i < keys.length; i++) {
                BinaryRow binaryValue;
                if (cachedValues.containsKey(keys[i])) {
                    binaryValue = cachedValues.get(keys[i]);
                } else {
                    InternalRow value =
                            lookup.lookup(BinaryRow.EMPTY_ROW, BucketMode.UNAWARE_BUCKET, keys[i]);
                    binaryValue = value == null ? null : valueSerializer.toBinaryRow(value);
                    if (binaryValue != null) {
                        long candidateBytes =
                                totalValueBytes + Integer.BYTES + binaryValue.getSizeInBytes();
                        GlobalIndexResponse.validateTotalValueBytes(candidateBytes);
                        binaryValue = binaryValue.copy();
                        totalValueBytes = candidateBytes;
                    }
                    cachedValues.put(keys[i], binaryValue);
                    values[i] = binaryValue;
                    continue;
                }
                if (binaryValue != null) {
                    totalValueBytes += Integer.BYTES + binaryValue.getSizeInBytes();
                    GlobalIndexResponse.validateTotalValueBytes(totalValueBytes);
                }
                values[i] = binaryValue;
            }
            long responseSnapshotId = lookup.servedSnapshotId();
            long responseGeneration = lookup.servedGeneration();
            long acceptedResponseGeneration = lookup.latestGeneration();
            if (responseGeneration != request.servedGeneration()
                    || acceptedResponseGeneration != request.servedGeneration()) {
                responseFuture.complete(
                        failureResponse(
                                STALE_GENERATION,
                                String.format(
                                        "%s : Requested global-index generation %s, but this server serves %s.",
                                        getServerName(),
                                        request.servedGeneration(),
                                        acceptedResponseGeneration)));
                return responseFuture;
            }
            responseFuture.complete(
                    new GlobalIndexResponse(
                            serverEpoch, responseGeneration, responseSnapshotId, values));
        } catch (QueryServiceNotReadyException e) {
            responseFuture.complete(failureResponse(NOT_READY, e.getMessage()));
        } catch (DuplicateLookupKeyException e) {
            responseFuture.complete(failureResponse(DUPLICATE_KEY, e.getMessage()));
        } catch (IllegalArgumentException e) {
            responseFuture.complete(failureResponse(INVALID_REQUEST, e.getMessage()));
        } catch (GlobalIndexQueryException e) {
            responseFuture.complete(
                    GlobalIndexResponse.failure(
                            serverEpoch,
                            safeLatestGeneration(),
                            safeServedSnapshotId(),
                            e.errorCode(),
                            e.retryable(),
                            e.getMessage()));
        } catch (Throwable t) {
            LOG.error(
                    "{} : Internal error while processing request {}.",
                    getServerName(),
                    requestId,
                    t);
            responseFuture.complete(
                    failureResponse(
                            INTERNAL_ERROR,
                            String.format(
                                    "%s : Internal error while processing request %s.",
                                    getServerName(), requestId)));
        }
        return responseFuture;
    }

    @Override
    protected GlobalIndexResponse rejectedExecutionResponse(
            long requestId, GlobalIndexRequest request) {
        return failureResponse(
                OVERLOADED,
                String.format(
                        "%s : Query executor is overloaded; rejected request %s.",
                        getServerName(), requestId));
    }

    private GlobalIndexResponse failureResponse(
            org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode errorCode,
            String message) {
        return GlobalIndexResponse.failure(
                serverEpoch, safeLatestGeneration(), safeServedSnapshotId(), errorCode, message);
    }

    private long safeLatestGeneration() {
        try {
            return lookup.latestGeneration();
        } catch (Throwable ignored) {
            return Long.MIN_VALUE;
        }
    }

    private long safeServedSnapshotId() {
        try {
            return lookup.servedSnapshotId();
        } catch (Throwable ignored) {
            return -1L;
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }
}
