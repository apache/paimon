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
import org.apache.paimon.service.exceptions.GlobalIndexQueryException;
import org.apache.paimon.service.messages.GlobalIndexRequest;
import org.apache.paimon.service.messages.GlobalIndexResponse;
import org.apache.paimon.service.network.LegacyFailureFramePolicy;
import org.apache.paimon.service.network.NetworkClient;
import org.apache.paimon.service.network.messages.MessageSerializer;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.INVALID_REQUEST;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TIMEOUT;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.REQUEST_TOO_LARGE;
import static org.apache.paimon.service.exceptions.GlobalIndexQueryErrorCode.STALE_GENERATION;

/** Client for querying values from a sharded global-index query service. */
public class GlobalIndexQueryClient {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexQueryClient.class);
    private static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("Paimon Global Index Query Timeout %d")
                            .build());

    private final GlobalIndexQueryLocation queryLocation;
    private final RequestSender requestSender;
    private final long requestTimeoutNanos;
    @Nullable private final NetworkClient<GlobalIndexRequest, GlobalIndexResponse> networkClient;

    public GlobalIndexQueryClient(GlobalIndexQueryLocation queryLocation, int numEventLoopThreads) {
        this(queryLocation, numEventLoopThreads, DEFAULT_REQUEST_TIMEOUT);
    }

    public GlobalIndexQueryClient(
            GlobalIndexQueryLocation queryLocation,
            int numEventLoopThreads,
            Duration requestTimeout) {
        this.queryLocation = Preconditions.checkNotNull(queryLocation);
        this.requestTimeoutNanos = validateRequestTimeout(requestTimeout);
        MessageSerializer<GlobalIndexRequest, GlobalIndexResponse> serializer =
                new MessageSerializer<>(
                        new GlobalIndexRequest.Deserializer(),
                        new GlobalIndexResponse.Deserializer());
        this.networkClient =
                new NetworkClient<>(
                        "Global Index Query Client",
                        numEventLoopThreads,
                        serializer,
                        new DisabledServiceRequestStats(),
                        GlobalIndexResponse.MAX_NETWORK_FRAME_BYTES,
                        LegacyFailureFramePolicy.REJECT_SERIALIZED_THROWABLE);
        this.requestSender = networkClient::sendRequest;
    }

    GlobalIndexQueryClient(GlobalIndexQueryLocation queryLocation, RequestSender requestSender) {
        this(queryLocation, requestSender, DEFAULT_REQUEST_TIMEOUT);
    }

    GlobalIndexQueryClient(
            GlobalIndexQueryLocation queryLocation,
            RequestSender requestSender,
            Duration requestTimeout) {
        this.queryLocation = Preconditions.checkNotNull(queryLocation);
        this.requestSender = Preconditions.checkNotNull(requestSender);
        this.requestTimeoutNanos = validateRequestTimeout(requestTimeout);
        this.networkClient = null;
    }

    /**
     * Queries arbitrary keys, grouping them by their current server address and restoring the
     * original key order in the result.
     */
    public CompletableFuture<BinaryRow[]> getValues(BinaryRow[] keys) {
        return getValuesWithMetadata(keys).thenApply(LookupResult::values);
    }

    /** Queries arbitrary keys and returns the common advertised snapshot fence with the values. */
    public CompletableFuture<LookupResult> getValuesWithMetadata(BinaryRow[] keys) {
        Preconditions.checkNotNull(keys, "Global-index query keys are null.");
        if (keys.length == 0) {
            return CompletableFuture.completedFuture(
                    new LookupResult(new BinaryRow[0], Long.MIN_VALUE, -1L, null));
        }
        if (keys.length > GlobalIndexRequest.MAX_KEYS) {
            throw new GlobalIndexQueryException(
                    REQUEST_TOO_LARGE,
                    String.format(
                            "Global-index query batch contains %s keys; maximum is %s.",
                            keys.length, GlobalIndexRequest.MAX_KEYS));
        }
        long totalKeyBytes = 0L;
        BinaryRow[] normalizedKeys = new BinaryRow[keys.length];
        for (int i = 0; i < keys.length; i++) {
            BinaryRow key = keys[i];
            Preconditions.checkNotNull(key, "Global-index query key is null.");
            BinaryRow normalized = GlobalIndexQueryServiceUtils.normalizeKey(key);
            normalizedKeys[i] = normalized;
            totalKeyBytes += Integer.BYTES + normalized.getSizeInBytes();
            if (totalKeyBytes > GlobalIndexRequest.MAX_TOTAL_KEY_BYTES) {
                throw new GlobalIndexQueryException(
                        REQUEST_TOO_LARGE,
                        String.format(
                                "Global-index query key payload is %s bytes; maximum is %s.",
                                totalKeyBytes, GlobalIndexRequest.MAX_TOTAL_KEY_BYTES));
            }
        }

        CompletableFuture<LookupResult> result = new CompletableFuture<>();
        execute(result, normalizedKeys, false, false);
        return result;
    }

    private void execute(
            CompletableFuture<LookupResult> result,
            BinaryRow[] keys,
            boolean forceUpdate,
            boolean retried) {
        if (result.isDone()) {
            return;
        }

        final CompletableFuture<LookupResult> attempt;
        try {
            attempt = sendGrouped(keys, forceUpdate);
        } catch (Throwable t) {
            Throwable cause = unwrapCompletion(t);
            if (!retried && isRetryable(cause)) {
                LOG.debug(
                        "Retrying global-index query after refreshing service addresses: {}",
                        cause.getMessage());
                execute(result, keys, true, true);
            } else {
                result.completeExceptionally(cause);
            }
            return;
        }

        attempt.whenComplete(
                (values, throwable) -> {
                    if (result.isDone()) {
                        return;
                    }
                    if (throwable == null) {
                        result.complete(values);
                        return;
                    }

                    Throwable cause = unwrapCompletion(throwable);
                    if (!retried && isRetryable(cause)) {
                        LOG.debug(
                                "Retrying global-index query after refreshing service addresses: {}",
                                cause.getMessage());
                        execute(result, keys, true, true);
                    } else {
                        result.completeExceptionally(cause);
                    }
                });
        result.whenComplete((ignored, throwable) -> attempt.cancel(false));
    }

    private CompletableFuture<LookupResult> sendGrouped(BinaryRow[] keys, boolean forceUpdate)
            throws IOException {
        Map<EndpointKey, RequestGroup> groups = new LinkedHashMap<>();
        ServiceFence serviceFence = null;
        boolean refresh = forceUpdate;
        for (int i = 0; i < keys.length; i++) {
            GlobalIndexQueryEndpoint endpoint = queryLocation.getLocation(keys[i], refresh);
            refresh = false;
            Preconditions.checkNotNull(
                    endpoint, "Cannot find endpoint for global-index query key.");
            ServiceFence currentFence = ServiceFence.from(endpoint);
            if (serviceFence == null) {
                serviceFence = currentFence;
            } else if (!serviceFence.equals(currentFence)) {
                throw new GlobalIndexQueryException(
                        STALE_GENERATION,
                        "Global-index discovery returned mixed service generations for one batch.");
            }
            EndpointKey endpointKey = EndpointKey.from(endpoint);
            groups.computeIfAbsent(endpointKey, ignored -> new RequestGroup()).add(i, keys[i]);
        }

        List<GroupRequest> requests = new ArrayList<>(groups.size());
        try {
            for (Map.Entry<EndpointKey, RequestGroup> entry : groups.entrySet()) {
                EndpointKey endpoint = entry.getKey();
                RequestGroup group = entry.getValue();
                CompletableFuture<GlobalIndexResponse> response =
                        withRequestTimeout(
                                requestSender.send(
                                        endpoint.address,
                                        new GlobalIndexRequest(
                                                endpoint.serverEpoch,
                                                endpoint.servedGeneration,
                                                group.keysArray())),
                                endpoint.address);
                requests.add(new GroupRequest(group, endpoint, response));
            }
        } catch (RuntimeException | Error t) {
            for (GroupRequest request : requests) {
                request.response.cancel(false);
            }
            throw t;
        }
        ServiceFence batchFence =
                Preconditions.checkNotNull(
                        serviceFence, "Global-index query batch has no discovery fence.");

        CompletableFuture<?>[] futures =
                requests.stream()
                        .map(request -> request.response)
                        .toArray(CompletableFuture[]::new);
        CompletableFuture<LookupResult> combined =
                CompletableFuture.allOf(futures)
                        .handle(
                                (ignored, throwable) -> {
                                    if (throwable != null) {
                                        throw new CompletionException(selectFailure(requests));
                                    }

                                    GlobalIndexQueryException responseFailure =
                                            validateAndSelectResponseFailure(requests);
                                    if (responseFailure != null) {
                                        throw responseFailure;
                                    }

                                    BinaryRow[] values = new BinaryRow[keys.length];
                                    for (GroupRequest request : requests) {
                                        GlobalIndexResponse response = request.response.join();
                                        BinaryRow[] groupValues = response.values();
                                        if (groupValues.length != request.group.size()) {
                                            throw new GlobalIndexQueryException(
                                                    INVALID_REQUEST,
                                                    String.format(
                                                            "Global-index query response size %s does not match request size %s.",
                                                            groupValues.length,
                                                            request.group.size()));
                                        }
                                        for (int i = 0; i < groupValues.length; i++) {
                                            values[request.group.position(i)] = groupValues[i];
                                        }
                                    }
                                    return new LookupResult(
                                            values,
                                            batchFence.servedGeneration,
                                            batchFence.servedSnapshotId,
                                            batchFence.snapshotUuid);
                                });
        combined.whenComplete(
                (ignored, throwable) -> {
                    if (combined.isCancelled()) {
                        for (GroupRequest request : requests) {
                            request.response.cancel(false);
                        }
                    }
                });
        return combined;
    }

    @Nullable
    private static GlobalIndexQueryException validateAndSelectResponseFailure(
            List<GroupRequest> requests) {
        // Validate every group before interpreting any structured business error. Otherwise an
        // error response from a stale or replaced server could bypass the batch fence.
        for (GroupRequest request : requests) {
            request.validate(request.response.join());
        }

        GlobalIndexQueryException retryable = null;
        for (GroupRequest request : requests) {
            GlobalIndexResponse response = request.response.join();
            if (!response.isSuccess()) {
                GlobalIndexQueryException failure = response.toException();
                if (!failure.retryable()) {
                    return failure;
                }
                if (retryable == null) {
                    retryable = failure;
                }
            }
        }
        return retryable;
    }

    private static Throwable selectFailure(List<GroupRequest> requests) {
        Throwable retryable = null;
        for (GroupRequest request : requests) {
            try {
                request.response.join();
            } catch (CompletionException e) {
                Throwable cause = unwrapCompletion(e);
                if (!isRetryable(cause)) {
                    return cause;
                }
                if (retryable == null) {
                    retryable = cause;
                }
            }
        }
        return Preconditions.checkNotNull(retryable, "No failed global-index request found.");
    }

    private static boolean isRetryable(Throwable throwable) {
        return throwable instanceof GlobalIndexQueryException
                        && ((GlobalIndexQueryException) throwable).retryable()
                || containsCause(throwable, ConnectException.class)
                || containsCause(throwable, ClosedChannelException.class);
    }

    private CompletableFuture<GlobalIndexResponse> withRequestTimeout(
            CompletableFuture<GlobalIndexResponse> source, InetSocketAddress address) {
        CompletableFuture<GlobalIndexResponse> bounded = new CompletableFuture<>();
        ScheduledFuture<?> timeout =
                TIMEOUT_SCHEDULER.schedule(
                        () -> {
                            GlobalIndexQueryException failure =
                                    new GlobalIndexQueryException(
                                            REQUEST_TIMEOUT,
                                            true,
                                            String.format(
                                                    "Global-index request to %s timed out after %s ms.",
                                                    address,
                                                    TimeUnit.NANOSECONDS.toMillis(
                                                            requestTimeoutNanos)));
                            if (bounded.completeExceptionally(failure)) {
                                source.cancel(false);
                            }
                        },
                        requestTimeoutNanos,
                        TimeUnit.NANOSECONDS);
        source.whenComplete(
                (response, throwable) -> {
                    timeout.cancel(false);
                    if (throwable == null) {
                        bounded.complete(response);
                    } else {
                        bounded.completeExceptionally(unwrapCompletion(throwable));
                    }
                });
        bounded.whenComplete(
                (ignored, throwable) -> {
                    timeout.cancel(false);
                    if (bounded.isCancelled()) {
                        source.cancel(false);
                    }
                });
        return bounded;
    }

    private static long validateRequestTimeout(Duration requestTimeout) {
        Preconditions.checkNotNull(requestTimeout, "Global-index request timeout is null.");
        Preconditions.checkArgument(
                !requestTimeout.isZero() && !requestTimeout.isNegative(),
                "Global-index request timeout must be positive.");
        return requestTimeout.toNanos();
    }

    private static boolean containsCause(Throwable throwable, Class<? extends Throwable> expected) {
        Throwable current = throwable;
        while (current != null) {
            if (expected.isInstance(current)) {
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

    private static Throwable unwrapCompletion(Throwable throwable) {
        Throwable current = throwable;
        while ((current instanceof CompletionException || current instanceof ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    public void shutdown() {
        try {
            shutdownFuture().get(60L, TimeUnit.SECONDS);
            if (networkClient != null) {
                LOG.info("{} was shutdown successfully.", networkClient.getClientName());
            }
        } catch (Exception e) {
            LOG.warn("Global Index Query Client shutdown failed.", e);
        }
    }

    public CompletableFuture<Void> shutdownFuture() {
        return networkClient == null
                ? CompletableFuture.completedFuture(null)
                : networkClient.shutdown();
    }

    int numPendingNetworkRequests() {
        return networkClient == null ? 0 : networkClient.getNumPendingRequests();
    }

    /** Values returned from one consistently fenced discovery snapshot. */
    public static final class LookupResult {
        private final BinaryRow[] values;
        private final long servedGeneration;
        private final long servedSnapshotId;
        @Nullable private final String snapshotUuid;

        private LookupResult(
                BinaryRow[] values,
                long servedGeneration,
                long servedSnapshotId,
                @Nullable String snapshotUuid) {
            this.values = values;
            this.servedGeneration = servedGeneration;
            this.servedSnapshotId = servedSnapshotId;
            this.snapshotUuid = snapshotUuid;
        }

        public BinaryRow[] values() {
            return values;
        }

        public long servedGeneration() {
            return servedGeneration;
        }

        public long servedSnapshotId() {
            return servedSnapshotId;
        }

        @Nullable
        public String snapshotUuid() {
            return snapshotUuid;
        }
    }

    @FunctionalInterface
    interface RequestSender {
        CompletableFuture<GlobalIndexResponse> send(
                InetSocketAddress address, GlobalIndexRequest request);
    }

    private static class RequestGroup {
        private final List<Integer> positions = new ArrayList<>();
        private final List<BinaryRow> keys = new ArrayList<>();

        private void add(int position, BinaryRow key) {
            positions.add(position);
            keys.add(key);
        }

        private int size() {
            return keys.size();
        }

        private int position(int index) {
            return positions.get(index);
        }

        private BinaryRow[] keysArray() {
            return keys.toArray(new BinaryRow[0]);
        }
    }

    private static class GroupRequest {
        private final RequestGroup group;
        private final EndpointKey endpoint;
        private final CompletableFuture<GlobalIndexResponse> response;

        private GroupRequest(
                RequestGroup group,
                EndpointKey endpoint,
                CompletableFuture<GlobalIndexResponse> response) {
            this.group = group;
            this.endpoint = endpoint;
            this.response = response;
        }

        private void validate(GlobalIndexResponse response) {
            if (!endpoint.serverEpoch.equals(response.serverEpoch())
                    || endpoint.servedGeneration != response.servedGeneration()
                    || endpoint.servedSnapshotId != response.servedSnapshotId()) {
                throw new GlobalIndexQueryException(
                        STALE_GENERATION,
                        String.format(
                                "Global-index response fence (%s, %s, %s) does not match expected (%s, %s, %s).",
                                response.serverEpoch(),
                                response.servedGeneration(),
                                response.servedSnapshotId(),
                                endpoint.serverEpoch,
                                endpoint.servedGeneration,
                                endpoint.servedSnapshotId));
            }
        }
    }

    private static class EndpointKey {
        private final InetSocketAddress address;
        private final String serverEpoch;
        private final long servedGeneration;
        private final long servedSnapshotId;

        private EndpointKey(
                InetSocketAddress address,
                String serverEpoch,
                long servedGeneration,
                long servedSnapshotId) {
            this.address = address;
            this.serverEpoch = serverEpoch;
            this.servedGeneration = servedGeneration;
            this.servedSnapshotId = servedSnapshotId;
        }

        private static EndpointKey from(GlobalIndexQueryEndpoint endpoint) {
            return new EndpointKey(
                    endpoint.address(),
                    endpoint.serverEpoch(),
                    endpoint.servedGeneration(),
                    endpoint.servedSnapshotId());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointKey)) {
                return false;
            }
            EndpointKey that = (EndpointKey) o;
            return servedGeneration == that.servedGeneration
                    && servedSnapshotId == that.servedSnapshotId
                    && address.equals(that.address)
                    && serverEpoch.equals(that.serverEpoch);
        }

        @Override
        public int hashCode() {
            int result = address.hashCode();
            result = 31 * result + serverEpoch.hashCode();
            result = 31 * result + Long.hashCode(servedGeneration);
            return 31 * result + Long.hashCode(servedSnapshotId);
        }
    }

    private static class ServiceFence {
        private final long servedGeneration;
        private final long servedSnapshotId;
        @Nullable private final String snapshotUuid;

        private ServiceFence(
                long servedGeneration, long servedSnapshotId, @Nullable String snapshotUuid) {
            this.servedGeneration = servedGeneration;
            this.servedSnapshotId = servedSnapshotId;
            this.snapshotUuid = snapshotUuid;
        }

        private static ServiceFence from(GlobalIndexQueryEndpoint endpoint) {
            return new ServiceFence(
                    endpoint.servedGeneration(),
                    endpoint.servedSnapshotId(),
                    endpoint.snapshotUuid());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ServiceFence)) {
                return false;
            }
            ServiceFence that = (ServiceFence) o;
            return servedGeneration == that.servedGeneration
                    && servedSnapshotId == that.servedSnapshotId
                    && java.util.Objects.equals(snapshotUuid, that.snapshotUuid);
        }

        @Override
        public int hashCode() {
            int result = Long.hashCode(servedGeneration);
            result = 31 * result + Long.hashCode(servedSnapshotId);
            return 31 * result + (snapshotUuid == null ? 0 : snapshotUuid.hashCode());
        }
    }
}
