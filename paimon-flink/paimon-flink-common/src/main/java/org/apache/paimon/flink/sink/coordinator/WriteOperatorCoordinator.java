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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_MEMORY;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/**
 * {@link OperatorCoordinator} for {@link TableWriteOperator}, to use a single point to obtain the
 * list of initialization files required for write operators.
 */
public class WriteOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private final FileStoreTable table;

    private ThreadPoolExecutor executor;
    private TableWriteCoordinator coordinator;

    public WriteOperatorCoordinator(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void start() throws Exception {
        executor = createCachedThreadPool(1, "WriteCoordinator");
        MemorySize cacheMemory =
                table.coreOptions().toConfiguration().get(SINK_WRITER_COORDINATOR_CACHE_MEMORY);
        SegmentsCache<Path> manifestCache = SegmentsCache.create(cacheMemory, Long.MAX_VALUE);
        table.setManifestCache(manifestCache);
        coordinator = new TableWriteCoordinator(table);
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        CoordinationResponse response;
                        if (request instanceof PagedCoordinationRequest) {
                            response = coordinator.scan((PagedCoordinationRequest) request);
                        } else if (request instanceof LatestIdentifierRequest) {
                            response =
                                    new LatestIdentifierResponse(
                                            coordinator.latestCommittedIdentifier(
                                                    ((LatestIdentifierRequest) request).user()));
                        } else {
                            throw new UnsupportedOperationException(
                                    "Unsupported request type: " + request);
                        }
                        future.complete(CoordinationResponseUtils.wrap(response));
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        coordinator.checkpoint();
        executor.execute(
                () -> {
                    try {
                        result.complete(new byte[0]);
                    } catch (Throwable throwable) {
                        result.completeExceptionally(new CompletionException(throwable));
                    }
                });
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {}

    @Override
    public void subtaskReset(int subtask, long checkpointId) {}

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {}

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    /** Provider for {@link WriteOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorId;
        private final FileStoreTable table;

        public Provider(OperatorID operatorId, FileStoreTable table) {
            this.operatorId = operatorId;
            this.table = table;
        }

        @Override
        public OperatorID getOperatorId() {
            return this.operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new WriteOperatorCoordinator(table);
        }
    }
}
