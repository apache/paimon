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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.write.WriteRestore;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_MEMORY;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/**
 * {@link OperatorCoordinator} for {@link TableWriteOperator}, to use a single point to obtain the
 * list of initialization files required for write operators.
 */
public class WriteOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private final FileStoreTable table;

    private ThreadPoolExecutor executor;

    private volatile Snapshot snapshot;
    private volatile FileStoreScan scan;

    public WriteOperatorCoordinator(FileStoreTable table) {
        this.table = table;
    }

    private synchronized void refreshOrCreateScan() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        if (!latestSnapshot.isPresent()) {
            return;
        }
        if (scan == null) {
            scan = table.store().newScan();
            if (table.coreOptions().manifestDeleteFileDropStats()) {
                scan.dropStats();
            }
        }
        snapshot = latestSnapshot.get();
        scan.withSnapshot(snapshot);
    }

    private synchronized WriteCoordinationResponse scanDataFiles(WriteCoordinationRequest request)
            throws IOException {
        if (scan == null) {
            return new WriteCoordinationResponse(null, null, null);
        }
        BinaryRow partition = deserializeBinaryRow(request.partition());
        int bucket = request.bucket();
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        List<ManifestEntry> entries = scan.withPartitionBucket(partition, bucket).plan().files();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);
        return new WriteCoordinationResponse(snapshot, restoreFiles, totalBuckets);
    }

    @Override
    public void start() throws Exception {
        executor = createCachedThreadPool(1, "WriteCoordinator");
        CoreOptions options = table.coreOptions();
        MemorySize cacheMemory =
                options.toConfiguration().get(SINK_WRITER_COORDINATOR_CACHE_MEMORY);
        SegmentsCache<Path> manifestCache = SegmentsCache.create(cacheMemory, Long.MAX_VALUE);
        table.setManifestCache(manifestCache);
        refreshOrCreateScan();
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
                    WriteCoordinationRequest writeRequest = (WriteCoordinationRequest) request;
                    try {
                        future.complete(scanDataFiles(writeRequest));
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        refreshOrCreateScan();
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
