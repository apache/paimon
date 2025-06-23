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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
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
    private Map<String, Long> latestCommittedIdentifiers;

    private volatile Snapshot snapshot;
    private volatile FileStoreScan dataFileScan;
    private volatile IndexFileHandler indexFileHandler;

    public WriteOperatorCoordinator(FileStoreTable table) {
        this.table = table;
    }

    private synchronized void refreshOrCreateScan() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        if (!latestSnapshot.isPresent()) {
            return;
        }
        if (dataFileScan == null) {
            dataFileScan = table.store().newScan();
            if (table.coreOptions().manifestDeleteFileDropStats()) {
                dataFileScan.dropStats();
            }
        }
        if (indexFileHandler == null) {
            indexFileHandler = table.store().newIndexFileHandler();
        }
        snapshot = latestSnapshot.get();
        dataFileScan.withSnapshot(snapshot);
    }

    private synchronized ScanCoordinationResponse scanDataFiles(ScanCoordinationRequest request)
            throws IOException {
        if (snapshot == null) {
            return new ScanCoordinationResponse(null, null, null, null, null);
        }

        BinaryRow partition = deserializeBinaryRow(request.partition());
        int bucket = request.bucket();

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        List<ManifestEntry> entries =
                dataFileScan.withPartitionBucket(partition, bucket).plan().files();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);

        IndexFileMeta dynamicBucketIndex = null;
        if (request.scanDynamicBucketIndex()) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null);
        }

        List<IndexFileMeta> deleteVectorsIndex = null;
        if (request.scanDeleteVectorsIndex()) {
            deleteVectorsIndex =
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new ScanCoordinationResponse(
                snapshot, totalBuckets, restoreFiles, dynamicBucketIndex, deleteVectorsIndex);
    }

    private synchronized LatestIdentifierResponse latestCommittedIdentifier(
            LatestIdentifierRequest request) {
        String user = request.user();
        long identifier =
                latestCommittedIdentifiers.computeIfAbsent(
                        user,
                        k ->
                                table.snapshotManager()
                                        .latestSnapshotOfUser(user)
                                        .map(Snapshot::commitIdentifier)
                                        .orElse(Long.MIN_VALUE));
        return new LatestIdentifierResponse(identifier);
    }

    @Override
    public void start() throws Exception {
        executor = createCachedThreadPool(1, "WriteCoordinator");
        latestCommittedIdentifiers = new ConcurrentHashMap<>();
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
                    try {
                        if (request instanceof ScanCoordinationRequest) {
                            future.complete(scanDataFiles((ScanCoordinationRequest) request));
                        } else if (request instanceof LatestIdentifierRequest) {
                            future.complete(
                                    latestCommittedIdentifier((LatestIdentifierRequest) request));
                        } else {
                            throw new UnsupportedOperationException(
                                    "Unsupported request type: " + request);
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
        // refresh latest snapshot for data & index files scan
        refreshOrCreateScan();
        // refresh latest committed identifiers for all users
        latestCommittedIdentifiers.clear();
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
