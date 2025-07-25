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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.InstantiationUtil.deserializeObject;
import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Coordinator for a table, to use a single point to obtain the list of initialization files
 * required for write operators.
 */
public class TableWriteCoordinator {

    private final FileStoreTable table;
    private final Map<String, Long> latestCommittedIdentifiers;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;
    private final int pageSize;
    private final Cache<CoordinationKey, byte[]> pagedCoordination;

    private volatile Snapshot snapshot;

    public TableWriteCoordinator(FileStoreTable table) {
        this.table = table;
        checkNotNull(table.getManifestCache());
        this.latestCommittedIdentifiers = new ConcurrentHashMap<>();
        this.scan = table.store().newScan();
        if (table.coreOptions().manifestDeleteFileDropStats()) {
            scan.dropStats();
        }
        this.indexFileHandler = table.store().newIndexFileHandler();
        this.pageSize =
                (int)
                        table.coreOptions()
                                .toConfiguration()
                                .get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE)
                                .getBytes();
        this.pagedCoordination =
                Caffeine.newBuilder()
                        .executor(Runnable::run)
                        .expireAfterAccess(Duration.ofMinutes(30))
                        .build();
        refresh();
    }

    private synchronized void refresh() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        if (!latestSnapshot.isPresent()) {
            return;
        }
        this.snapshot = latestSnapshot.get();
        this.scan.withSnapshot(snapshot);
    }

    public synchronized PagedCoordinationResponse scan(PagedCoordinationRequest request)
            throws IOException {
        if (snapshot == null) {
            return new PagedCoordinationResponse(
                    serializeObject(new ScanCoordinationResponse(null, null, null, null, null)),
                    null);
        }

        Integer pageToken = request.pageToken();
        CoordinationKey requestKey = new CoordinationKey(request.content(), request.requestId());
        if (pageToken != null) {
            byte[] full = pagedCoordination.getIfPresent(requestKey);
            if (full == null) {
                throw new RuntimeException(
                        "This is a bug for write coordinator, request non existence content.");
            }
            int len = Math.min(full.length - pageToken, pageSize);
            byte[] content = Arrays.copyOfRange(full, pageToken, pageToken + len);
            Integer nextPageToken = pageToken + len;
            if (nextPageToken >= full.length) {
                nextPageToken = null;
                pagedCoordination.invalidate(requestKey);
            }
            return new PagedCoordinationResponse(content, nextPageToken);
        }

        ScanCoordinationRequest coordination;
        try {
            coordination = deserializeObject(request.content(), getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        ScanCoordinationResponse response = scan(coordination);
        byte[] full = serializeObject(response);
        if (full.length <= pageSize) {
            return new PagedCoordinationResponse(full, null);
        }

        pagedCoordination.put(requestKey, full);
        byte[] content = Arrays.copyOfRange(full, 0, pageSize);
        return new PagedCoordinationResponse(content, pageSize);
    }

    public synchronized ScanCoordinationResponse scan(ScanCoordinationRequest request)
            throws IOException {
        if (snapshot == null) {
            return new ScanCoordinationResponse(null, null, null, null, null);
        }

        BinaryRow partition = deserializeBinaryRow(request.partition());
        int bucket = request.bucket();

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        List<ManifestEntry> entries = scan.withPartitionBucket(partition, bucket).plan().files();
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

    public synchronized long latestCommittedIdentifier(String user) {
        return latestCommittedIdentifiers.computeIfAbsent(user, this::computeLatestIdentifier);
    }

    private synchronized long computeLatestIdentifier(String user) {
        Optional<Snapshot> snapshotOptional = table.snapshotManager().latestSnapshotOfUser(user);
        if (!snapshotOptional.isPresent()) {
            return Long.MIN_VALUE;
        }

        Snapshot latestSnapshotOfUser = snapshotOptional.get();
        if (snapshot == null || latestSnapshotOfUser.id() > snapshot.id()) {
            snapshot = latestSnapshotOfUser;
            scan.withSnapshot(snapshot);
        }
        return latestSnapshotOfUser.commitIdentifier();
    }

    public void checkpoint() {
        // refresh latest snapshot for data & index files scan
        refresh();
        // refresh latest committed identifiers for all users
        latestCommittedIdentifiers.clear();
    }

    private static class CoordinationKey {

        private final byte[] content;
        private final String uuid;

        private CoordinationKey(byte[] content, String uuid) {
            this.content = content;
            this.uuid = uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CoordinationKey that = (CoordinationKey) o;
            return Objects.deepEquals(content, that.content) && Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(content), uuid);
        }
    }
}
