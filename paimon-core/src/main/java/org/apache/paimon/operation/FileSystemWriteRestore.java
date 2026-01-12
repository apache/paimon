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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** {@link WriteRestore} to restore files directly from file system. */
public class FileSystemWriteRestore implements WriteRestore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriteRestore.class);

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;

    private final Boolean usePrefetchManifestEntries;

    @Nullable
    private static Cache<String, PrefetchedManifestEntries> prefetchedManifestEntriesCache;

    public FileSystemWriteRestore(
            CoreOptions options,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            IndexFileHandler indexFileHandler) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFileHandler = indexFileHandler;
        if (options.manifestDeleteFileDropStats()) {
            if (this.scan != null) {
                this.scan.dropStats();
            }
        }
        this.usePrefetchManifestEntries = options.prefetchManifestEntries();
        initializeCacheIfNeeded();
    }

    private static synchronized void initializeCacheIfNeeded() {
        if (prefetchedManifestEntriesCache == null) {
            prefetchedManifestEntriesCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(Duration.ofMinutes(30))
                            .executor(Runnable::run)
                            .build();
            // .softValues() - not used as we want to hold onto a copy of manifest for each table we
            // are writing to
            // .maximumSize(...) - not used as number of keys is static = number of tables being
            // written to
            // .maximumWeight(...).weigher(...) - not used as there isn't a convenient way of
            // measuring memory size
            //    of a ManifestEntry object
        }
    }

    @Override
    public long latestCommittedIdentifier(String user) {
        return snapshotManager
                .latestSnapshotOfUserFromFilesystem(user)
                .map(Snapshot::commitIdentifier)
                .orElse(Long.MIN_VALUE);
    }

    private String getPrefetchManifestEntriesCacheKey() {
        return snapshotManager.tablePath().toString();
    }

    private List<ManifestEntry> fetchManifestEntries(
            Snapshot snapshot, @Nullable BinaryRow partition, @Nullable Integer bucket) {
        FileStoreScan snapshotScan = scan.withSnapshot(snapshot);
        if (partition != null && bucket != null) {
            snapshotScan = snapshotScan.withPartitionBucket(partition, bucket);
        }
        return snapshotScan.plan().files();
    }

    public PrefetchedManifestEntries prefetchManifestEntries(Snapshot snapshot) {
        LOG.info(
                "FileSystemWriteRestore started prefetching manifestEntries for table {}, snapshot {}",
                snapshotManager.tablePath(),
                snapshot.id());
        List<ManifestEntry> manifestEntries = fetchManifestEntries(snapshot, null, null);
        LOG.info(
                "FileSystemWriteRestore prefetched manifestEntries for table {}, snapshot {}: {} entries",
                snapshotManager.tablePath(),
                snapshot.id(),
                manifestEntries.size());

        RowType partitionType = scan.manifestsReader().partitionType();
        PrefetchedManifestEntries prefetchedManifestEntries =
                new PrefetchedManifestEntries(snapshot, partitionType, manifestEntries);

        if (prefetchedManifestEntriesCache == null) {
            initializeCacheIfNeeded();
        }
        prefetchedManifestEntriesCache.put(
                getPrefetchManifestEntriesCacheKey(), prefetchedManifestEntries);
        return prefetchedManifestEntries;
    }

    @Override
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        // NOTE: don't use snapshotManager.latestSnapshot() here,
        // because we don't want to flood the catalog with high concurrency
        Snapshot snapshot = snapshotManager.latestSnapshotFromFileSystem();
        if (snapshot == null) {
            return RestoreFiles.empty();
        }

        List<ManifestEntry> entries;
        if (usePrefetchManifestEntries) {
            PrefetchedManifestEntries prefetch =
                    prefetchedManifestEntriesCache.getIfPresent(
                            getPrefetchManifestEntriesCacheKey());
            if (prefetch == null || prefetch.snapshot.id() != snapshot.id()) {
                // manifest entries if snapshot ids don't match
                prefetch = prefetchManifestEntries(snapshot);
            }

            entries = prefetch.filter(partition, bucket);
        } else {
            entries = fetchManifestEntries(snapshot, partition, bucket);
        }
        LOG.info(
                "FileSystemWriteRestore filtered manifestEntries for {}, {}, {}: {} entries",
                snapshotManager.tablePath(),
                partition,
                bucket,
                entries.size());

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);

        IndexFileMeta dynamicBucketIndex = null;
        if (scanDynamicBucketIndex) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null);
        }

        List<IndexFileMeta> deleteVectorsIndex = null;
        if (scanDeleteVectorsIndex) {
            deleteVectorsIndex =
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new RestoreFiles(
                snapshot, totalBuckets, restoreFiles, dynamicBucketIndex, deleteVectorsIndex);
    }

    /**
     * Container for a {@link Snapshot}'s manifest entries, used by {@link FileSystemWriteRestore}
     * to broker thread-safe access to cached results.
     */
    public static class PrefetchedManifestEntries {

        private final Snapshot snapshot;
        private final RowType partitionType;
        private final List<ManifestEntry> manifestEntries;

        public PrefetchedManifestEntries(
                Snapshot snapshot, RowType partitionType, List<ManifestEntry> manifestEntries) {
            this.snapshot = snapshot;
            this.partitionType = partitionType;
            this.manifestEntries = manifestEntries;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public RowType partitionType() {
            return partitionType;
        }

        public List<ManifestEntry> manifestEntries() {
            return manifestEntries;
        }

        public List<ManifestEntry> filter(BinaryRow partition, int bucket) {
            PartitionPredicate partitionPredicate =
                    PartitionPredicate.fromMultiple(
                            partitionType, Collections.singletonList(partition));

            BucketFilter bucketFilter = BucketFilter.create(false, bucket, null, null);
            return manifestEntries.stream()
                    .filter(
                            m ->
                                    (partitionPredicate == null
                                                    || partitionPredicate.test(m.partition()))
                                            && bucketFilter.test(m.bucket(), m.totalBuckets()))
                    .collect(Collectors.toList());
        }
    }
}
