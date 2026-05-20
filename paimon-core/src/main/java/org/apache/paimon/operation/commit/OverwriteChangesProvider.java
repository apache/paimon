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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A stateful {@link CommitChangesProvider} for overwrite operations that caches the current files
 * of the target partitions across commit retries to avoid repeated full scans.
 *
 * <p>On retry, if the latest snapshot has changed, the cache is reused only when the snapshots in
 * between have not touched the target partitions; otherwise it is rebuilt by a full scan.
 */
public class OverwriteChangesProvider implements CommitChangesProvider {

    private static final Logger LOG = LoggerFactory.getLogger(OverwriteChangesProvider.class);

    private final Supplier<FileStoreScan> scanSupplier;
    private final SnapshotManager snapshotManager;
    private final IndexManifestFile indexManifestFile;
    private final boolean dropStats;
    private final int numBucket;
    private final List<ManifestEntry> newChanges;
    private final List<IndexManifestEntry> newIndexFiles;
    @Nullable private final PartitionPredicate partitionFilter;

    @Nullable private Snapshot cachedSnapshot;
    private final List<ManifestEntry> cachedManifestEntries = new ArrayList<>();
    private final List<IndexManifestEntry> cachedIndexManifestEntries = new ArrayList<>();

    @VisibleForTesting int fullScanManifestCount;
    @VisibleForTesting int fullScanIndexCount;
    @VisibleForTesting int deltaProbeCount;

    public OverwriteChangesProvider(
            Supplier<FileStoreScan> scanSupplier,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile,
            boolean dropStats,
            int numBucket,
            List<ManifestEntry> newChanges,
            List<IndexManifestEntry> newIndexFiles,
            @Nullable PartitionPredicate partitionFilter) {
        this.scanSupplier = scanSupplier;
        this.snapshotManager = snapshotManager;
        this.indexManifestFile = indexManifestFile;
        this.dropStats = dropStats;
        this.numBucket = numBucket;
        this.newChanges = newChanges;
        this.newIndexFiles = newIndexFiles;
        this.partitionFilter = partitionFilter;
    }

    @Override
    public CommitChanges provide(@Nullable Snapshot latestSnapshot) {
        if (latestSnapshot == null) {
            return new CommitChanges(newChanges, Collections.emptyList(), newIndexFiles);
        }

        if (cachedSnapshot == null) {
            fullScanManifestEntries(latestSnapshot);
            fullScanIndexManifestEntries(latestSnapshot);
            cachedSnapshot = latestSnapshot;
        } else {
            if (cachedSnapshot.id() > latestSnapshot.id()) {
                throw new IllegalStateException(
                        "Cached snapshot id "
                                + cachedSnapshot.id()
                                + " is greater than latest snapshot id "
                                + latestSnapshot.id());
            }

            if (cachedSnapshot.id() < latestSnapshot.id()) {
                updateCache(latestSnapshot);
            }
        }

        return buildResult();
    }

    private void fullScanManifestEntries(Snapshot latestSnapshot) {
        cachedManifestEntries.clear();
        FileStoreScan scan =
                newScan()
                        .withSnapshot(latestSnapshot)
                        .withPartitionFilter(partitionFilter)
                        .withKind(ScanMode.ALL);
        cachedManifestEntries.addAll(scan.plan().files());
        fullScanManifestCount++;
    }

    private void fullScanIndexManifestEntries(Snapshot latestSnapshot) {
        cachedIndexManifestEntries.clear();
        if (latestSnapshot.indexManifest() != null) {
            for (IndexManifestEntry entry :
                    indexManifestFile.read(latestSnapshot.indexManifest())) {
                if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                    cachedIndexManifestEntries.add(entry);
                }
            }
        }
        fullScanIndexCount++;
    }

    private void updateCache(Snapshot latestSnapshot) {
        if (!canUseManifestEntriesCache(latestSnapshot)) {
            fullScanManifestEntries(latestSnapshot);
        }
        if (!canUseIndexManifestEntriesCache(latestSnapshot)) {
            fullScanIndexManifestEntries(latestSnapshot);
        }
        cachedSnapshot = latestSnapshot;
    }

    private boolean canUseManifestEntriesCache(Snapshot latestSnapshot) {
        if (partitionFilter == null) {
            // If overwrite the whole table, any commit between snapshots must touch the target,
            // skip the delta probe and force a full scan
            return false;
        }
        for (long id = cachedSnapshot.id() + 1; id <= latestSnapshot.id(); id++) {
            deltaProbeCount++;
            try {
                Snapshot snapshot = snapshotManager.tryGetSnapshot(id);
                if (snapshot.commitKind() != CommitKind.APPEND) {
                    // Only APPEND snapshots produce a reliable DELTA manifest for probing. For
                    // example, OVERWRITE snapshot might rewrite or reorganize manifests in ways
                    // that make the DELTA probe unreliable.
                    return false;
                }
                FileStoreScan scan =
                        newScan()
                                .withSnapshot(snapshot)
                                .withPartitionFilter(partitionFilter)
                                .withKind(ScanMode.DELTA);
                Iterator<ManifestEntry> iterator = scan.readFileIterator();
                if (iterator.hasNext()) {
                    LOG.info(
                            "Cannot advance cache from {} to {} because snapshot {} has changed target partitions.",
                            cachedSnapshot.id(),
                            latestSnapshot.id(),
                            id);
                    return false;
                }
            } catch (Exception e) {
                // For example, the snapshot is being expired. Using full scan is safe.
                return false;
            }
        }
        return true;
    }

    private boolean canUseIndexManifestEntriesCache(Snapshot latestSnapshot) {
        return Objects.equals(cachedSnapshot.indexManifest(), latestSnapshot.indexManifest());
    }

    private FileStoreScan newScan() {
        FileStoreScan scan = scanSupplier.get();
        if (dropStats) {
            scan.dropStats();
        }
        if (numBucket != BucketMode.POSTPONE_BUCKET) {
            scan.withBucketFilter(bucket -> bucket >= 0);
        }
        return scan;
    }

    private CommitChanges buildResult() {
        List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
        List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();

        for (ManifestEntry entry : cachedManifestEntries) {
            changesWithOverwrite.add(
                    ManifestEntry.create(
                            FileKind.DELETE,
                            entry.partition(),
                            entry.bucket(),
                            entry.totalBuckets(),
                            entry.file()));
        }
        for (IndexManifestEntry entry : cachedIndexManifestEntries) {
            indexChangesWithOverwrite.add(entry.toDeleteEntry());
        }

        changesWithOverwrite.addAll(newChanges);
        indexChangesWithOverwrite.addAll(newIndexFiles);

        return new CommitChanges(
                changesWithOverwrite, Collections.emptyList(), indexChangesWithOverwrite);
    }
}
