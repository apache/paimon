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

package org.apache.paimon.flink.expire;

import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.ExpireSnapshotsImpl;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static org.apache.paimon.table.ExpireSnapshotsImpl.findSkippingTags;

/**
 * Planner for snapshot expiration. This class computes the expiration plan including:
 *
 * <ul>
 *   <li>The range of snapshots to expire [beginInclusiveId, endExclusiveId)
 *   <li>Protection set containing manifests that should not be deleted
 *   <li>Four groups of tasks organized by deletion phase
 * </ul>
 *
 * <p>Tag data files are loaded on-demand by workers using {@link
 * SnapshotDeletion#createDataFileSkipperForTags}, which has internal caching to avoid repeated
 * reads.
 */
public class ExpireSnapshotsPlanner {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsPlanner.class);

    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final TagManager tagManager;

    public ExpireSnapshotsPlanner(
            SnapshotManager snapshotManager,
            ConsumerManager consumerManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager) {
        this.snapshotManager = snapshotManager;
        this.consumerManager = consumerManager;
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
    }

    /** Creates an ExpireSnapshotsPlanner from a FileStoreTable. */
    public static ExpireSnapshotsPlanner create(FileStoreTable table) {
        SnapshotManager snapshotManager = table.snapshotManager();
        ConsumerManager consumerManager =
                new ConsumerManager(table.fileIO(), table.location(), snapshotManager.branch());
        return new ExpireSnapshotsPlanner(
                table.snapshotManager(),
                consumerManager,
                table.store().newSnapshotDeletion(),
                table.tagManager());
    }

    /**
     * Plan the snapshot expiration.
     *
     * @param config expiration configuration
     * @return the expiration plan with three groups of tasks, or empty plan if nothing to expire
     */
    public ExpireSnapshotsPlan plan(ExpireConfig config) {
        snapshotDeletion.setChangelogDecoupled(config.isChangelogDecoupled());

        ExpireSnapshotsImpl.ExpireRange range =
                ExpireSnapshotsImpl.computeSnapshotExpireRange(
                        config, snapshotManager, consumerManager);
        if (range.isEmpty()) {
            return ExpireSnapshotsPlan.empty();
        }
        return plan(range.earliestId, range.maxExclusive, config.isChangelogDecoupled());
    }

    private ExpireSnapshotsPlan plan(
            long earliestId, long endExclusiveId, boolean changelogDecoupled) {
        // Boundary check
        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.earliestFileNotExists()) {
                try {
                    snapshotManager.commitEarliestHint(earliestId);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return ExpireSnapshotsPlan.empty();
        }

        // find first snapshot to expire
        Map<Long, Snapshot> snapshots = collectSnapshots(earliestId, endExclusiveId);
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            if (!snapshots.containsKey(id)) {
                // only latest snapshots are retained, as we cannot find this snapshot, we can
                // assume that all snapshots preceding it have been removed
                beginInclusiveId = id + 1;
                break;
            }
        }

        // Get endSnapshot from cache
        Snapshot cachedEndSnapshot = snapshots.get(endExclusiveId);
        if (cachedEndSnapshot == null) {
            // the end exclusive snapshot is gone
            // there is no need to proceed
            LOG.warn("End snapshot {} not found, abort expiration", endExclusiveId);
            return ExpireSnapshotsPlan.empty();
        }

        // Build protection set
        ProtectionSet protectionSet =
                buildProtectionSet(beginInclusiveId, endExclusiveId, cachedEndSnapshot);

        // Generate four groups of tasks
        List<SnapshotExpireTask> dataFileTasks = new ArrayList<>();
        List<SnapshotExpireTask> changelogFileTasks = new ArrayList<>();
        List<SnapshotExpireTask> manifestTasks = new ArrayList<>();
        List<SnapshotExpireTask> snapshotFileTasks = new ArrayList<>();

        // Data file tasks: range is (beginInclusiveId, endExclusiveId]
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            dataFileTasks.add(SnapshotExpireTask.forDataFiles(id));
        }

        // Changelog file tasks: range is [beginInclusiveId, endExclusiveId)
        // Only when changelog is not decoupled
        if (!changelogDecoupled) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                changelogFileTasks.add(SnapshotExpireTask.forChangelogFiles(id));
            }
        }

        // Manifest and snapshot file tasks: range is [beginInclusiveId, endExclusiveId)
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            // Skip cleaning manifest files due to failed to build skipping set
            if (protectionSet.manifestSkippingSet() != null) {
                manifestTasks.add(SnapshotExpireTask.forManifests(id));
            }
            snapshotFileTasks.add(SnapshotExpireTask.forSnapshot(id, changelogDecoupled));
        }

        LOG.info(
                "Planned expiration: range=[{}, {}), dataFileTasks={}, changelogFileTasks={}, manifestTasks={}, snapshotFileTasks={}",
                beginInclusiveId,
                endExclusiveId,
                dataFileTasks.size(),
                changelogFileTasks.size(),
                manifestTasks.size(),
                snapshotFileTasks.size());

        return new ExpireSnapshotsPlan(
                dataFileTasks,
                changelogFileTasks,
                manifestTasks,
                snapshotFileTasks,
                protectionSet,
                beginInclusiveId,
                endExclusiveId,
                snapshots);
    }

    private ProtectionSet buildProtectionSet(
            long beginInclusiveId, long endExclusiveId, Snapshot cachedEndSnapshot) {

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // 1. Find skipping tags that overlap with expire range
        List<Snapshot> skippingTags =
                findSkippingTags(taggedSnapshots, beginInclusiveId, endExclusiveId);

        // 2. Build manifest skipping set
        List<Snapshot> manifestProtectedSnapshots = new ArrayList<>(skippingTags);
        manifestProtectedSnapshots.add(cachedEndSnapshot);
        Set<String> manifestSkippingSet = null;
        try {
            manifestSkippingSet =
                    new HashSet<>(snapshotDeletion.manifestSkippingSet(manifestProtectedSnapshots));
        } catch (Exception e) {
            LOG.warn("Skip cleaning manifest files due to failed to build skipping set.", e);
        }

        return new ProtectionSet(taggedSnapshots, manifestSkippingSet);
    }

    /** Concurrently collect all snapshots in range [earliestId, endExclusiveId]. */
    private Map<Long, Snapshot> collectSnapshots(long earliestId, long endExclusiveId) {
        Executor fileExecutor = snapshotDeletion.fileExecutor();
        Map<Long, Snapshot> snapshots = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (long id = earliestId; id <= endExclusiveId; id++) {
            long snapshotId = id;
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    Snapshot snapshot = snapshotManager.tryGetSnapshot(snapshotId);
                                    snapshots.put(snapshotId, snapshot);
                                } catch (FileNotFoundException ignored) {
                                }
                            },
                            fileExecutor));
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return snapshots;
    }
}
