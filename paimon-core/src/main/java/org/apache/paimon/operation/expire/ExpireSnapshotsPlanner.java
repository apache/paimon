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

package org.apache.paimon.operation.expire;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
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
import java.util.Set;

import static org.apache.paimon.utils.SnapshotManager.findPreviousOrEqualSnapshot;
import static org.apache.paimon.utils.SnapshotManager.findPreviousSnapshot;

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
        int retainMax = config.getSnapshotRetainMax();
        int retainMin = config.getSnapshotRetainMin();
        int maxDeletes = config.getSnapshotMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - config.getSnapshotTimeRetain().toMillis();

        // 1. Get snapshot range
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return ExpireSnapshotsPlan.empty();
        }

        Long earliestId = snapshotManager.earliestSnapshotId();
        if (earliestId == null) {
            return ExpireSnapshotsPlan.empty();
        }

        Preconditions.checkArgument(
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

        // the min snapshot to retain from 'snapshot.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliestId);

        // the max exclusive snapshot to expire until
        // protected by 'snapshot.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long endExclusiveId = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        long consumerProtection = consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE);
        endExclusiveId = Math.min(endExclusiveId, consumerProtection);

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        endExclusiveId = Math.min(endExclusiveId, earliestId + maxDeletes);

        for (long id = min; id < endExclusiveId; id++) {
            // Early exit the loop for 'snapshot.time-retained'
            // (the maximum time of snapshots to retain)
            if (snapshotManager.snapshotExists(id)
                    && olderThanMills <= snapshotManager.snapshot(id).timeMillis()) {
                endExclusiveId = id;
                break;
            }
        }

        return plan(earliestId, endExclusiveId, config.isChangelogDecoupled());
    }

    @VisibleForTesting
    public ExpireSnapshotsPlan plan(
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
        long beginInclusiveId = earliestId;
        for (long id = endExclusiveId - 1; id >= earliestId; id--) {
            if (!snapshotManager.snapshotExists(id)) {
                // only latest snapshots are retained, as we cannot find this snapshot, we can
                // assume that all snapshots preceding it have been removed
                beginInclusiveId = id + 1;
                break;
            }
        }

        // Pre-read and cache endSnapshot
        Snapshot cachedEndSnapshot;
        try {
            cachedEndSnapshot = snapshotManager.tryGetSnapshot(endExclusiveId);
        } catch (FileNotFoundException e) {
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
                endExclusiveId);
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

    /** Find the skipping tags in sortedTags for range of [beginInclusive, endExclusive). */
    public static List<Snapshot> findSkippingTags(
            List<Snapshot> sortedTags, long beginInclusive, long endExclusive) {
        List<Snapshot> overlappedSnapshots = new ArrayList<>();
        int right = findPreviousSnapshot(sortedTags, endExclusive);
        if (right >= 0) {
            int left = Math.max(findPreviousOrEqualSnapshot(sortedTags, beginInclusive), 0);
            for (int i = left; i <= right; i++) {
                overlappedSnapshots.add(sortedTags.get(i));
            }
        }
        return overlappedSnapshots;
    }
}
