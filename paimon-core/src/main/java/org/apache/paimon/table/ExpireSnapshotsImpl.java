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

package org.apache.paimon.table;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** An implementation for {@link ExpireSnapshots}. */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final TagManager tagManager;
    private final boolean cleanEmptyDirectories;
    /** Whether to clean the changelog or delta files. */
    private final boolean changelogDecoupled;

    private int retainMax = Integer.MAX_VALUE;
    private int retainMin = 1;
    private long olderThanMills = 0;
    private int maxDeletes = Integer.MAX_VALUE;

    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager,
            boolean cleanEmptyDirectories,
            boolean changelogDecoupled) {
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.changelogDecoupled = changelogDecoupled;
    }

    @Override
    public ExpireSnapshotsImpl retainMax(int retainMax) {
        this.retainMax = retainMax;
        return this;
    }

    @Override
    public ExpireSnapshotsImpl retainMin(int retainMin) {
        this.retainMin = retainMin;
        return this;
    }

    @Override
    public ExpireSnapshotsImpl olderThanMills(long olderThanMills) {
        this.olderThanMills = olderThanMills;
        return this;
    }

    @Override
    public ExpireSnapshotsImpl maxDeletes(int maxDeletes) {
        this.maxDeletes = maxDeletes;
        return this;
    }

    @Override
    public int expire() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return 0;
        }

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return 0;
        }

        Preconditions.checkArgument(
                retainMax > retainMin, "retainMax must greater than retainMin.");

        // the min snapshot to retain from 'snapshot.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliest);

        // the max exclusive snapshot to expire until
        // protected by 'snapshot.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        maxExclusive = Math.min(maxExclusive, earliest + maxDeletes);

        for (long id = min; id < maxExclusive; id++) {
            // Early exit the loop for 'snapshot.time-retained'
            // (the maximum time of snapshots to retain)
            if (snapshotManager.snapshotExists(id)
                    && olderThanMills <= snapshotManager.snapshot(id).timeMillis()) {
                return expireUntil(earliest, id);
            }
        }

        return expireUntil(earliest, maxExclusive);
    }

    @VisibleForTesting
    public int expireUntil(long earliestId, long endExclusiveId) {
        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.readHint(SnapshotManager.EARLIEST) == null) {
                writeEarliestHint(endExclusiveId);
            }

            // fast exit
            return 0;
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

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Snapshot expire range is [" + beginInclusiveId + ", " + endExclusiveId + ")");
        }

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            // expire merge tree files and collect changed buckets
            Predicate<ManifestEntry> skipper;
            try {
                skipper = snapshotDeletion.dataFileSkipper(taggedSnapshots, id);
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Skip cleaning data files of snapshot '%s' due to failed to build skipping set.",
                                id),
                        e);
                continue;
            }

            snapshotDeletion.cleanUnusedDataFiles(snapshot, skipper);
        }

        // delete changelog files
        if (!changelogDecoupled) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete changelog files from snapshot #" + id);
                }
                Snapshot snapshot = snapshotManager.snapshot(id);
                if (snapshot.changelogManifestList() != null) {
                    snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
                }
            }
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        if (cleanEmptyDirectories) {
            snapshotDeletion.cleanDataDirectories();
        }

        // delete manifests and indexFiles
        List<Snapshot> skippingSnapshots =
                TagManager.findOverlappedSnapshots(
                        taggedSnapshots, beginInclusiveId, endExclusiveId);
        skippingSnapshots.add(snapshotManager.snapshot(endExclusiveId));
        Set<String> skippingSet = snapshotDeletion.manifestSkippingSet(skippingSnapshots);
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete manifests in snapshot #" + id);
            }

            Snapshot snapshot = snapshotManager.snapshot(id);
            snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet, !changelogDecoupled);
            if (changelogDecoupled) {
                commitChangelog(new Changelog(snapshot));
            }
            snapshotManager.fileIO().deleteQuietly(snapshotManager.snapshotPath(id));
        }

        writeEarliestHint(endExclusiveId);
        return (int) (endExclusiveId - beginInclusiveId);
    }

    private void commitChangelog(Changelog changelog) {
        try {
            snapshotManager.commitChangelog(changelog, changelog.id());
            snapshotManager.commitLongLivedChangelogLatestHint(changelog.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeEarliestHint(long earliest) {
        try {
            snapshotManager.commitEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }
}
