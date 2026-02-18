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
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.ChangelogManager;
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
import java.util.function.Predicate;

import static org.apache.paimon.utils.SnapshotManager.findPreviousOrEqualSnapshot;
import static org.apache.paimon.utils.SnapshotManager.findPreviousSnapshot;

/** An implementation for {@link ExpireSnapshots}. */
public class ExpireSnapshotsImpl implements ExpireSnapshots {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsImpl.class);

    private final SnapshotManager snapshotManager;
    private final ChangelogManager changelogManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;
    private final boolean detectExpirationSettingEnabled;

    private ExpireConfig expireConfig;
    private long latestSchemaId;

    public ExpireSnapshotsImpl(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager,
            SchemaManager schemaManager,
            boolean detectExpirationSettingEnabled) {
        this.snapshotManager = snapshotManager;
        this.changelogManager = changelogManager;
        this.consumerManager =
                new ConsumerManager(
                        snapshotManager.fileIO(),
                        snapshotManager.tablePath(),
                        snapshotManager.branch());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.expireConfig = ExpireConfig.builder().build();
        this.schemaManager = schemaManager;
        this.detectExpirationSettingEnabled = detectExpirationSettingEnabled;
        if (this.detectExpirationSettingEnabled) {
            this.latestSchemaId = this.schemaManager.latest().get().id();
        }
    }

    @Override
    public ExpireSnapshots config(ExpireConfig expireConfig) {
        this.expireConfig = expireConfig;
        return this;
    }

    @Override
    public int expire() {
        if (this.detectExpirationSettingEnabled) {
            TableSchema latestTableSchema = this.schemaManager.latest().get();
            if (this.latestSchemaId != latestTableSchema.id()) {
                this.expireConfig = CoreOptions.fromMap(latestTableSchema.options()).expireConfig();
                this.latestSchemaId = latestTableSchema.id();
            }
        }

        snapshotDeletion.setChangelogDecoupled(expireConfig.isChangelogDecoupled());
        int retainMax = expireConfig.getSnapshotRetainMax();
        int retainMin = expireConfig.getSnapshotRetainMin();
        int maxDeletes = expireConfig.getSnapshotMaxDeletes();
        long olderThanMills =
                System.currentTimeMillis() - expireConfig.getSnapshotTimeRetain().toMillis();

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
                retainMax >= retainMin,
                String.format(
                        "retainMax (%s) must not be less than retainMin (%s).",
                        retainMax, retainMin));

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
        long startTime = System.currentTimeMillis();

        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.earliestFileNotExists()) {
                writeEarliestHint(earliestId);
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

        List<Snapshot> taggedSnapshots = tagManager.taggedSnapshots();

        // delete merge tree files
        // deleted merge tree files in a snapshot are not used by the next snapshot, so the range of
        // id should be (beginInclusiveId, endExclusiveId]
        for (long id = beginInclusiveId + 1; id <= endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete merge tree files not used by snapshot #" + id);
            }
            Snapshot snapshot;
            try {
                snapshot = snapshotManager.tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                beginInclusiveId = id + 1;
                continue;
            }
            // expire merge tree files and collect changed buckets
            Predicate<ExpireFileEntry> skipper;
            try {
                skipper = snapshotDeletion.createDataFileSkipperForTags(taggedSnapshots, id);
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
        if (!expireConfig.isChangelogDecoupled()) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete changelog files from snapshot #" + id);
                }
                Snapshot snapshot;
                try {
                    snapshot = snapshotManager.tryGetSnapshot(id);
                } catch (FileNotFoundException e) {
                    beginInclusiveId = id + 1;
                    continue;
                }
                if (snapshot.changelogManifestList() != null) {
                    snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
                }
            }
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        snapshotDeletion.cleanEmptyDirectories();

        // delete manifests and indexFiles
        List<Snapshot> skippingSnapshots =
                findSkippingTags(taggedSnapshots, beginInclusiveId, endExclusiveId);

        try {
            skippingSnapshots.add(snapshotManager.tryGetSnapshot(endExclusiveId));
        } catch (FileNotFoundException e) {
            // the end exclusive snapshot is gone
            // there is no need to proceed
            return 0;
        }

        Set<String> skippingSet = null;
        try {
            skippingSet = new HashSet<>(snapshotDeletion.manifestSkippingSet(skippingSnapshots));
        } catch (Exception e) {
            LOG.info("Skip cleaning manifest files due to failed to build skipping set.", e);
        }
        if (skippingSet != null) {
            for (long id = beginInclusiveId; id < endExclusiveId; id++) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ready to delete manifests in snapshot #" + id);
                }

                Snapshot snapshot;
                try {
                    snapshot = snapshotManager.tryGetSnapshot(id);
                } catch (FileNotFoundException e) {
                    beginInclusiveId = id + 1;
                    continue;
                }
                snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet);
            }
        }

        // delete snapshot file finally
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            Snapshot snapshot;
            try {
                snapshot = snapshotManager.tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                beginInclusiveId = id + 1;
                continue;
            }
            if (expireConfig.isChangelogDecoupled()) {
                commitChangelog(new Changelog(snapshot));
            }
            snapshotManager.deleteSnapshot(id);
        }

        writeEarliestHint(endExclusiveId);
        long duration = System.currentTimeMillis() - startTime;
        LOG.info(
                "Finished expire snapshots, duration {} ms, range is [{}, {})",
                duration,
                beginInclusiveId,
                endExclusiveId);
        return (int) (endExclusiveId - beginInclusiveId);
    }

    private void commitChangelog(Changelog changelog) {
        try {
            changelogManager.commitChangelog(changelog, changelog.id());
            changelogManager.commitLongLivedChangelogLatestHint(changelog.id());
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
