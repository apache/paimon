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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

/**
 * Default implementation of {@link FileStoreExpire}. It retains a certain number or period of
 * latest snapshots.
 *
 * <p>NOTE: This implementation will keep at least one snapshot so that users will not accidentally
 * clear all snapshots.
 *
 * <p>TODO: add concurrent tests.
 */
public class FileStoreExpireImpl implements FileStoreExpire {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreExpireImpl.class);

    private final int numRetainedMin;
    // snapshots exceeding any constraint will be expired
    private final int numRetainedMax;
    private final long millisRetained;

    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;

    private final TagManager tagManager;
    private final int expireLimit;
    private final boolean snapshotExpireCleanEmptyDirectories;

    private Lock lock;

    public FileStoreExpireImpl(
            int numRetainedMin,
            int numRetainedMax,
            long millisRetained,
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            TagManager tagManager,
            int expireLimit,
            boolean snapshotExpireCleanEmptyDirectories) {
        Preconditions.checkArgument(
                numRetainedMin >= 1,
                "The minimum number of completed snapshots to retain should be >= 1.");
        Preconditions.checkArgument(
                numRetainedMax >= numRetainedMin,
                "The maximum number of snapshots to retain should be >= the minimum number.");
        Preconditions.checkArgument(
                expireLimit > 1,
                String.format("The %s should be > 1.", CoreOptions.SNAPSHOT_EXPIRE_LIMIT.key()));
        this.numRetainedMin = numRetainedMin;
        this.numRetainedMax = numRetainedMax;
        this.millisRetained = millisRetained;
        this.snapshotManager = snapshotManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.snapshotDeletion = snapshotDeletion;
        this.tagManager = tagManager;
        this.expireLimit = expireLimit;
        this.snapshotExpireCleanEmptyDirectories = snapshotExpireCleanEmptyDirectories;
    }

    @Override
    public FileStoreExpire withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public void expire() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            // no snapshot, nothing to expire
            return;
        }

        long currentMillis = System.currentTimeMillis();

        Long earliest = snapshotManager.earliestSnapshotId();
        if (earliest == null) {
            return;
        }

        // locate the first snapshot between the numRetainedMax th and (numRetainedMin+1) th latest
        // snapshots to be retained. This snapshot needs to be preserved because it
        // doesn't fulfill the time threshold condition for expiration.
        for (long id = Math.max(latestSnapshotId - numRetainedMax + 1, earliest);
                id <= latestSnapshotId - numRetainedMin;
                id++) {
            if (snapshotManager.snapshotExists(id)
                    && currentMillis - snapshotManager.snapshot(id).timeMillis()
                            <= millisRetained) {
                // within time threshold, can assume that all snapshots after it are also within
                // the threshold
                expireUntil(earliest, id);
                return;
            }
        }

        // by default, expire until there are only numRetainedMin snapshots left
        expireUntil(earliest, latestSnapshotId - numRetainedMin + 1);
    }

    @VisibleForTesting
    public void expireUntil(long earliestId, long endExclusiveId) {
        OptionalLong minNextSnapshot = consumerManager.minNextSnapshot();
        if (minNextSnapshot.isPresent()) {
            endExclusiveId = Math.min(minNextSnapshot.getAsLong(), endExclusiveId);
        }

        if (endExclusiveId <= earliestId) {
            // No expire happens:
            // write the hint file in order to see the earliest snapshot directly next time
            // should avoid duplicate writes when the file exists
            if (snapshotManager.readHint(SnapshotManager.EARLIEST) == null) {
                writeEarliestHint(endExclusiveId);
            }

            // fast exit
            return;
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

        endExclusiveId = Math.min(beginInclusiveId + expireLimit, endExclusiveId);

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
        for (long id = beginInclusiveId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.changelogManifestList() != null) {
                snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
            }
        }

        // data files and changelog files in bucket directories has been deleted
        // then delete changed bucket directories if they are empty
        if (snapshotExpireCleanEmptyDirectories) {
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
            snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet);

            // delete snapshot last
            snapshotManager.fileIO().deleteQuietly(snapshotManager.snapshotPath(id));
        }

        writeEarliestHint(endExclusiveId);
    }

    private void writeEarliestHint(long earliest) {
        // update earliest hint file

        Callable<Void> callable =
                () -> {
                    snapshotManager.commitEarliestHint(earliest);
                    return null;
                };

        try {
            if (lock != null) {
                lock.runWithLock(callable);
            } else {
                callable.call();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    SnapshotDeletion snapshotDeletion() {
        return snapshotDeletion;
    }
}
