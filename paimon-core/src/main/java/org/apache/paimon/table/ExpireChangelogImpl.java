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
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;

/** Cleanup the changelog in changelog directory. */
public class ExpireChangelogImpl implements ExpireSnapshots {

    public static final Logger LOG = LoggerFactory.getLogger(ExpireChangelogImpl.class);

    private final SnapshotManager snapshotManager;
    private final ConsumerManager consumerManager;
    private final SnapshotDeletion snapshotDeletion;
    private final boolean cleanEmptyDirectories;
    private final TagManager tagManager;

    private long olderThanMills = 0;
    public int retainMin = 1;
    private int retainMax = Integer.MAX_VALUE;
    private int maxDeletes = Integer.MAX_VALUE;

    public ExpireChangelogImpl(
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SnapshotDeletion snapshotDeletion,
            boolean cleanEmptyDirectories) {
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.consumerManager =
                new ConsumerManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        this.snapshotDeletion = snapshotDeletion;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
    }

    @Override
    public ExpireChangelogImpl retainMax(int retainMax) {
        this.retainMax = retainMax;
        return this;
    }

    @Override
    public ExpireChangelogImpl retainMin(int retainMin) {
        this.retainMin = retainMin;
        return this;
    }

    @Override
    public ExpireChangelogImpl olderThanMills(long olderThanMills) {
        this.olderThanMills = olderThanMills;
        return this;
    }

    @Override
    public ExpireChangelogImpl maxDeletes(int maxDeletes) {
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

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        if (earliestSnapshotId == null) {
            return 0;
        }

        Long latestChangelogId = snapshotManager.latestLongLivedChangelogId();
        if (latestChangelogId == null) {
            return 0;
        }
        Long earliestChangelogId = snapshotManager.earliestLongLivedChangelogId();
        if (earliestChangelogId == null) {
            return 0;
        }

        Preconditions.checkArgument(retainMax > retainMin, "retainMax must greater than retainMin");

        // the min snapshot to retain from 'changelog.num-retained.max'
        // (the maximum number of snapshots to retain)
        long min = Math.max(latestSnapshotId - retainMax + 1, earliestChangelogId);

        // the max exclusive snapshot to expire until
        // protected by 'changelog.num-retained.min'
        // (the minimum number of completed snapshots to retain)
        long maxExclusive = latestSnapshotId - retainMin + 1;

        // the snapshot being read by the consumer cannot be deleted
        maxExclusive =
                Math.min(maxExclusive, consumerManager.minNextSnapshot().orElse(Long.MAX_VALUE));

        // protected by 'snapshot.expire.limit'
        // (the maximum number of snapshots allowed to expire at a time)
        maxExclusive = Math.min(maxExclusive, earliestChangelogId + maxDeletes);

        // Only clean the snapshot in changelog dir
        maxExclusive = Math.min(maxExclusive, latestChangelogId);

        for (long id = min; id <= maxExclusive; id++) {
            if (snapshotManager.longLivedChangelogExists(id)
                    && olderThanMills <= snapshotManager.longLivedChangelog(id).timeMillis()) {
                return expireUntil(earliestChangelogId, id);
            }
        }
        return expireUntil(earliestChangelogId, maxExclusive);
    }

    public int expireUntil(long earliestId, long endExclusiveId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Changelog expire range is [" + earliestId + ", " + endExclusiveId + ")");
        }

        for (long id = earliestId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #" + id);
            }
            Changelog changelog = snapshotManager.longLivedChangelog(id);
            // delete changelog files
            if (changelog.changelogManifestList() != null) {
                snapshotDeletion.deleteAddedDataFiles(changelog.changelogManifestList());
                snapshotDeletion.cleanUnusedManifestList(
                        changelog.changelogManifestList(), new HashSet<>());
            }

            snapshotManager.fileIO().deleteQuietly(snapshotManager.longLivedChangelogPath(id));
        }

        if (cleanEmptyDirectories) {
            snapshotDeletion.cleanDataDirectories();
        }
        writeEarliestHintFile(endExclusiveId);
        return (int) (endExclusiveId - earliestId);
    }

    private void writeEarliestHintFile(long earliest) {
        try {
            snapshotManager.commitLongLivedChangelogEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
