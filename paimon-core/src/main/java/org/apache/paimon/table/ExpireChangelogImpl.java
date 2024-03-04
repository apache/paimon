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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/** Cleanup the changelog in changelog directory. */
public class ExpireChangelogImpl implements ExpireSnapshots {

    public static final Logger LOG = LoggerFactory.getLogger(ExpireChangelogImpl.class);

    private final SnapshotManager snapshotManager;
    private final SnapshotDeletion snapshotDeletion;
    private final boolean cleanEmptyDirectories;
    /** Whether to keep the last snapshot. */
    private final boolean keepLastOne;

    private long olderThanMills = 0;
    private int maxDeletes = Integer.MAX_VALUE;

    public ExpireChangelogImpl(
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            boolean cleanEmptyDirectories,
            boolean keepLastOne) {
        this.snapshotManager = snapshotManager;
        this.snapshotDeletion = snapshotDeletion;
        this.cleanEmptyDirectories = cleanEmptyDirectories;
        this.keepLastOne = keepLastOne;
    }

    @Override
    public ExpireChangelogImpl retainMax(int retainMax) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExpireChangelogImpl retainMin(int retainMin) {
        throw new UnsupportedOperationException();
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
        Long latest = snapshotManager.latestLongLivedChangelogId();
        if (latest == null) {
            return 0;
        }
        Long earliest = snapshotManager.earliestLongLivedChangelogId();
        if (earliest == null) {
            return 0;
        }
        long maxId = Math.min(maxDeletes + earliest - 1, latest);
        for (long id = earliest; id <= maxId; id++) {
            if (snapshotManager.longLivedChangelogExists(id)
                    && olderThanMills <= snapshotManager.longLivedChangelog(id).timeMillis()) {
                return expireUntil(earliest, keepLastOne ? id : id + 1);
            }
        }
        return expireUntil(earliest, keepLastOne ? maxId : maxId + 1);
    }

    public int expireUntil(long earliestId, long endExclusiveId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Changelog expire range is [" + earliestId + ", " + endExclusiveId + ")");
        }

        for (long id = earliestId; id < endExclusiveId; id++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #" + id);
            }
            Snapshot snapshot = snapshotManager.longLivedChangelog(id);
            // delete changelog files
            if (snapshot.changelogManifestList() != null) {
                snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
            }
            snapshotDeletion.cleanUnusedChangelogManifests(snapshot);
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

    @VisibleForTesting
    public long getOlderThanMills() {
        return olderThanMills;
    }
}
