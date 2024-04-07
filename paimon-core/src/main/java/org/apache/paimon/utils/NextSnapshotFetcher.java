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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.OutOfRangeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/** Fetcher for getting the next snapshot by snapshot id. */
public class NextSnapshotFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(NextSnapshotFetcher.class);
    private final SnapshotManager snapshotManager;
    private final boolean changelogDecoupled;
    // Only support changelog as follow-up now.
    private final boolean changelogAsFollowup;

    public NextSnapshotFetcher(
            SnapshotManager snapshotManager,
            boolean changelogDecoupled,
            boolean changelogAsFollowup) {
        this.snapshotManager = snapshotManager;
        this.changelogDecoupled = changelogDecoupled;
        this.changelogAsFollowup = changelogAsFollowup;
    }

    @Nullable
    public Snapshot getNextSnapshot(long nextSnapshotId) {
        if (snapshotManager.snapshotExists(nextSnapshotId)) {
            return snapshotManager.snapshot(nextSnapshotId);
        }

        Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
        // No snapshot now
        if (earliestSnapshotId == null || earliestSnapshotId <= nextSnapshotId) {
            LOG.debug(
                    "Next snapshot id {} does not exist, wait for the snapshot generation.",
                    nextSnapshotId);
            return null;
        }

        if (!changelogAsFollowup
                || !changelogDecoupled
                || !snapshotManager.longLivedChangelogExists(nextSnapshotId)) {
            throw new OutOfRangeException(
                    String.format(
                            "The snapshot with id %d has expired. You can: "
                                    + "1. increase the snapshot or changelog expiration time. "
                                    + "2. use consumer-id to ensure that unconsumed snapshots will not be expired.",
                            nextSnapshotId));
        }
        return snapshotManager.changelog(nextSnapshotId);
    }
}
