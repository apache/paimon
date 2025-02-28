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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;

import static org.apache.paimon.utils.SnapshotManager.EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_TIMESTAMP} startup mode of a
 * streaming read.
 */
public class ContinuousFromTimestampStartingScanner extends AbstractStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(ContinuousFromTimestampStartingScanner.class);

    private final ChangelogManager changelogManager;
    private final long startupMillis;
    private final boolean startFromChangelog;

    public ContinuousFromTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long startupMillis,
            boolean changelogDecoupled) {
        super(snapshotManager);
        this.changelogManager = changelogManager;
        this.startupMillis = startupMillis;
        this.startFromChangelog = changelogDecoupled;
        this.startingSnapshotId =
                earlierThanTimeMills(
                        snapshotManager, changelogManager, startupMillis, startFromChangelog);
    }

    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId + 1, false);
        }
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        Long startingSnapshotId =
                earlierThanTimeMills(
                        snapshotManager, changelogManager, startupMillis, startFromChangelog);
        if (startingSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Waiting for snapshot generation.");
            return new NoSnapshot();
        }
        return new NextSnapshot(startingSnapshotId + 1);
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public static @Nullable Long earlierThanTimeMills(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long timestampMills,
            boolean startFromChangelog) {
        Long latest = snapshotManager.latestSnapshotId();
        if (latest == null) {
            return null;
        }

        Snapshot earliestSnapshot =
                earliestSnapshot(snapshotManager, changelogManager, startFromChangelog, latest);
        if (earliestSnapshot == null) {
            return latest - 1;
        }

        if (earliestSnapshot.timeMillis() >= timestampMills) {
            return earliestSnapshot.id() - 1;
        }

        long earliest = earliestSnapshot.id();
        while (earliest < latest) {
            long mid = (earliest + latest + 1) / 2;
            Snapshot snapshot =
                    startFromChangelog
                            ? changelogOrSnapshot(snapshotManager, changelogManager, mid)
                            : snapshotManager.snapshot(mid);
            if (snapshot.timeMillis() < timestampMills) {
                earliest = mid;
            } else {
                latest = mid - 1;
            }
        }
        return earliest;
    }

    private static @Nullable Snapshot earliestSnapshot(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean includeChangelog,
            @Nullable Long stopSnapshotId) {
        Long snapshotId = null;
        if (includeChangelog) {
            snapshotId = changelogManager.earliestLongLivedChangelogId();
        }
        if (snapshotId == null) {
            snapshotId = snapshotManager.earliestSnapshotId();
        }
        if (snapshotId == null) {
            return null;
        }

        if (stopSnapshotId == null) {
            stopSnapshotId = snapshotId + EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;
        }

        FunctionWithException<Long, Snapshot, FileNotFoundException> snapshotFunction =
                includeChangelog
                        ? s -> tryGetChangelogOrSnapshot(snapshotManager, changelogManager, s)
                        : snapshotManager::tryGetSnapshot;

        do {
            try {
                return snapshotFunction.apply(snapshotId);
            } catch (FileNotFoundException e) {
                snapshotId++;
                if (snapshotId > stopSnapshotId) {
                    return null;
                }
                LOG.warn(
                        "The earliest snapshot or changelog was once identified but disappeared. "
                                + "It might have been expired by other jobs operating on this table. "
                                + "Searching for the second earliest snapshot or changelog instead. ");
            }
        } while (true);
    }

    private static Snapshot tryGetChangelogOrSnapshot(
            SnapshotManager snapshotManager, ChangelogManager changelogManager, long snapshotId)
            throws FileNotFoundException {
        if (changelogManager.longLivedChangelogExists(snapshotId)) {
            return changelogManager.tryGetChangelog(snapshotId);
        } else {
            return snapshotManager.tryGetSnapshot(snapshotId);
        }
    }

    private static Snapshot changelogOrSnapshot(
            SnapshotManager snapshotManager, ChangelogManager changelogManager, long snapshotId) {
        if (changelogManager.longLivedChangelogExists(snapshotId)) {
            return changelogManager.changelog(snapshotId);
        } else {
            return snapshotManager.snapshot(snapshotId);
        }
    }
}
