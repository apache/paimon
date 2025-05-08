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
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_CREATION_TIMESTAMP} startup
 * mode.
 */
public class CreationTimestampStartingScanner extends ReadPlanStartingScanner {

    private static final Logger LOG =
            LoggerFactory.getLogger(CreationTimestampStartingScanner.class);

    private final ChangelogManager changelogManager;
    private final long startupMillis;
    private final boolean startFromChangelog;

    public CreationTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long startupMillis,
            boolean changelogDecoupled) {
        super(snapshotManager);
        this.changelogManager = changelogManager;
        this.startupMillis = startupMillis;
        this.startFromChangelog = changelogDecoupled;
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        Long startingSnapshotPrevId =
                TimeTravelUtil.earlierThanTimeMills(
                        snapshotManager, changelogManager, startupMillis, startFromChangelog, true);
        if (startingSnapshotPrevId == null) {
            LOG.debug("There is no previous snapshot, so use earliest snapshot.");
            startingSnapshotId = snapshotManager.earliestSnapshotId();
        } else if (snapshotManager.snapshotExists(startingSnapshotPrevId + 1)
                || changelogManager.longLivedChangelogExists(startingSnapshotPrevId + 1)) {
            startingSnapshotId = startingSnapshotPrevId + 1;
        } else {
            LOG.debug("There is no next snapshot, so use latest snapshot.");
            startingSnapshotId = snapshotManager.latestSnapshotId();
        }
        if (startingSnapshotId == null) {
            LOG.debug("There is currently no snapshot. Waiting for snapshot generation.");
            return null;
        }
        return snapshotReader
                .withMode(ScanMode.ALL)
                .withSnapshot(startingSnapshotId)
                .withManifestEntryFilter(
                        entry -> entry.file().creationTimeEpochMillis() >= startupMillis);
    }
}
