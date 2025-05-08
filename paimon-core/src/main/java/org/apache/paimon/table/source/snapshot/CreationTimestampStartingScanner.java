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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import java.util.List;

/**
 * {@link StartingScanner} for the {@link CoreOptions.StartupMode#FROM_CREATION_TIMESTAMP} startup
 * mode.
 */
public class CreationTimestampStartingScanner extends AbstractStartingScanner {

    private final AbstractStartingScanner scanner;

    public CreationTimestampStartingScanner(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long creationMillis,
            boolean changelogDecoupled,
            boolean isStreaming) {
        super(snapshotManager);
        Long startingSnapshotPrevId =
                TimeTravelUtil.earlierThanTimeMills(
                        snapshotManager,
                        changelogManager,
                        creationMillis,
                        changelogDecoupled,
                        true);
        if (startingSnapshotPrevId != null
                && (snapshotManager.snapshotExists(startingSnapshotPrevId + 1)
                        || changelogManager.longLivedChangelogExists(startingSnapshotPrevId + 1))) {
            startingSnapshotId = startingSnapshotPrevId + 1;
        }
        if (startingSnapshotId != null) {
            scanner =
                    isStreaming
                            ? new ContinuousFromTimestampStartingScanner(
                                    snapshotManager,
                                    changelogManager,
                                    creationMillis,
                                    changelogDecoupled)
                            : new StaticFromTimestampStartingScanner(
                                    snapshotManager, creationMillis);
        } else {
            scanner = new FileCreationTimeStartingScanner(snapshotManager, creationMillis);
        }
    }

    public AbstractStartingScanner scanner() {
        return scanner;
    }

    @Override
    public ScanMode startingScanMode() {
        return scanner.startingScanMode();
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        return scanner.scan(snapshotReader);
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        return scanner.scanPartitions(snapshotReader);
    }
}
