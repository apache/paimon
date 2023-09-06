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

import org.apache.paimon.CoreOptions.StartupMode;
import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

/**
 * {@link StartingScanner} for the {@link StartupMode#COMPACTED_FULL} startup mode with
 * 'full-compaction.delta-commits'.
 */
public class FullCompactedStartingScanner extends CompactedStartingScanner {

    private final int deltaCommits;

    public FullCompactedStartingScanner(SnapshotManager snapshotManager, int deltaCommits) {
        super(snapshotManager);
        this.deltaCommits = deltaCommits;
    }

    @Override
    @Nullable
    protected Long pick() {
        return snapshotManager.pickOrLatest(this::picked);
    }

    private boolean picked(Snapshot snapshot) {
        long identifier = snapshot.commitIdentifier();
        return snapshot.commitKind() == CommitKind.COMPACT
                && isFullCompactedIdentifier(identifier, deltaCommits);
    }

    public static boolean isFullCompactedIdentifier(long identifier, int deltaCommits) {
        return identifier % deltaCommits == 0 || identifier == Long.MAX_VALUE;
    }
}
