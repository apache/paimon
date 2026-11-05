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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Checks primary-key compaction against the restored snapshot of each partition's writers. */
public class PartitionCompactionChecker {

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final CommitScanner scanner;

    // Successfully checked intervals (from, to], independently of the writers' immutable bases.
    private final Map<BinaryRow, Range> checkedSnapshots = new HashMap<>();

    public PartitionCompactionChecker(
            SnapshotManager snapshotManager, String commitUser, CommitScanner scanner) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scanner = scanner;
    }

    /** Checks each partition from its restored snapshot to the latest published snapshot. */
    public void check(long newSnapshotId, Map<BinaryRow, Long> baseSnapshots) {
        if (baseSnapshots.isEmpty()) {
            return;
        }
        Map<BinaryRow, Range> ranges = new HashMap<>();
        for (Map.Entry<BinaryRow, Long> entry : baseSnapshots.entrySet()) {
            long base = entry.getValue();
            Range checked = checkedSnapshots.get(entry.getKey());
            // A newer writer may have skipped an earlier conflicting COMPACT. Its checked
            // interval cannot be reused for a subsequently received older writer state.
            ranges.put(
                    entry.getKey(),
                    checked != null && checked.from <= base && base <= checked.to
                            ? checked
                            : new Range(base, base));
        }
        long firstSnapshot =
                ranges.values().stream().mapToLong(range -> range.to).min().getAsLong() + 1;
        for (long id = firstSnapshot; id < newSnapshotId; id++) {
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)
                    || (snapshot.commitKind() != CommitKind.COMPACT
                            && snapshot.commitKind() != CommitKind.OVERWRITE)) {
                continue;
            }
            Set<BinaryRow> partitions = new HashSet<>();
            for (Map.Entry<BinaryRow, Range> entry : ranges.entrySet()) {
                if (entry.getValue().to < id) {
                    partitions.add(entry.getKey());
                }
            }
            if (scanner.hasDataChanges(snapshot, partitions, false)
                    || scanner.hasIndexChanges(snapshot, partitions, true)) {
                throw new RuntimeException(
                        String.format(
                                "Concurrent compaction conflict when committing snapshot %d: "
                                        + "%s snapshot %d by user %s modified the same partition "
                                        + "after this writer restored its state. Reload the partition "
                                        + "and recompute compaction.",
                                newSnapshotId, snapshot.commitKind(), id, snapshot.commitUser()));
            }
        }
        // Only cache a fully verified interval. Publishing may still fail afterwards, but a
        // retry must inspect all snapshots committed after this verified endpoint.
        ranges.forEach(
                (partition, range) ->
                        checkedSnapshots.put(
                                partition.copy(), new Range(range.from, newSnapshotId - 1)));
    }
}
