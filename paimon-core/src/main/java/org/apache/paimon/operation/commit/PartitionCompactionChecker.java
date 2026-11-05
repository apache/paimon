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
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.Preconditions.checkArgument;

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

    /** Captures the baselines before APPEND and checks them on every COMPACT attempt. */
    public UnaryOperator<CommitChangesProvider> prepareCheck(List<CommitMessage> messages) {
        Map<BinaryRow, Long> baseSnapshots = new HashMap<>();
        for (CommitMessage message : messages) {
            if (!changesDataOrDeletionVectors(((CommitMessageImpl) message).compactIncrement())) {
                continue;
            }
            Long snapshotId = message.checkFromSnapshot();
            checkArgument(
                    snapshotId != null && snapshotId >= 0,
                    "A primary-key deletion-vector compaction message is missing its "
                            + "restored snapshot. Reload the partition and recompute compaction.");
            baseSnapshots.merge(message.partition(), snapshotId, Math::min);
        }
        if (baseSnapshots.isEmpty()) {
            return UnaryOperator.identity();
        }
        return provider ->
                latestSnapshot -> {
                    long newSnapshotId =
                            latestSnapshot == null
                                    ? Snapshot.FIRST_SNAPSHOT_ID
                                    : latestSnapshot.id() + 1;
                    check(newSnapshotId, baseSnapshots);
                    return provider.provide(latestSnapshot);
                };
    }

    private static boolean changesDataOrDeletionVectors(CompactIncrement compact) {
        if (Stream.concat(compact.newIndexFiles().stream(), compact.deletedIndexFiles().stream())
                .anyMatch(file -> DELETION_VECTORS_INDEX.equals(file.indexType()))) {
            return true;
        }
        List<DataFileMeta> before = compact.compactBefore();
        List<DataFileMeta> after = compact.compactAfter();
        if (before.isEmpty() && after.isEmpty()) {
            return false;
        }
        // File index rewrites replace only index metadata on a single file. Level upgrades
        // keep the same file name too, but change the data layout and still need validation.
        return before.size() != 1
                || after.size() != 1
                || !before.get(0).fileName().equals(after.get(0).fileName())
                || !withoutFileIndex(before.get(0)).equals(withoutFileIndex(after.get(0)));
    }

    private static DataFileMeta withoutFileIndex(DataFileMeta file) {
        List<String> otherExtraFiles =
                file.extraFiles().stream()
                        .filter(name -> !name.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                        .collect(Collectors.toList());
        return file.copy(otherExtraFiles).copy((byte[]) null);
    }

    private void check(long newSnapshotId, Map<BinaryRow, Long> baseSnapshots) {
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
