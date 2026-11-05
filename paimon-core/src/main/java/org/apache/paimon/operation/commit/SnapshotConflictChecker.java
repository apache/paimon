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

import org.apache.paimon.CoreOptions;
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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.operation.commit.ManifestEntryChanges.changedPartitions;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Checks concurrent changes in snapshot history before committing. */
public class SnapshotConflictChecker {

    private final SnapshotManager snapshotManager;
    private final String commitUser;
    private final CommitScanner scanner;
    private final boolean dataEvolutionEnabled;
    private final boolean partitionCompactionCheckEnabled;

    @Nullable private Long lastSafeSnapshot;
    // Successfully checked intervals (from, to], independently of the writers' immutable bases.
    private final Map<BinaryRow, Range> checkedCompactionSnapshots = new HashMap<>();

    public SnapshotConflictChecker(
            SnapshotManager snapshotManager,
            String commitUser,
            CommitScanner scanner,
            boolean dataEvolutionEnabled,
            @Nullable Long lastSafeSnapshot,
            boolean partitionCompactionCheckEnabled) {
        this.snapshotManager = snapshotManager;
        this.commitUser = commitUser;
        this.scanner = scanner;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
        this.lastSafeSnapshot = lastSafeSnapshot;
        this.partitionCompactionCheckEnabled = partitionCompactionCheckEnabled;
    }

    /** Collects the writer baselines before publishing any part of the commit. */
    public Map<BinaryRow, Long> prepareCompactionBaselines(List<CommitMessage> messages) {
        if (!partitionCompactionCheckEnabled) {
            return Collections.emptyMap();
        }
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
        return baseSnapshots;
    }

    /** Checks snapshot history and returns the changed partitions if collected for strict mode. */
    @Nullable
    public List<BinaryRow> check(
            long newSnapshotId,
            CommitKind newCommitKind,
            CommitChanges changes,
            Set<Long> rebasedReassignments,
            Map<BinaryRow, Long> compactionBaselines) {
        if (newCommitKind == CommitKind.COMPACT && !compactionBaselines.isEmpty()) {
            checkPartitionCompaction(newSnapshotId, compactionBaselines);
        }
        if (lastSafeSnapshot == null) {
            return null;
        }
        List<BinaryRow> partitions = changedPartitions(changes.tableFiles, changes.indexFiles);
        checkStrictMode(newSnapshotId, newCommitKind, partitions, rebasedReassignments);
        lastSafeSnapshot = newSnapshotId - 1;
        return partitions;
    }

    public void onCommitSuccess(long snapshotId) {
        if (lastSafeSnapshot != null) {
            lastSafeSnapshot = snapshotId;
        }
    }

    private void checkStrictMode(
            long newSnapshotId,
            CommitKind newCommitKind,
            List<BinaryRow> newChangedPartitions,
            Set<Long> rebasedReassignments) {
        Set<BinaryRow> newPartitions = new HashSet<>(newChangedPartitions);
        for (long id = lastSafeSnapshot + 1; id < newSnapshotId; id++) {
            if ((newCommitKind == CommitKind.COMPACT || newCommitKind == CommitKind.APPEND)
                    && rebasedReassignments.contains(id)) {
                continue;
            }
            Snapshot snapshot = snapshotManager.snapshot(id);
            if (snapshot.commitUser().equals(commitUser)) {
                continue;
            }
            if (snapshot.commitKind() == CommitKind.COMPACT
                    || snapshot.commitKind() == CommitKind.OVERWRITE) {
                boolean hasOverlap = scanner.hasDataChanges(snapshot, newPartitions, false);
                // OVERWRITE may contain logical changes represented only by deletion vectors,
                // while an index-only COMPACT on a data evolution table does not change data.
                if (!hasOverlap
                        && (snapshot.commitKind() == CommitKind.OVERWRITE
                                || !dataEvolutionEnabled)) {
                    hasOverlap = scanner.hasIndexChanges(snapshot, newPartitions, false);
                }
                if (hasOverlap) {
                    throw new RuntimeException(
                            String.format(
                                    "When trying to commit snapshot %d, "
                                            + "commit user %s has found a %s snapshot (id: %d) by another user %s "
                                            + "which modified the same partition. Giving up committing as %s is true.",
                                    newSnapshotId,
                                    commitUser,
                                    snapshot.commitKind().name(),
                                    id,
                                    snapshot.commitUser(),
                                    CoreOptions.COMMIT_STRICT_MODE_ENABLED.key()));
                }
            }
            if (snapshot.commitKind() == CommitKind.APPEND
                    && newCommitKind == CommitKind.OVERWRITE
                    && scanner.hasDataChanges(snapshot, newPartitions, true)) {
                throw new RuntimeException(
                        String.format(
                                "When trying to commit snapshot %d, "
                                        + "commit user %s has found a APPEND snapshot (id: %d) by another user %s "
                                        + "which committed files to fixed bucket on the same partition. "
                                        + "Giving up committing as %s is true.",
                                newSnapshotId,
                                commitUser,
                                id,
                                snapshot.commitUser(),
                                CoreOptions.COMMIT_STRICT_MODE_ENABLED.key()));
            }
        }
    }

    private void checkPartitionCompaction(long newSnapshotId, Map<BinaryRow, Long> baseSnapshots) {
        Map<BinaryRow, Range> ranges = new HashMap<>();
        for (Map.Entry<BinaryRow, Long> entry : baseSnapshots.entrySet()) {
            long base = entry.getValue();
            Range checked = checkedCompactionSnapshots.get(entry.getKey());
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
                        checkedCompactionSnapshots.put(
                                partition.copy(), new Range(range.from, newSnapshotId - 1)));
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
}
