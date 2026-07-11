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

package org.apache.paimon.append;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.table.source.DataSplit;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites the {@link CommitMessage}s produced by a sort compact write into compact commit
 * messages, so that the commit is a {@link Snapshot.CommitKind#COMPACT} commit instead of an {@link
 * Snapshot.CommitKind#OVERWRITE} commit.
 *
 * <p>The sort compact write only creates new data files (the sorted output). The old files which
 * are replaced by the sort compact are captured by the planned input {@link DataSplit}s. This
 * helper rewrites them into compact changes: all planned old files become {@code compactBefore} for
 * their original (partition, bucket), and all newly written files become {@code compactAfter} for
 * their output (partition, bucket). This is important for hash-dynamic bucket sort compact, where
 * output files may be assigned to different buckets from the planned input files. The result is a
 * {@link CommitMessageImpl} with an empty {@link DataIncrement} and a populated {@link
 * CompactIncrement}.
 *
 * <p>For deletion-vector enabled append tables, the deletion-vector index entries of the removed
 * old files are cleaned up, mirroring {@link AppendCompactTask}.
 */
public class SortCompactCommitMessageRewriter {

    private static final String ABORT_COMMIT_USER = "sort-compact-abort";

    private final FileStoreTable table;
    private final long baseSnapshotId;

    /** Old files grouped by partition then bucket, captured from the planned input splits. */
    private final Map<BinaryRow, Map<Integer, List<DataFileMeta>>> compactBeforeFiles;

    /**
     * Total bucket counts grouped like {@link #compactBeforeFiles}, when carried by input splits.
     */
    private final Map<BinaryRow, Map<Integer, Integer>> compactBeforeTotalBuckets;

    /**
     * Deletion-vector index entries captured from the base snapshot at planning time, grouped by
     * partition.
     */
    private final Map<BinaryRow, List<IndexManifestEntry>> baseDeletionVectorEntries;

    /**
     * Hash index files captured from the base snapshot at planning time, grouped by partition then
     * bucket.
     */
    private final Map<BinaryRow, Map<Integer, IndexFileMeta>> baseHashIndexes;

    public SortCompactCommitMessageRewriter(
            FileStoreTable table, long baseSnapshotId, List<DataSplit> compactInputSplits) {
        this(table, baseSnapshotId, compactInputSplits, null);
    }

    public SortCompactCommitMessageRewriter(
            FileStoreTable table,
            long baseSnapshotId,
            List<DataSplit> compactInputSplits,
            @Nullable SortCompactPlanMetadata planMetadata) {
        this.table = table;
        this.baseSnapshotId = baseSnapshotId;
        this.compactBeforeFiles = new HashMap<>();
        this.compactBeforeTotalBuckets = new HashMap<>();
        this.baseDeletionVectorEntries = new HashMap<>();
        this.baseHashIndexes = new HashMap<>();
        Set<BinaryRow> partitions = new HashSet<>();
        for (DataSplit split : compactInputSplits) {
            partitions.add(split.partition());
            compactBeforeFiles
                    .computeIfAbsent(split.partition(), k -> new HashMap<>())
                    .computeIfAbsent(split.bucket(), k -> new ArrayList<>())
                    .addAll(split.dataFiles());
            if (split.totalBuckets() != null) {
                Integer previous =
                        compactBeforeTotalBuckets
                                .computeIfAbsent(split.partition(), k -> new HashMap<>())
                                .putIfAbsent(split.bucket(), split.totalBuckets());
                if (previous != null && !previous.equals(split.totalBuckets())) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Conflicting total bucket counts for partition %s bucket %s: "
                                            + "%s and %s.",
                                    split.partition(),
                                    split.bucket(),
                                    previous,
                                    split.totalBuckets()));
                }
            }
        }
        if (planMetadata != null) {
            planMetadata.copyInto(baseDeletionVectorEntries, baseHashIndexes);
        } else {
            SortCompactPlanMetadata.captureInto(
                    table,
                    baseSnapshotId,
                    partitions,
                    compactInputSplits,
                    baseDeletionVectorEntries,
                    baseHashIndexes);
        }
    }

    /**
     * Rewrite the given written append commit messages into compact commit messages.
     *
     * <p>Both the planned input splits and the written messages are grouped by (partition, bucket).
     * Planned input groups are always emitted, even if the sort compact write produces no files for
     * that group. Written output groups which were not present in the planned input are emitted as
     * add-only compact messages.
     *
     * @param writtenMessages commit messages produced by the sort compact write stage (only new
     *     files in {@link DataIncrement})
     * @return rewritten commit messages carrying {@link CompactIncrement}s
     */
    public List<CommitMessage> rewrite(List<CommitMessage> writtenMessages) {
        validateWriteOnlyMessages(writtenMessages);

        // group written messages by (partition, bucket)
        Map<BinaryRow, Map<Integer, List<CommitMessageImpl>>> grouped = new HashMap<>();
        for (CommitMessage written : writtenMessages) {
            CommitMessageImpl impl = (CommitMessageImpl) written;
            grouped.computeIfAbsent(impl.partition(), k -> new HashMap<>())
                    .computeIfAbsent(impl.bucket(), k -> new ArrayList<>())
                    .add(impl);
        }

        List<CommitMessage> result = new ArrayList<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionEntry :
                compactBeforeFiles.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            Map<Integer, List<CommitMessageImpl>> writtenInPartition = grouped.get(partition);
            for (Integer bucket : partitionEntry.getValue().keySet()) {
                List<CommitMessageImpl> group = Collections.emptyList();
                if (writtenInPartition != null) {
                    List<CommitMessageImpl> writtenGroup = writtenInPartition.remove(bucket);
                    if (writtenGroup != null) {
                        group = writtenGroup;
                    }
                }
                result.add(rewriteGroup(partition, bucket, group));
            }
            if (writtenInPartition != null && writtenInPartition.isEmpty()) {
                grouped.remove(partition);
            }
        }

        for (Map.Entry<BinaryRow, Map<Integer, List<CommitMessageImpl>>> partitionEntry :
                grouped.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, List<CommitMessageImpl>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                result.add(rewriteGroup(partition, bucketEntry.getKey(), bucketEntry.getValue()));
            }
        }
        return result;
    }

    /**
     * Validate that the written messages only contain write-only append output. Sort compact must
     * not run inline compaction in the write stage; otherwise compact output would be dropped and
     * orphan files would be left on disk.
     */
    private void validateWriteOnlyMessages(List<CommitMessage> writtenMessages) {
        for (CommitMessage written : writtenMessages) {
            CommitMessageImpl impl = (CommitMessageImpl) written;
            CompactIncrement compactIncrement = impl.compactIncrement();
            if (!compactIncrement.compactBefore().isEmpty()
                    || !compactIncrement.compactAfter().isEmpty()) {
                abortAndFail(
                        writtenMessages,
                        String.format(
                                "Sort compact write produced inline compaction changes for "
                                        + "partition %s bucket %s (compactBefore = %s, "
                                        + "compactAfter = %s). The write stage must run in "
                                        + "write-only mode without waiting for compaction.",
                                impl.partition(),
                                impl.bucket(),
                                compactIncrement.compactBefore(),
                                compactIncrement.compactAfter()));
            }
        }
    }

    /** Abort newly written files when sort compact rewrite or commit fails. */
    public void abortWrittenMessages(List<CommitMessage> writtenMessages) {
        if (writtenMessages.isEmpty()) {
            return;
        }
        try (TableCommit commit = table.newCommit(ABORT_COMMIT_USER)) {
            commit.abort(writtenMessages);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to clean up sort compact write output before aborting commit.", e);
        }
    }

    private void abortAndFail(List<CommitMessage> writtenMessages, String message) {
        abortWrittenMessages(writtenMessages);
        throw new IllegalStateException(message);
    }

    private CommitMessage rewriteGroup(
            BinaryRow partition, int bucket, List<CommitMessageImpl> group) {
        List<DataFileMeta> compactBefore = compactBefore(partition, bucket);

        // merge all newly written sorted files of this (partition, bucket) as compact output
        List<DataFileMeta> compactAfter = new ArrayList<>();
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        Integer totalBuckets = null;
        for (CommitMessageImpl impl : group) {
            compactAfter.addAll(toCompactAfter(impl.newFilesIncrement().newFiles()));
            newIndexFiles.addAll(impl.compactIncrement().newIndexFiles());
            newIndexFiles.addAll(impl.newFilesIncrement().newIndexFiles());
            deletedIndexFiles.addAll(impl.compactIncrement().deletedIndexFiles());
            deletedIndexFiles.addAll(impl.newFilesIncrement().deletedIndexFiles());
            if (totalBuckets == null) {
                totalBuckets = impl.totalBuckets();
            }
        }
        if (totalBuckets == null) {
            totalBuckets = compactBeforeTotalBuckets(partition, bucket);
        }

        // for deletion-vector append tables, clean up the DV index entries of removed old files
        if (table.coreOptions().deletionVectorsEnabled()
                && table.bucketMode() == BucketMode.BUCKET_UNAWARE
                && !compactBefore.isEmpty()) {
            AppendDeleteFileMaintainer dvMaintainer =
                    BaseAppendDeleteFileMaintainer.forUnawareAppend(
                            table.store().newIndexFileHandler(),
                            partition,
                            baseDeletionVectorEntries.getOrDefault(
                                    partition, Collections.emptyList()));
            for (DataFileMeta oldFile : compactBefore) {
                dvMaintainer.notifyRemovedDeletionVector(oldFile.fileName());
            }
            for (IndexManifestEntry entry : dvMaintainer.persist()) {
                if (entry.kind() == FileKind.ADD) {
                    newIndexFiles.add(entry.indexFile());
                } else {
                    deletedIndexFiles.add(entry.indexFile());
                }
            }
        }

        // for hash-dynamic bucket tables, remove the old hash index of compacted buckets so that
        // subsequent writes do not route keys back to stale buckets
        if (table.bucketMode() == BucketMode.HASH_DYNAMIC && !compactBefore.isEmpty()) {
            IndexFileMeta hashIndex =
                    baseHashIndexes
                            .getOrDefault(partition, Collections.emptyMap())
                            .get(bucket);
            if (hashIndex != null) {
                deletedIndexFiles.add(hashIndex);
            }
        }

        CompactIncrement compactIncrement =
                new CompactIncrement(
                        compactBefore,
                        compactAfter,
                        Collections.emptyList(),
                        newIndexFiles,
                        deletedIndexFiles);
        return new CommitMessageImpl(
                partition, bucket, totalBuckets, DataIncrement.emptyIncrement(), compactIncrement);
    }

    private List<DataFileMeta> toCompactAfter(List<DataFileMeta> newFiles) {
        if (newFiles.isEmpty()) {
            return newFiles;
        }
        List<DataFileMeta> result = new ArrayList<>(newFiles.size());
        for (DataFileMeta newFile : newFiles) {
            if (newFile.fileSource().orElse(null) == org.apache.paimon.manifest.FileSource.COMPACT) {
                result.add(newFile);
                continue;
            }
            // The sort compact writer preserves the sequence numbers in the records and computes
            // the range for each output file. In particular, a hash-dynamic compact output bucket
            // can contain records from several input buckets, so its range must not be inferred
            // from the old files of the target bucket.
            result.add(newFile.assignFileSource(org.apache.paimon.manifest.FileSource.COMPACT));
        }
        return result;
    }

    private List<DataFileMeta> compactBefore(BinaryRow partition, int bucket) {
        return compactBeforeFiles
                .getOrDefault(partition, Collections.emptyMap())
                .getOrDefault(bucket, Collections.emptyList());
    }

    @Nullable
    private Integer compactBeforeTotalBuckets(BinaryRow partition, int bucket) {
        return compactBeforeTotalBuckets
                .getOrDefault(partition, Collections.emptyMap())
                .get(bucket);
    }

    /** Latest snapshot id, or 0 when the table has no snapshot yet. */
    public long latestSnapshotIdOrZero() {
        Long latestId = table.snapshotManager().latestSnapshotId();
        return latestId == null ? 0L : latestId;
    }

    /**
     * Whether the given compact commit messages were already committed after {@code
     * snapshotIdBeforeCommit}.
     *
     * <p>Used to avoid aborting sort compact write output when {@link
     * org.apache.paimon.table.sink.TableCommitImpl} fails after the snapshot is already visible.
     * Matching is based on the compact output file set (or removed input files for delete-only
     * commits), not on any unrelated batch COMPACT snapshot.
     */
    public boolean isBatchCompactCommitSucceeded(
            long snapshotIdBeforeCommit, List<CommitMessage> compactMessages) {
        Long latestId = table.snapshotManager().latestSnapshotId();
        if (latestId == null || latestId <= snapshotIdBeforeCommit) {
            return false;
        }

        CompactCommitFingerprint fingerprint = CompactCommitFingerprint.from(compactMessages);
        Snapshot latestSnapshot = table.snapshotManager().snapshot(latestId);
        if (matchesCompactCommit(latestSnapshot, fingerprint)) {
            return true;
        }

        // Check older snapshots in reverse order. The compact commit is usually near latest, and
        // scoped scans below avoid full-table manifest reads when probing history.
        for (long id = latestId - 1; id > snapshotIdBeforeCommit; id--) {
            Snapshot snapshot;
            try {
                snapshot = table.snapshotManager().tryGetSnapshot(id);
            } catch (FileNotFoundException e) {
                // Expired snapshots create gaps. Keep scanning older snapshots instead of treating
                // the commit as failed.
                continue;
            }
            if (matchesCompactCommit(snapshot, fingerprint)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesCompactCommit(Snapshot snapshot, CompactCommitFingerprint fingerprint) {
        if (!fingerprint.compactAfterFileNames.isEmpty()) {
            // Output file names are unique. Match them in any surviving snapshot so that a
            // successful compact is still detected after its COMPACT snapshot expires.
            return snapshotContainsAllFiles(
                    snapshot, fingerprint, fingerprint.compactAfterFileNames);
        }
        if (snapshot.commitIdentifier() != BatchWriteBuilder.COMMIT_IDENTIFIER
                || snapshot.commitKind() != Snapshot.CommitKind.COMPACT) {
            return false;
        }
        if (!fingerprint.compactBeforeFileNames.isEmpty()) {
            return !snapshotContainsAnyFile(
                    snapshot, fingerprint, fingerprint.compactBeforeFileNames);
        }
        return true;
    }

    private boolean snapshotContainsAllFiles(
            Snapshot snapshot, CompactCommitFingerprint fingerprint, Set<String> fileNames) {
        if (fileNames.isEmpty()) {
            return true;
        }
        Set<String> found = new HashSet<>();
        for (ManifestEntry entry :
                createScopedScan(snapshot, fingerprint, fileNames).plan().files()) {
            found.add(entry.file().fileName());
            if (found.size() == fileNames.size()) {
                return true;
            }
        }
        return false;
    }

    private boolean snapshotContainsAnyFile(
            Snapshot snapshot, CompactCommitFingerprint fingerprint, Set<String> fileNames) {
        if (fileNames.isEmpty()) {
            return false;
        }
        for (ManifestEntry entry :
                createScopedScan(snapshot, fingerprint, fileNames).plan().files()) {
            if (fileNames.contains(entry.file().fileName())) {
                return true;
            }
        }
        return false;
    }

    private FileStoreScan createScopedScan(
            Snapshot snapshot, CompactCommitFingerprint fingerprint, Set<String> fileNames) {
        FileStoreScan scan = table.store().newScan().withSnapshot(snapshot).dropStats();
        if (!fingerprint.partitions.isEmpty()) {
            scan = scan.withPartitionFilter(fingerprint.partitions);
        }
        if (!fingerprint.buckets.isEmpty()) {
            scan = scan.withBucketFilter(fingerprint.buckets::contains);
        }
        if (!fileNames.isEmpty()) {
            scan = scan.withDataFileNameFilter(fileNames::contains);
        }
        return scan;
    }

    private static final class CompactCommitFingerprint {
        private final Set<String> compactAfterFileNames;
        private final Set<String> compactBeforeFileNames;
        private final List<BinaryRow> partitions;
        private final Set<Integer> buckets;

        private CompactCommitFingerprint(
                Set<String> compactAfterFileNames,
                Set<String> compactBeforeFileNames,
                List<BinaryRow> partitions,
                Set<Integer> buckets) {
            this.compactAfterFileNames = compactAfterFileNames;
            this.compactBeforeFileNames = compactBeforeFileNames;
            this.partitions = partitions;
            this.buckets = buckets;
        }

        private static CompactCommitFingerprint from(List<CommitMessage> compactMessages) {
            Set<String> compactAfterFileNames = new HashSet<>();
            Set<String> compactBeforeFileNames = new HashSet<>();
            Set<BinaryRow> partitionSet = new HashSet<>();
            Set<Integer> buckets = new HashSet<>();
            for (CommitMessage message : compactMessages) {
                CommitMessageImpl impl = (CommitMessageImpl) message;
                partitionSet.add(impl.partition());
                buckets.add(impl.bucket());
                for (DataFileMeta file : impl.compactIncrement().compactAfter()) {
                    compactAfterFileNames.add(file.fileName());
                }
                for (DataFileMeta file : impl.compactIncrement().compactBefore()) {
                    compactBeforeFileNames.add(file.fileName());
                }
            }
            return new CompactCommitFingerprint(
                    compactAfterFileNames,
                    compactBeforeFileNames,
                    new ArrayList<>(partitionSet),
                    buckets);
        }
    }

    /** Whether all planned compact-before files are already absent from the latest snapshot. */
    public boolean isPlannedInputAlreadyCommitted() {
        if (!hasInput()) {
            return true;
        }
        Long latestId = table.snapshotManager().latestSnapshotId();
        if (latestId == null) {
            return false;
        }
        Snapshot snapshot = table.snapshotManager().snapshot(latestId);
        Set<String> beforeFileNames = new HashSet<>();
        Set<BinaryRow> partitionSet = new HashSet<>();
        Set<Integer> buckets = new HashSet<>();
        for (Map.Entry<BinaryRow, Map<Integer, List<DataFileMeta>>> partitionEntry :
                compactBeforeFiles.entrySet()) {
            partitionSet.add(partitionEntry.getKey());
            for (Map.Entry<Integer, List<DataFileMeta>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                buckets.add(bucketEntry.getKey());
                for (DataFileMeta file : bucketEntry.getValue()) {
                    beforeFileNames.add(file.fileName());
                }
            }
        }
        CompactCommitFingerprint fingerprint =
                new CompactCommitFingerprint(
                        Collections.emptySet(),
                        beforeFileNames,
                        new ArrayList<>(partitionSet),
                        buckets);
        return !snapshotContainsAnyFile(snapshot, fingerprint, beforeFileNames);
    }

    /** Whether this rewriter has captured any old files to compact. */
    public boolean hasInput() {
        return !compactBeforeFiles.isEmpty();
    }
}
