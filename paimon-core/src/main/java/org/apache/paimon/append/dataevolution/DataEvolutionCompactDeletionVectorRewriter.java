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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.append.AppendDeleteFileMaintainer;
import org.apache.paimon.deletionvectors.append.BaseAppendDeleteFileMaintainer;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.DataEvolutionUtils.checkContiguousRowRange;
import static org.apache.paimon.utils.DataEvolutionUtils.retrieveAnchorFile;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Rewrites deletion vectors for non-materialized data-evolution compaction.
 *
 * <p>Compaction tasks rewrite data files without applying deletion vectors. This class consumes all
 * compaction messages before commit, moves old anchor-file DVs to new compacted files, and returns
 * index-only compact messages for the rewritten DV index files.
 *
 * <p>Unlike append compaction, data-evolution compaction does not pack compact tasks by deletion
 * vector index files. Data-evolution compaction is batch-oriented, and packing by DV may create a
 * huge compact task when many files contain only a few deleted rows. This is especially risky when
 * blob compaction is involved. Instead, compact tasks are planned normally and DVs are rewritten
 * centrally from compact commit messages. This is acceptable because most DVs only need to be
 * renamed to the new anchor file and are not fully rewritten.
 */
public class DataEvolutionCompactDeletionVectorRewriter {

    private final FileStoreTable table;

    public DataEvolutionCompactDeletionVectorRewriter(FileStoreTable table) {
        this.table = table;
    }

    public List<CommitMessage> rewriteDeletionVectors(List<CommitMessage> messages) {
        if (!table.coreOptions().deletionVectorsEnabled() || messages.isEmpty()) {
            return Collections.emptyList();
        }

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        Map<BinaryRow, AppendDeleteFileMaintainer> maintainerByParts =
                collectMaintainers(snapshot, messages);
        if (maintainerByParts.isEmpty()) {
            return Collections.emptyList();
        }

        List<CommitMessage> result = new ArrayList<>();
        for (Map.Entry<BinaryRow, AppendDeleteFileMaintainer> entry :
                maintainerByParts.entrySet()) {
            List<IndexManifestEntry> indexChanges = entry.getValue().persist();
            if (!indexChanges.isEmpty()) {
                result.add(toCommitMessage(entry.getKey(), indexChanges));
            }
        }
        return result;
    }

    private Map<BinaryRow, AppendDeleteFileMaintainer> collectMaintainers(
            Snapshot snapshot, List<CommitMessage> messages) {
        Map<BinaryRow, AppendDeleteFileMaintainer> result = new LinkedHashMap<>();
        RangeHelper<DataFileMeta> rangeHelper = new RangeHelper<>(DataFileMeta::nonNullRowIdRange);

        for (CommitMessage message : messages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            CompactIncrement compactIncrement = commitMessage.compactIncrement();
            checkNoCompactIndexChanges(compactIncrement);

            checkState(
                    commitMessage.bucket() == UNAWARE_BUCKET
                            && commitMessage.totalBuckets() == null,
                    "Data evolution compaction should only produce unaware-bucket commit "
                            + "messages, but got bucket %s and total buckets %s.",
                    commitMessage.bucket(),
                    commitMessage.totalBuckets());

            List<DataFileMeta> before = normalFiles(compactIncrement.compactBefore());
            List<DataFileMeta> after = normalFiles(compactIncrement.compactAfter());

            // Skip blob-only tasks and index-only commit messages.
            if (before.isEmpty() && after.isEmpty()) {
                continue;
            }

            AppendDeleteFileMaintainer maintainer =
                    result.computeIfAbsent(
                            commitMessage.partition(),
                            ignored ->
                                    BaseAppendDeleteFileMaintainer.forUnawareAppend(
                                            table.store().newIndexFileHandler(),
                                            snapshot,
                                            commitMessage.partition()));
            if (isMaterialized(compactIncrement)) {
                removeMaterializedDeletionVectors(maintainer, rangeHelper, before);
                continue;
            }

            checkState(
                    after.size() == 1,
                    "Only one normal file should be generated by a single compact task.");
            Range beforeRange = checkContiguousRowRange(before);
            Range afterRange = after.get(0).nonNullRowIdRange();
            checkState(
                    beforeRange.equals(afterRange),
                    "Non-materialized data evolution compaction should keep the same row-id "
                            + "range, but compact before range is %s and compact after range is %s.",
                    beforeRange,
                    afterRange);
            DeletionVector merged = newDeletionVector();

            // Merge all old DeletionVectors, should consider row range offset of each sub dv
            for (List<DataFileMeta> previousRowGroup : rangeHelper.mergeOverlappingRanges(before)) {
                DataFileMeta oldAnchor = retrieveAnchorFile(previousRowGroup, file -> file);
                moveDeletionVector(
                        maintainer,
                        merged,
                        oldAnchor.fileName(),
                        oldAnchor.nonNullRowIdRange(),
                        afterRange);
            }
            if (!merged.isEmpty()) {
                maintainer.notifyNewDeletionVector(after.get(0).fileName(), merged);
            }
        }
        return result;
    }

    private void removeMaterializedDeletionVectors(
            AppendDeleteFileMaintainer maintainer,
            RangeHelper<DataFileMeta> rangeHelper,
            List<DataFileMeta> before) {
        for (List<DataFileMeta> previousRowGroup : rangeHelper.mergeOverlappingRanges(before)) {
            DataFileMeta oldAnchor = retrieveAnchorFile(previousRowGroup, file -> file);
            maintainer.notifyRemovedDeletionVector(oldAnchor.fileName());
        }
    }

    private boolean isMaterialized(CompactIncrement compactIncrement) {
        List<DataFileMeta> before = normalFiles(compactIncrement.compactBefore());
        if (before.isEmpty()) {
            return false;
        }
        return compactIncrement.compactAfter().stream().allMatch(file -> file.firstRowId() == null);
    }

    /**
     * 'Move' the deletion vectors of old anchor files to the new anchor file. This may merge
     * several deletion vectors into one if row-level compaction is triggered,
     */
    private void moveDeletionVector(
            AppendDeleteFileMaintainer maintainer,
            DeletionVector merged,
            String oldFileName,
            Range oldRange,
            Range newRange) {
        DeletionVector old = maintainer.getDeletionVector(oldFileName);
        if (old == null || old.isEmpty()) {
            return;
        }

        // Fast path for renaming deletion vectors only
        if (oldRange.equals(newRange)) {
            merged.merge(old);
        } else {
            old.forEachDeletedPosition(
                    position -> {
                        long absolutePosition = oldRange.from + position;
                        long newPosition = absolutePosition - newRange.from;
                        checkState(
                                newPosition >= 0 && newPosition < newRange.count(),
                                "Cannot move deletion position %s from old range %s to new range %s.",
                                absolutePosition,
                                oldRange,
                                newRange);
                        merged.delete(newPosition);
                    });
        }
        maintainer.notifyRemovedDeletionVector(oldFileName);
    }

    private List<DataFileMeta> normalFiles(List<DataFileMeta> files) {
        return files.stream().filter(this::isNormalFile).collect(Collectors.toList());
    }

    private boolean isNormalFile(DataFileMeta fileMeta) {
        return !isBlobFile(fileMeta.fileName()) && !isVectorStoreFile(fileMeta.fileName());
    }

    private DeletionVector newDeletionVector() {
        return table.coreOptions().deletionVectorBitmap64()
                ? new Bitmap64DeletionVector()
                : new BitmapDeletionVector();
    }

    private CommitMessage toCommitMessage(
            BinaryRow partition, List<IndexManifestEntry> indexChanges) {
        List<IndexFileMeta> newIndexFiles = new ArrayList<>();
        List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
        for (IndexManifestEntry entry : indexChanges) {
            if (entry.kind() == FileKind.ADD) {
                newIndexFiles.add(entry.indexFile());
            } else {
                deletedIndexFiles.add(entry.indexFile());
            }
        }

        return new CommitMessageImpl(
                partition,
                UNAWARE_BUCKET,
                null,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        newIndexFiles,
                        deletedIndexFiles));
    }

    private void checkNoCompactIndexChanges(CompactIncrement compactIncrement) {
        checkState(
                compactIncrement.newIndexFiles().isEmpty()
                        && compactIncrement.deletedIndexFiles().isEmpty(),
                "Data evolution compaction should not produce index changes before deletion vector rewrite, "
                        + "but got new index files %s and deleted index files %s.",
                compactIncrement.newIndexFiles(),
                compactIncrement.deletedIndexFiles());
    }
}
