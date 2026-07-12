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

package org.apache.paimon.flink.sink;

import org.apache.paimon.append.SortCompactCommitMessageRewriter;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link StoreCommitter} for the sort compact topology. It rewrites the append {@link
 * CommitMessage}s produced by the sort compact write stage into compact commit messages (via {@link
 * SortCompactCommitMessageRewriter}) before delegating to the normal {@link StoreCommitter} commit,
 * so that the resulting snapshot is a {@code COMPACT} snapshot instead of an {@code OVERWRITE}
 * snapshot.
 */
public class SortCompactCommitter extends StoreCommitter {

    private final SortCompactCommitMessageRewriter rewriter;

    public SortCompactCommitter(
            FileStoreTable table,
            TableCommit commit,
            Context context,
            SortCompactCommitMessageRewriter rewriter) {
        super(table, commit, context);
        this.rewriter = rewriter;
    }

    @Override
    protected long additionalBytesOut(CommitMessageImpl impl) {
        return calcTotalFileSize(impl.compactIncrement().compactAfter());
    }

    @Override
    protected long additionalRecordsOut(CommitMessageImpl impl) {
        return calcTotalFileRowCount(impl.compactIncrement().compactAfter());
    }

    @Override
    public void commit(List<ManifestCommittable> committables)
            throws java.io.IOException, InterruptedException {
        super.commit(rewriteAll(committables));
    }

    @Override
    public int filterAndCommit(
            List<ManifestCommittable> globalCommittables,
            boolean checkAppendFiles,
            boolean partitionMarkDoneRecoverFromState) {
        List<ManifestCommittable> sortedCommittables =
                globalCommittables.stream()
                        .sorted(Comparator.comparingLong(ManifestCommittable::identifier))
                        .collect(Collectors.toList());
        List<ManifestCommittable> retryCommittables = commit.filterCommitted(sortedCommittables);
        if (retryCommittables.isEmpty()) {
            // Delete-only compact commits are only valid at job end (filterAndCommit with
            // checkAppendFiles=false, e.g. CommitterOperator#endInput). Recovery paths call
            // filterAndCommit with checkAppendFiles=true and an empty restored list; treating
            // that as delete-only would remove all planned input files before writers run.
            if (!checkAppendFiles
                    && sortedCommittables.isEmpty()
                    && rewriter.hasInput()
                    && !rewriter.isPlannedInputAlreadyCommitted()) {
                return filterAndCommitDeleteOnly(
                        globalCommittables, checkAppendFiles, partitionMarkDoneRecoverFromState);
            }
            commitListeners.notifyCommittable(
                    globalCommittables, partitionMarkDoneRecoverFromState);
            return 0;
        }

        List<ManifestCommittable> rewritten = rewriteAll(retryCommittables);
        int committed = commit.filterAndCommitMultiple(rewritten, checkAppendFiles);
        calcNumBytesAndRecordsOut(rewritten);
        commitListeners.notifyCommittable(globalCommittables, partitionMarkDoneRecoverFromState);
        return committed;
    }

    private int filterAndCommitDeleteOnly(
            List<ManifestCommittable> globalCommittables,
            boolean checkAppendFiles,
            boolean partitionMarkDoneRecoverFromState) {
        List<ManifestCommittable> rewritten = rewriteAll(Collections.emptyList());
        if (rewritten.isEmpty()) {
            return 0;
        }
        int committed = commit.filterAndCommitMultiple(rewritten, checkAppendFiles);
        calcNumBytesAndRecordsOut(rewritten);
        commitListeners.notifyCommittable(globalCommittables, partitionMarkDoneRecoverFromState);
        return committed;
    }

    /**
     * Merge all partial write outputs from multiple committables and rewrite once. Each individual
     * rewrite would attach the full planned {@code compactBefore}, so committing partial outputs
     * separately would delete all old files too early.
     *
     * <p>This also collapses multiple committables into a single one with the maximum identifier,
     * which changes the per-identifier deduplication semantics used during job recovery. Sort
     * compact is a batch job and does not rely on that recovery path, so this is acceptable here.
     */
    private List<ManifestCommittable> rewriteAll(List<ManifestCommittable> committables) {
        if (committables.isEmpty()) {
            if (!rewriter.hasInput()) {
                return committables;
            }

            // A sort compact is a batch job. Even when all input rows are filtered out (for
            // example, by deletion vectors), its planned input files still need to be removed.
            return Collections.singletonList(
                    new ManifestCommittable(
                            Long.MAX_VALUE,
                            null,
                            rewriter.rewrite(Collections.emptyList()),
                            Collections.emptyMap()));
        }

        List<CommitMessage> allWrittenMessages = new ArrayList<>();
        long identifier = Long.MIN_VALUE;
        Long watermark = null;
        Map<String, String> properties = new HashMap<>();

        for (ManifestCommittable committable : committables) {
            allWrittenMessages.addAll(committable.fileCommittables());
            identifier = Math.max(identifier, committable.identifier());
            if (committable.watermark() != null) {
                watermark =
                        watermark == null
                                ? committable.watermark()
                                : Math.max(watermark, committable.watermark());
            }
            properties.putAll(committable.properties());
        }

        List<CommitMessage> compactMessages = rewriter.rewrite(allWrittenMessages);
        return Collections.singletonList(
                new ManifestCommittable(identifier, watermark, compactMessages, properties));
    }
}
