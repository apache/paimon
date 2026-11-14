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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Publication contract for capture and replacement of delete ordering guard segments. */
public class DeleteOrderingGuardCommit {

    private DeleteOrderingGuardCommit() {}

    /** Recover the committed input set from the writer's delivered compaction state. */
    public static List<String> sources(List<IndexFileMeta> current, CompactIncrement increment) {
        Set<String> sources = guardNames(current);
        sources.removeAll(guardNames(increment.newIndexFiles()));
        sources.addAll(guardNames(increment.deletedIndexFiles()));
        List<String> result = new ArrayList<>(sources);
        Collections.sort(result);
        return result;
    }

    /** Validate both reset generations and compaction inputs on every publication attempt. */
    public static CommitChangesProvider fence(
            CommitChangesProvider delegate,
            List<CommitMessage> messages,
            Function<Snapshot, List<IndexManifestEntry>> indexFiles) {
        return new CommitChangesProvider() {
            @Override
            public CommitChanges provide(@Nullable Snapshot latestSnapshot) {
                DeleteOrderingGuardGeneration.validate(latestSnapshot, messages);
                validateSources(latestSnapshot, messages, indexFiles);
                return delegate.provide(latestSnapshot);
            }

            @Override
            public Set<Long> rebasedReassignments() {
                return delegate.rebasedReassignments();
            }

            @Override
            public boolean resetsDeleteOrderingGuards() {
                return delegate.resetsDeleteOrderingGuards();
            }

            @Nullable
            @Override
            public PartitionPredicate deleteOrderingGuardResetFilter() {
                return delegate.deleteOrderingGuardResetFilter();
            }
        };
    }

    private static void validateSources(
            @Nullable Snapshot latest,
            List<CommitMessage> messages,
            Function<Snapshot, List<IndexManifestEntry>> indexFiles) {
        List<CommitMessage> compactions =
                messages.stream()
                        .filter(
                                message ->
                                        !((CommitMessageImpl) message).compactIncrement().isEmpty())
                        .collect(Collectors.toList());
        if (compactions.isEmpty()) {
            return;
        }
        Map<Pair<BinaryRow, Integer>, Set<String>> active = new HashMap<>();
        if (latest != null
                && (DeleteOrderingGuardFile.mayContainGuards(latest)
                        || compactions.stream()
                                .anyMatch(
                                        message ->
                                                message.deleteOrderingGuardSources() != null
                                                        && !message.deleteOrderingGuardSources()
                                                                .isEmpty()))) {
            for (IndexManifestEntry entry : indexFiles.apply(latest)) {
                if (entry.kind() == FileKind.ADD
                        && DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType())) {
                    active.computeIfAbsent(
                                    Pair.of(entry.partition(), entry.bucket()),
                                    ignored -> new HashSet<>())
                            .add(entry.indexFile().fileName());
                }
            }
        }
        for (CommitMessage message : compactions) {
            List<String> observed = message.deleteOrderingGuardSources();
            Set<String> expected =
                    observed == null ? Collections.emptySet() : new HashSet<>(observed);
            checkState(
                    expected.equals(
                            active.getOrDefault(
                                    Pair.of(message.partition(), message.bucket()),
                                    Collections.emptySet())),
                    "Delete ordering guard sources changed for partition %s, bucket %s. "
                            + "Reload the bucket and recompute compaction before retrying.",
                    message.partition(),
                    message.bucket());
        }
    }

    private static Set<String> guardNames(List<IndexFileMeta> files) {
        return files.stream()
                .filter(file -> DELETE_ORDERING_GUARD.equals(file.indexType()))
                .map(IndexFileMeta::fileName)
                .collect(Collectors.toSet());
    }

    /**
     * Every new guard must replace source data or active guard segments in the same bucket and
     * snapshot. Data deletions require native file conflict checks. Guard-source identity is
     * checked here on every retry, so concurrent consolidation and resets cannot retire another
     * writer's protection. Retirement without a guard replacement requires explicit overwrite.
     *
     * @return whether guard additions require source-file conflict checks
     */
    public static boolean validate(
            @Nullable Snapshot latestSnapshot,
            Snapshot.CommitKind commitKind,
            List<ManifestEntry> dataChanges,
            List<IndexManifestEntry> indexChanges,
            List<IndexManifestEntry> activeIndexFiles) {
        boolean publishesGuards = false;
        Set<Pair<BinaryRow, Integer>> sourceBuckets =
                dataChanges.stream()
                        .filter(data -> data.kind() == FileKind.DELETE)
                        .map(data -> Pair.of(data.partition(), data.bucket()))
                        .collect(Collectors.toSet());
        Set<Pair<BinaryRow, Integer>> replacementBuckets = guardBuckets(indexChanges, FileKind.ADD);
        Set<Pair<BinaryRow, Integer>> retiredBuckets = guardBuckets(indexChanges, FileKind.DELETE);
        Set<IndexManifestEntry> activeSources =
                activeIndexFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .filter(
                                entry ->
                                        DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType()))
                        .map(IndexManifestEntry::toDeleteEntry)
                        .collect(Collectors.toSet());
        for (IndexManifestEntry entry : indexChanges) {
            if (!DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType())) {
                continue;
            }
            if (entry.kind() == FileKind.DELETE) {
                checkArgument(
                        commitKind == Snapshot.CommitKind.OVERWRITE
                                || (commitKind == Snapshot.CommitKind.COMPACT
                                        && replacementBuckets.contains(
                                                Pair.of(entry.partition(), entry.bucket()))),
                        "Delete ordering guard references can only be removed by overwrite/reset "
                                + "or compaction with replacement guards in the same bucket.");
                if (commitKind == Snapshot.CommitKind.COMPACT) {
                    checkState(
                            activeSources.contains(entry),
                            "Delete ordering guard compaction source is no longer active: %s.",
                            entry.indexFile().fileName());
                }
                continue;
            }
            publishesGuards = true;
            checkArgument(
                    commitKind == Snapshot.CommitKind.COMPACT && latestSnapshot != null,
                    "Delete ordering guards must be published by compaction of committed data.");
            checkArgument(
                    sourceBuckets.contains(Pair.of(entry.partition(), entry.bucket()))
                            || retiredBuckets.contains(Pair.of(entry.partition(), entry.bucket())),
                    "Delete ordering guard publication requires source-file replacements in the same bucket "
                            + "or replacement of active guard segments.");
        }
        return publishesGuards;
    }

    private static Set<Pair<BinaryRow, Integer>> guardBuckets(
            List<IndexManifestEntry> entries, FileKind kind) {
        return entries.stream()
                .filter(entry -> entry.kind() == kind)
                .filter(entry -> DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType()))
                .map(entry -> Pair.of(entry.partition(), entry.bucket()))
                .collect(Collectors.toSet());
    }
}
