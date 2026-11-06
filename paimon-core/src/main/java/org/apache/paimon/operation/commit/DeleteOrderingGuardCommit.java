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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Publication contract for capture and replacement of delete ordering guard segments. */
public class DeleteOrderingGuardCommit {

    private DeleteOrderingGuardCommit() {}

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
        return validate(
                latestSnapshot, commitKind, dataChanges, indexChanges, activeIndexFiles, false);
    }

    public static boolean validate(
            @Nullable Snapshot latestSnapshot,
            Snapshot.CommitKind commitKind,
            List<ManifestEntry> dataChanges,
            List<IndexManifestEntry> indexChanges,
            List<IndexManifestEntry> activeIndexFiles,
            boolean rescale) {
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
            if (rescale) {
                checkArgument(
                        commitKind == Snapshot.CommitKind.OVERWRITE && latestSnapshot != null,
                        "Guard rescaling requires an existing snapshot and atomic overwrite.");
                checkArgument(
                        retiredBuckets.stream()
                                .anyMatch(bucket -> bucket.getLeft().equals(entry.partition())),
                        "Guard rescaling must replace committed guards in the same partition.");
                continue;
            }
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
