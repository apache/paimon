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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitChangesProvider;
import org.apache.paimon.operation.commit.CommitChangesRewriter;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.materializedRowIdRanges;

/** Refreshes index changes for operations which replace data-evolution row IDs. */
public abstract class DataEvolutionCommitChangesRewriter implements CommitChangesRewriter {

    private final IndexManifestFile indexManifestFile;

    protected DataEvolutionCommitChangesRewriter(IndexManifestFile indexManifestFile) {
        this.indexManifestFile = indexManifestFile;
    }

    @Override
    public CommitChangesProvider compactChangesProvider(
            CommitChanges changes, List<CommitMessage> messages, boolean checkRowIdsFromSnapshot) {
        Set<Pair<BinaryRow, Integer>> materializedBuckets = new HashSet<>();
        for (CommitMessage message : messages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            if (!materializedRowIdRanges(
                            commitMessage.compactIncrement().compactBefore(),
                            commitMessage.compactIncrement().compactAfter())
                    .isEmpty()) {
                materializedBuckets.add(Pair.of(commitMessage.partition(), commitMessage.bucket()));
            }
        }
        if (materializedBuckets.isEmpty()) {
            return CommitChangesRewriter.super.compactChangesProvider(
                    changes, messages, checkRowIdsFromSnapshot);
        }

        return latestSnapshot -> {
            List<IndexManifestEntry> indexFiles =
                    changes.indexFiles.stream()
                            // Replace global-index deletions prepared against an older snapshot.
                            .filter(
                                    entry ->
                                            entry.kind() != FileKind.DELETE
                                                    || entry.indexFile().globalIndexMeta() == null
                                                    || !materializedBuckets.contains(
                                                            Pair.of(
                                                                    entry.partition(),
                                                                    entry.bucket())))
                            .collect(Collectors.toList());

            // Each optimistic retry must include concurrently created indexes before assigning
            // final row IDs to this operation's outputs.
            if (latestSnapshot != null && latestSnapshot.indexManifest() != null) {
                for (IndexManifestEntry entry :
                        indexManifestFile.read(latestSnapshot.indexManifest())) {
                    if (entry.indexFile().globalIndexMeta() != null
                            && materializedBuckets.contains(
                                    Pair.of(entry.partition(), entry.bucket()))) {
                        indexFiles.add(entry.toDeleteEntry());
                    }
                }
            }

            return new CommitChanges(changes.tableFiles, changes.changelogFiles, indexFiles);
        };
    }
}
