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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.operation.commit.CommitChangesProvider;
import org.apache.paimon.operation.commit.CommitChangesRewriter;
import org.apache.paimon.operation.commit.ReassignCompactChangesProvider;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.materializedRowIdRanges;

/** Prepares data-evolution compact changes and remaps indexes after row-ID assignment. */
public final class DataEvolutionCommitChangesRewriter implements CommitChangesRewriter {

    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;
    private final SchemaManager schemaManager;
    private final CoreOptions options;
    private final SnapshotManager snapshotManager;
    private final IndexManifestFile indexManifestFile;

    public DataEvolutionCommitChangesRewriter(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotManager snapshotManager,
            IndexManifestFile indexManifestFile) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.schemaManager = schemaManager;
        this.options = options;
        this.snapshotManager = snapshotManager;
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
            Long lastSafeSnapshot = options.commitLastSafeSnapshot().orElse(null);
            if (!checkRowIdsFromSnapshot && lastSafeSnapshot != null && lastSafeSnapshot >= 0) {
                return new ReassignCompactChangesProvider(
                        fileIO, pathFactory, snapshotManager, lastSafeSnapshot, changes);
            }
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

            // This provider runs again after every optimistic-commit conflict, so each attempt
            // includes concurrently created indexes before their final row IDs are assigned.
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

    @Override
    public Attempt createAttempt(CommitKind kind, CommitChanges changes) {
        if (kind != CommitKind.COMPACT) {
            return Attempt.IDENTITY;
        }

        // Side files remain attached to committed data. Only outputs awaiting their first row-ID
        // assignment carry a permutation for this commit; later metadata rewrites must ignore it.
        Set<String> mappedClusteringOutputs =
                changes.tableFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .filter(entry -> entry.file().firstRowId() == null)
                        .filter(
                                entry ->
                                        entry.file()
                                                .extraFiles()
                                                .contains(
                                                        ClusteringRowIdMapping.fileName(
                                                                entry.file().fileName())))
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toSet());
        Set<String> rowIdPreservingOutputs =
                changes.tableFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .filter(entry -> entry.file().firstRowId() != null)
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toSet());
        return (newSnapshotId, assignedFiles, indexFiles, ioManager, registerNewFile) -> {
            if (assignedFiles.stream().noneMatch(entry -> entry.kind() == FileKind.DELETE)) {
                return indexFiles;
            }
            return ClusteringIndexRewriter.rewrite(
                    fileIO,
                    pathFactory,
                    schemaManager,
                    options,
                    ioManager,
                    assignedFiles,
                    indexFiles,
                    mappedClusteringOutputs,
                    rowIdPreservingOutputs,
                    newSnapshotId,
                    registerNewFile);
        };
    }
}
