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

import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.operation.commit.CommitChanges;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/** Maintains index coverage for deletion-vector materialization. */
public final class MaterializeDeletionVectorsCommitChangesRewriter
        extends DataEvolutionCommitChangesRewriter {

    private final FileStoreTable table;

    public MaterializeDeletionVectorsCommitChangesRewriter(FileStoreTable table) {
        super(table.store().indexManifestFileFactory().create());
        this.table = table;
    }

    @Override
    public Attempt createAttempt(CommitKind kind, CommitChanges changes) {
        if (kind != CommitKind.COMPACT) {
            return Attempt.IDENTITY;
        }
        Set<String> rowIdPreservingOutputs =
                changes.tableFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .filter(entry -> entry.file().firstRowId() != null)
                        .map(entry -> entry.file().fileName())
                        .collect(Collectors.toSet());
        return (newSnapshotId, assignedFiles, indexFiles, ioManager) -> {
            if (assignedFiles.stream().noneMatch(entry -> entry.kind() == FileKind.DELETE)) {
                return indexFiles;
            }
            return DataEvolutionIndexRewriter.rewrite(
                    table.fileIO(),
                    table.store().pathFactory(),
                    table.schemaManager(),
                    table.coreOptions(),
                    ioManager,
                    assignedFiles,
                    indexFiles,
                    Collections.emptySet(),
                    rowIdPreservingOutputs,
                    newSnapshotId);
        };
    }
}
