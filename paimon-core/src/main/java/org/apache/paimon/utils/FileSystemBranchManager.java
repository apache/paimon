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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link BranchManager} implementation to manage branches via file system. */
public class FileSystemBranchManager implements BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemBranchManager.class);

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;
    private final BranchMergeHandler mergeHandler;

    public FileSystemBranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager,
            BranchMergeHandler mergeHandler) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
        this.mergeHandler = mergeHandler;
    }

    /** Return the root Directory of branch. */
    private Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(BranchManager.branchPath(tablePath, branchName));
    }

    @Override
    public void createBranch(String branchName) {
        createBranch(branchName, false);
    }

    @Override
    public void createBranch(String branchName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        try {
            TableSchema latestSchema = schemaManager.latest().get();
            copySchemasToBranch(branchName, latestSchema.id());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        createBranch(branchName, tagName, false);
    }

    @Override
    public void createBranch(String branchName, String tagName, boolean ignoreIfExists) {
        if (ignoreIfExists && branchExists(branchName)) {
            return;
        }
        validateBranch(branchName);
        Snapshot snapshot = tagManager.getOrThrow(tagName).trimToSnapshot();

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFile(
                    tagManager.tagPath(tagName),
                    tagManager.copyWithBranch(branchName).tagPath(tagName),
                    true);
            fileIO.copyFile(
                    snapshotManager.snapshotPath(snapshot.id()),
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id()),
                    true);
            copySchemasToBranch(branchName, snapshot.schemaId());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void dropBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    private boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    @Override
    public void fastForward(String branchName) {
        BranchManager.fastForwardValidate(branchName, snapshotManager.branch());
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
        if (earliestSnapshotId == null) {
            throw new RuntimeException(
                    "Cannot fast forward branch "
                            + branchName
                            + ", because it does not have snapshot.");
        }
        Snapshot earliestSnapshot =
                snapshotManager.copyWithBranch(branchName).snapshot(earliestSnapshotId);
        long earliestSchemaId = earliestSnapshot.schemaId();

        try {
            // Delete snapshot, schema, and tag from the main branch which occurs after
            // earliestSnapshotId
            List<Path> deleteSnapshotPaths =
                    snapshotManager.snapshotPaths(id -> id >= earliestSnapshotId);
            List<Path> deleteSchemaPaths = schemaManager.schemaPaths(id -> id >= earliestSchemaId);
            List<Path> deleteTagPaths =
                    tagManager.tagPaths(
                            path -> Tag.fromPath(fileIO, path).id() >= earliestSnapshotId);

            List<Path> deletePaths =
                    Stream.of(deleteSnapshotPaths, deleteSchemaPaths, deleteTagPaths)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());

            // Delete latest snapshot hint
            snapshotManager.deleteLatestHint();

            fileIO.deleteFilesQuietly(deletePaths);
            fileIO.copyFiles(
                    snapshotManager.copyWithBranch(branchName).snapshotDirectory(),
                    snapshotManager.snapshotDirectory(),
                    true);
            fileIO.copyFiles(
                    schemaManager.copyWithBranch(branchName).schemaDirectory(),
                    schemaManager.schemaDirectory(),
                    true);
            // Continue fast-forward even without tags.
            Path branchTagDirectory = tagManager.copyWithBranch(branchName).tagDirectory();
            if (fileIO.exists(branchTagDirectory)) {
                fileIO.copyFiles(branchTagDirectory, tagManager.tagDirectory(), true);
            }
            snapshotManager.invalidateCache();
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void mergeBranch(String sourceBranch, String targetBranch) {
        BranchManager.mergeValidate(sourceBranch, targetBranch);
        validateMergeBranches(sourceBranch, targetBranch);
        validateAppendOnlyHistory(sourceBranch, targetBranch);
        validateAppendOnly(sourceBranch, targetBranch);
        validateNoDataEvolution(sourceBranch, targetBranch);
        validateRowTrackingConsistent(sourceBranch, targetBranch);
        validateLatestSchema(sourceBranch, targetBranch);

        List<ManifestEntry> filesToMerge = computeMergeDiff(sourceBranch, targetBranch);
        if (filesToMerge.isEmpty()) {
            return;
        }

        validateMergeFileSchemas(sourceBranch, targetBranch, filesToMerge);
        mergeHandler.commit(targetBranch, filesToMerge);
    }

    private void validateMergeBranches(String sourceBranch, String targetBranch) {
        if (!BranchManager.isMainBranch(sourceBranch)) {
            checkArgument(branchExists(sourceBranch), "Branch '%s' doesn't exist.", sourceBranch);
        }
        if (!BranchManager.isMainBranch(targetBranch)) {
            checkArgument(branchExists(targetBranch), "Branch '%s' doesn't exist.", targetBranch);
        }

        SnapshotManager sourceSm = snapshotManager.copyWithBranch(sourceBranch);
        checkArgument(
                sourceSm.latestSnapshotId() != null,
                "Cannot merge branch '%s', because it does not have snapshot.",
                sourceBranch);

        SnapshotManager targetSm = snapshotManager.copyWithBranch(targetBranch);
        checkArgument(
                targetSm.latestSnapshotId() != null,
                "Cannot merge into branch '%s', because it does not have snapshot.",
                targetBranch);
    }

    private void validateLatestSchema(String sourceBranch, String targetBranch) {
        SchemaManager sourceSchemaMgr = new SchemaManager(fileIO, tablePath, sourceBranch);
        SchemaManager targetSchemaMgr = new SchemaManager(fileIO, tablePath, targetBranch);
        TableSchema sourceSchema = sourceSchemaMgr.latest().get();
        TableSchema targetSchema = targetSchemaMgr.latest().get();
        checkArgument(
                sourceSchema.fields().equals(targetSchema.fields()),
                "Cannot merge branch '%s' into '%s', schema mismatch.",
                sourceBranch,
                targetBranch);
    }

    private void validateMergeFileSchemas(
            String sourceBranch, String targetBranch, List<ManifestEntry> filesToMerge) {
        SchemaManager sourceSchemaMgr = new SchemaManager(fileIO, tablePath, sourceBranch);
        SchemaManager targetSchemaMgr = new SchemaManager(fileIO, tablePath, targetBranch);

        for (Long schemaId :
                filesToMerge.stream()
                        .map(entry -> entry.file().schemaId())
                        .collect(Collectors.toSet())) {
            checkArgument(
                    targetSchemaMgr.schemaExists(schemaId),
                    "Cannot merge branch '%s' into '%s', because target branch does not contain "
                            + "schema id %s required by source files.",
                    sourceBranch,
                    targetBranch,
                    schemaId);

            TableSchema sourceSchema = sourceSchemaMgr.schema(schemaId);
            TableSchema targetSchema = targetSchemaMgr.schema(schemaId);
            checkArgument(
                    sourceSchema.equals(targetSchema),
                    "Cannot merge branch '%s' into '%s', schema history mismatch for schema id %s.",
                    sourceBranch,
                    targetBranch,
                    schemaId);
        }
    }

    private void validateAppendOnly(String sourceBranch, String targetBranch) {
        SchemaManager sourceSchemaMgr = new SchemaManager(fileIO, tablePath, sourceBranch);
        TableSchema sourceSchema = sourceSchemaMgr.latest().get();
        checkArgument(
                sourceSchema.primaryKeys().isEmpty(),
                "Branch merge is only supported for append-only tables, "
                        + "but branch '%s' has primary keys.",
                sourceBranch);

        SchemaManager targetSchemaMgr = new SchemaManager(fileIO, tablePath, targetBranch);
        TableSchema targetSchema = targetSchemaMgr.latest().get();
        checkArgument(
                targetSchema.primaryKeys().isEmpty(),
                "Branch merge is only supported for append-only tables, "
                        + "but branch '%s' has primary keys.",
                targetBranch);
    }

    // Branch merge is implemented as a conservative file-level merge. Without persisted branch
    // lineage metadata, we cannot reliably infer a fork point after snapshots expire. To preserve
    // correctness, both branches must retain complete append-only history from the first snapshot.
    // This restriction can be relaxed in the future if branch fork metadata is introduced.
    private void validateAppendOnlyHistory(String sourceBranch, String targetBranch) {
        validateCompleteAppendOnly(snapshotManager.copyWithBranch(sourceBranch), sourceBranch);
        validateCompleteAppendOnly(snapshotManager.copyWithBranch(targetBranch), targetBranch);
    }

    private void validateCompleteAppendOnly(SnapshotManager sm, String branch) {
        Long earliest = sm.earliestSnapshotId();
        Long latest = sm.latestSnapshotId();
        if (earliest == null || latest == null) {
            return;
        }
        if (earliest != Snapshot.FIRST_SNAPSHOT_ID) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot merge: branch '%s' does not start at snapshot %d "
                                    + "(earliest is %d). "
                                    + "Branch merge requires complete append-only snapshot history.",
                            branch, Snapshot.FIRST_SNAPSHOT_ID, earliest));
        }
        for (long id = Snapshot.FIRST_SNAPSHOT_ID; id <= latest; id++) {
            if (!sm.snapshotExists(id)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge: snapshot %d is missing on branch '%s'. "
                                        + "Branch merge requires complete append-only snapshot history.",
                                id, branch));
            }
            Snapshot snapshot = sm.snapshot(id);
            Snapshot.CommitKind kind = snapshot.commitKind();
            if (kind != Snapshot.CommitKind.APPEND && kind != Snapshot.CommitKind.ANALYZE) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge: snapshot %d on branch '%s' has commit kind '%s'. "
                                        + "Branch merge requires complete append-only snapshot history.",
                                id, branch, kind));
            }
        }
    }

    private void validateNoDataEvolution(String sourceBranch, String targetBranch) {
        for (String branch : new String[] {sourceBranch, targetBranch}) {
            SchemaManager sm = new SchemaManager(fileIO, tablePath, branch);
            TableSchema schema = sm.latest().get();
            CoreOptions opts = new CoreOptions(schema.options());
            checkArgument(
                    !opts.dataEvolutionEnabled(),
                    "Branch merge is not supported for data-evolution tables (branch '%s').",
                    branch);
        }
    }

    private void validateRowTrackingConsistent(String sourceBranch, String targetBranch) {
        boolean sourceEnabled = isRowTrackingEnabled(sourceBranch);
        boolean targetEnabled = isRowTrackingEnabled(targetBranch);
        checkArgument(
                sourceEnabled == targetEnabled,
                "Cannot merge branch '%s' into '%s': row-tracking settings must match "
                        + "(source=%s, target=%s).",
                sourceBranch,
                targetBranch,
                sourceEnabled,
                targetEnabled);
    }

    private boolean isRowTrackingEnabled(String branch) {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath, branch);
        TableSchema schema = schemaManager.latest().get();
        return new CoreOptions(schema.options()).rowTrackingEnabled();
    }

    private List<ManifestEntry> computeMergeDiff(String sourceBranch, String targetBranch) {
        Set<FileEntry.Identifier> targetFileIds =
                mergeHandler.readBranchFiles(targetBranch).keySet();

        Map<FileEntry.Identifier, ManifestEntry> sourceFiles =
                mergeHandler.readBranchFiles(sourceBranch);

        List<ManifestEntry> filesToMerge = new ArrayList<>();
        for (Map.Entry<FileEntry.Identifier, ManifestEntry> entry : sourceFiles.entrySet()) {
            if (!targetFileIds.contains(entry.getKey())) {
                ManifestEntry manifestEntry = entry.getValue();
                if (manifestEntry.kind() == FileKind.ADD) {
                    filesToMerge.add(manifestEntry);
                }
            }
        }
        return filesToMerge;
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    public void validateBranch(String branchName) {
        BranchManager.validateBranch(branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
    }

    @Override
    public void renameBranch(String fromBranch, String toBranch) {
        checkArgument(!BranchManager.isMainBranch(fromBranch), "Cannot rename the main branch.");
        checkArgument(branchExists(fromBranch), "Branch name '%s' doesn't exist.", fromBranch);
        checkArgument(!branchExists(toBranch), "Branch name '%s' already exists.", toBranch);
        BranchManager.validateBranch(toBranch);

        try {
            // Use rename for atomic operation and better performance
            fileIO.rename(branchPath(fromBranch), branchPath(toBranch));
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when rename branch from '%s' to '%s'.",
                            fromBranch, toBranch),
                    e);
        }
    }

    @Override
    public List<String> branches() {
        try {
            return listVersionedDirectories(fileIO, branchDirectory(), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copySchemasToBranch(String branchName, long schemaId) throws IOException {
        for (int i = 0; i <= schemaId; i++) {
            if (schemaManager.schemaExists(i)) {
                fileIO.copyFile(
                        schemaManager.toSchemaPath(i),
                        schemaManager.copyWithBranch(branchName).toSchemaPath(i),
                        true);
            }
        }
    }
}
