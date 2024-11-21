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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.catalog.CachingCatalog.invalidateMetaCacheForPrefix;
import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Branch}. */
public class BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(BranchManager.class);

    public static final String BRANCH_PREFIX = "branch-";
    public static final String DEFAULT_MAIN_BRANCH = "main";

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;

    public BranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
    }

    /** Return the root Directory of branch. */
    public Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    public static String normalizeBranch(String branch) {
        return StringUtils.isNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    public static boolean isMainBranch(String branch) {
        return branch.equals(DEFAULT_MAIN_BRANCH);
    }

    /** Return the path string of a branch. */
    public static String branchPath(Path tablePath, String branch) {
        return isMainBranch(branch)
                ? tablePath.toString()
                : tablePath.toString() + "/branch/" + BRANCH_PREFIX + branch;
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(branchPath(tablePath, branchName));
    }

    /** Create empty branch. */
    public void createBranch(String branchName) {
        validateBranch(branchName);

        try {
            TableSchema latestSchema = schemaManager.latest().get();
            copySchemasToBranch(branchName, latestSchema.id());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void createBranch(String branchName, String tagName) {
        validateBranch(branchName);
        checkArgument(tagManager.tagExists(tagName), "Tag name '%s' not exists.", tagName);

        Snapshot snapshot = tagManager.taggedSnapshot(tagName);

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
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    public void deleteBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    public boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    public void fastForward(String branchName) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                "Branch name '%s' do not use in fast-forward.",
                branchName);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
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
                            path -> Snapshot.fromPath(fileIO, path).id() >= earliestSnapshotId);

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
            fileIO.copyFiles(
                    tagManager.copyWithBranch(branchName).tagDirectory(),
                    tagManager.tagDirectory(),
                    true);
            invalidateMetaCacheForPrefix(tablePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    /** Get all branches for the table. */
    public List<String> branches() {
        try {
            return listVersionedDirectories(fileIO, branchDirectory(), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateBranch(String branchName) {
        checkArgument(
                !isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);
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
