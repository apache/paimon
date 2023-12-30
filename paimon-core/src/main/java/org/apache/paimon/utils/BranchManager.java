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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.paimon.schema.SchemaManager.SCHEMA_PREFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;
import static org.apache.paimon.utils.TagManager.TAG_PREFIX;

/** Manager for {@code Branch}. */
public class BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(BranchManager.class);

    private static final String BRANCH_PREFIX = "branch-";

    private final FileIO fileIO;
    private final Path tablePath;

    public BranchManager(FileIO fileIO, Path path) {
        this.fileIO = fileIO;
        this.tablePath = path;
    }

    /** Return the root Directory of branch. */
    public Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the path of a branch. */
    public String getBranchPath(String branchName) {
        return tablePath + "/branch/" + BRANCH_PREFIX + branchName;
    }

    public Path branchPath(String branchName) {
        return new Path(getBranchPath(branchName));
    }

    public Path tagPath(String tagName) {
        return new Path(tablePath + "/tag/" + TAG_PREFIX + tagName);
    }

    public Path branchTagPath(String branchName, String tagName) {
        return new Path(getBranchPath(branchName) + "/tag/" + TAG_PREFIX + tagName);
    }

    public Path snapshotPath(long snapshotId) {
        return new Path(tablePath + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path branchSnapshotPath(String branchName, long snapshotId) {
        return new Path(getBranchPath(branchName) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    public Path schemaPath(long schemaId) {
        return new Path(tablePath + "/schema/" + SCHEMA_PREFIX + schemaId);
    }

    public Path branchSchemaPath(String branchName, long schemaId) {
        return new Path(getBranchPath(branchName) + "/schema/" + SCHEMA_PREFIX + schemaId);
    }

    public void createBranch(String branchName, String tagName) {
        checkArgument(!StringUtils.isBlank(branchName), "Branch name '%s' is blank.", branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
        checkArgument(tagExists(tagName), "Tag name '%s' not exists.", tagName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);

        Snapshot snapshot = taggedSnapshot(tagName);

        try {
            fileIO.copyFileUtf8(tagPath(tagName), branchTagPath(branchName, tagName));
            fileIO.copyFileUtf8(
                    snapshotPath(snapshot.id()), branchSnapshotPath(branchName, snapshot.id()));
            fileIO.copyFileUtf8(
                    schemaPath(snapshot.schemaId()),
                    branchSchemaPath(branchName, snapshot.schemaId()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            getBranchPath(branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    public boolean fileExists(Path path) {
        try {
            if (fileIO.exists(path)) {
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    /** Check if a tag exists. */
    public boolean tagExists(String tagName) {
        Path tagPath = tagPath(tagName);
        return fileExists(tagPath);
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    /** Get the tagged snapshot by name. */
    public Snapshot taggedSnapshot(String tagName) {
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);
        return Snapshot.fromPath(fileIO, tagPath(tagName));
    }
}
