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

package org.apache.paimon.clone;

import org.apache.paimon.Changelog;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Validates the published output of a full-history clone. */
public class FullHistoryCloneValidator {

    private final FileStoreTable sourceTable;
    private final FileStoreTable targetTable;
    private final PathMapping pathMapping;
    private final FullHistoryCopyPlan payloadPlan;

    public FullHistoryCloneValidator(
            FileStoreTable sourceTable,
            FileStoreTable targetTable,
            PathMapping pathMapping,
            FullHistoryCopyPlan payloadPlan) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.pathMapping = pathMapping;
        this.payloadPlan = payloadPlan;
    }

    public ValidationResult validate() throws Exception {
        validateMetadata();
        validateAllTimeTravel();

        FullHistoryFileSet sourceFiles = new FullHistoryFileCollector(sourceTable).collect();
        FullHistoryFileSet targetFiles = new FullHistoryFileCollector(targetTable).collect();
        checkState(
                mappedPaths(sourceFiles.dataFiles()).equals(targetFiles.dataFiles()),
                "Target data files do not match the mapped source data files.");
        checkState(
                mappedPaths(sourceFiles.indexFiles()).equals(targetFiles.indexFiles()),
                "Target index files do not match the mapped source index files.");

        validatePayloadFiles(targetFiles);
        validateAllFilesExist(targetFiles);

        long payloadBytes =
                payloadPlan.files().stream()
                        .mapToLong(FullHistoryCopyPlan.FileCopy::expectedSize)
                        .sum();
        return new ValidationResult(
                payloadPlan.files().size(), payloadBytes, targetFiles.metadataFiles().size());
    }

    public void validateMetadata() throws Exception {
        validateBranchesAndRoots();
    }

    private void validateBranchesAndRoots() throws Exception {
        Set<String> sourceBranches = new HashSet<>(sourceTable.branchManager().branches());
        Set<String> targetBranches = new HashSet<>(targetTable.branchManager().branches());
        checkState(
                sourceBranches.equals(targetBranches),
                "Target branches %s do not match source branches %s.",
                targetBranches,
                sourceBranches);

        List<String> branches = new ArrayList<>(sourceBranches);
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            FileStoreTable source = sourceTable.switchToBranch(branch);
            FileStoreTable target = targetTable.switchToBranch(branch);
            checkState(
                    source.schemaManager().listAllIds().equals(target.schemaManager().listAllIds()),
                    "Target schema IDs in branch %s do not match the source.",
                    branch);
            checkState(
                    snapshotIds(source).equals(snapshotIds(target)),
                    "Target snapshot IDs in branch %s do not match the source.",
                    branch);
            checkState(
                    tagIdentities(source).equals(tagIdentities(target)),
                    "Target tags in branch %s do not match the source.",
                    branch);
            checkState(
                    changelogIds(source).equals(changelogIds(target)),
                    "Target changelog IDs in branch %s do not match the source.",
                    branch);
        }
    }

    private void validateAllTimeTravel() throws IOException {
        List<String> branches = new ArrayList<>(targetTable.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        for (String branch : branches) {
            validateTimeTravel(targetTable.switchToBranch(branch));
        }
    }

    private void validateTimeTravel(FileStoreTable target) throws IOException {
        for (Snapshot snapshot : target.snapshotManager().safelyGetAllSnapshots()) {
            target.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_SNAPSHOT_ID.key(),
                                    String.valueOf(snapshot.id())))
                    .newScan()
                    .plan();
        }
        for (Pair<Tag, String> tagAndName : target.tagManager().tagObjects()) {
            target.copy(
                            Collections.singletonMap(
                                    CoreOptions.SCAN_TAG_NAME.key(), tagAndName.getRight()))
                    .newScan()
                    .plan();
        }
    }

    private Set<Long> snapshotIds(FileStoreTable table) throws IOException {
        return table.snapshotManager().safelyGetAllSnapshots().stream()
                .map(Snapshot::id)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Map<String, TagIdentity> tagIdentities(FileStoreTable table) throws IOException {
        Map<String, TagIdentity> tags = new HashMap<>();
        for (Pair<Tag, String> tagAndName : table.tagManager().tagObjects()) {
            Tag tag = tagAndName.getLeft();
            Object createTime = tag.getTagCreateTime();
            if (createTime == null) {
                createTime =
                        DateTimeUtils.toLocalDateTime(
                                table.fileIO()
                                        .getFileStatus(
                                                table.tagManager().tagPath(tagAndName.getRight()))
                                        .getModificationTime());
            }
            tags.put(tagAndName.getRight(), new TagIdentity(tag, createTime));
        }
        return tags;
    }

    private Set<Long> changelogIds(FileStoreTable table) throws IOException {
        return table.changelogManager().safelyGetAllChangelogs().stream()
                .map(Changelog::id)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Set<Path> mappedPaths(Set<Path> sourcePaths) {
        return sourcePaths.stream()
                .map(path -> new Path(pathMapping.rewriteRequired(path.toString())))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void validatePayloadFiles(FullHistoryFileSet targetFiles) throws IOException {
        Set<Path> plannedTargets =
                payloadPlan.files().stream()
                        .map(FullHistoryCopyPlan.FileCopy::target)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        checkState(
                plannedTargets.equals(targetFiles.payloadFiles()),
                "Target reachable payload files do not match the copy plan.");

        for (FullHistoryCopyPlan.FileCopy file : payloadPlan.files()) {
            checkState(
                    targetTable.fileIO().exists(file.target()),
                    "Target payload file does not exist: %s",
                    file.target());
            long actualSize = targetTable.fileIO().getFileSize(file.target());
            checkState(
                    actualSize == file.expectedSize(),
                    "Target payload file %s has size %s but expected %s.",
                    file.target(),
                    actualSize,
                    file.expectedSize());
        }
    }

    private void validateAllFilesExist(FullHistoryFileSet targetFiles) throws IOException {
        for (Path file : targetFiles.allFiles()) {
            checkState(targetTable.fileIO().exists(file), "Target file does not exist: %s", file);
        }
    }

    /** Summary of a successful validation. */
    public static class ValidationResult implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int payloadFileCount;
        private final long payloadBytes;
        private final int metadataFileCount;

        private ValidationResult(int payloadFileCount, long payloadBytes, int metadataFileCount) {
            this.payloadFileCount = payloadFileCount;
            this.payloadBytes = payloadBytes;
            this.metadataFileCount = metadataFileCount;
        }

        public int payloadFileCount() {
            return payloadFileCount;
        }

        public long payloadBytes() {
            return payloadBytes;
        }

        public int metadataFileCount() {
            return metadataFileCount;
        }
    }

    private static class TagIdentity {

        private final long snapshotId;
        private final long schemaId;
        private final Object createTime;
        private final Object retainedTime;

        private TagIdentity(Tag tag, Object createTime) {
            this.snapshotId = tag.id();
            this.schemaId = tag.schemaId();
            this.createTime = createTime;
            this.retainedTime = tag.getTagTimeRetained();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof TagIdentity)) {
                return false;
            }
            TagIdentity that = (TagIdentity) object;
            return snapshotId == that.snapshotId
                    && schemaId == that.schemaId
                    && java.util.Objects.equals(createTime, that.createTime)
                    && java.util.Objects.equals(retainedTime, that.retainedTime);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(snapshotId, schemaId, createTime, retainedTime);
        }
    }
}
