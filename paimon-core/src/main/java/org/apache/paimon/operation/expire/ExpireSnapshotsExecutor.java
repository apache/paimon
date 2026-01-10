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

package org.apache.paimon.operation.expire;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Executor for snapshot expire tasks. This class handles the actual deletion logic based on the
 * task type defined by {@link SnapshotExpireTask.TaskType}.
 *
 * <p>The executor uses switch-based dispatch on task type:
 *
 * <ul>
 *   <li>{@link SnapshotExpireTask.TaskType#DELETE_DATA_FILES} → delete data files
 *   <li>{@link SnapshotExpireTask.TaskType#DELETE_CHANGELOG_FILES} → delete changelog files
 *   <li>{@link SnapshotExpireTask.TaskType#DELETE_MANIFESTS} → delete manifest files
 *   <li>{@link SnapshotExpireTask.TaskType#DELETE_SNAPSHOT} → delete snapshot metadata file
 * </ul>
 */
public class ExpireSnapshotsExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsExecutor.class);

    private final SnapshotManager snapshotManager;
    private final SnapshotDeletion snapshotDeletion;
    @Nullable private final ChangelogManager changelogManager;

    public ExpireSnapshotsExecutor(
            SnapshotManager snapshotManager, SnapshotDeletion snapshotDeletion) {
        this(snapshotManager, snapshotDeletion, null);
    }

    public ExpireSnapshotsExecutor(
            SnapshotManager snapshotManager,
            SnapshotDeletion snapshotDeletion,
            @Nullable ChangelogManager changelogManager) {
        this.snapshotManager = snapshotManager;
        this.snapshotDeletion = snapshotDeletion;
        this.changelogManager = changelogManager;
    }

    /**
     * Execute a snapshot expire task based on its task type.
     *
     * @param task the task to execute
     * @param taggedSnapshots taggedSnapshots set for data file deletion (required for
     *     DELETE_DATA_FILES)
     * @param skippingSet manifest skipping set (required for DELETE_MANIFESTS)
     * @return deletion report with execution results
     */
    public DeletionReport execute(
            SnapshotExpireTask task,
            @Nullable List<Snapshot> taggedSnapshots,
            @Nullable Set<String> skippingSet) {

        Snapshot snapshot;
        try {
            snapshot = snapshotManager.tryGetSnapshot(task.snapshotId());
        } catch (FileNotFoundException e) {
            LOG.warn("Snapshot {} not found, skipping task", task.snapshotId());
            return DeletionReport.skipped(task.snapshotId());
        }

        switch (task.taskType()) {
            case DELETE_DATA_FILES:
                return executeDeleteDataFiles(task, snapshot, taggedSnapshots);
            case DELETE_CHANGELOG_FILES:
                return executeDeleteChangelogFiles(task, snapshot);
            case DELETE_MANIFESTS:
                return executeDeleteManifests(task, snapshot, skippingSet);
            case DELETE_SNAPSHOT:
                return executeDeleteSnapshot(task, snapshot);
            default:
                throw new IllegalArgumentException("Unknown task type: " + task.taskType());
        }
    }

    private DeletionReport executeDeleteDataFiles(
            SnapshotExpireTask task, Snapshot snapshot, List<Snapshot> taggedSnapshots) {
        checkNotNull(taggedSnapshots);

        // expire merge tree files and collect changed buckets
        Predicate<ExpireFileEntry> skipper = null;
        try {
            skipper =
                    snapshotDeletion.createDataFileSkipperForTags(
                            taggedSnapshots, task.snapshotId());
        } catch (Exception e) {
            LOG.warn(
                    String.format(
                            "Skip cleaning data files of snapshot '%s' due to failed to build skipping set.",
                            task.snapshotId()),
                    e);
            return DeletionReport.skipped(task.snapshotId());
        }

        if (skipper != null) {
            snapshotDeletion.cleanUnusedDataFiles(snapshot, skipper);
        }
        DeletionReport report = new DeletionReport(task.snapshotId());
        report.setDataFilesDeleted(true);

        return report;
    }

    private DeletionReport executeDeleteChangelogFiles(SnapshotExpireTask task, Snapshot snapshot) {

        DeletionReport report = new DeletionReport(task.snapshotId());

        if (snapshot.changelogManifestList() != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Ready to delete changelog files from snapshot #" + task.snapshotId());
            }
            snapshotDeletion.deleteAddedDataFiles(snapshot.changelogManifestList());
            report.setChangelogDeleted(true);
        }

        return report;
    }

    private DeletionReport executeDeleteManifests(
            SnapshotExpireTask task, Snapshot snapshot, Set<String> skippingSet) {
        checkNotNull(skippingSet);

        DeletionReport report = new DeletionReport(task.snapshotId());

        snapshotDeletion.cleanUnusedManifests(snapshot, skippingSet);
        report.setManifestsDeleted(true);

        return report;
    }

    private DeletionReport executeDeleteSnapshot(SnapshotExpireTask task, Snapshot snapshot) {
        DeletionReport report = new DeletionReport(task.snapshotId());

        // 1. Commit changelog before deleting snapshot
        if (task.isChangelogDecoupled() && changelogManager != null) {
            commitChangelog(new Changelog(snapshot));
        }

        // 2. Delete snapshot metadata file
        snapshotManager.deleteSnapshot(snapshot.id());
        report.setSnapshotDeleted(true);

        return report;
    }

    // ==================== Utility Methods ====================
    public Map<BinaryRow, Set<Integer>> drainDeletionBuckets() {
        return snapshotDeletion.drainDeletionBuckets();
    }

    /**
     * Clean empty directories using externally aggregated deletion buckets.
     *
     * <p>Used in parallel sink mode where buckets are aggregated from multiple workers.
     *
     * @param aggregatedBuckets merged deletion buckets from all workers
     */
    public void cleanEmptyDirectories(Map<BinaryRow, Set<Integer>> aggregatedBuckets) {
        snapshotDeletion.cleanEmptyDirectories(aggregatedBuckets);
    }

    /** Clean empty directories after data files have been deleted. */
    public void cleanEmptyDirectories() {
        snapshotDeletion.cleanEmptyDirectories();
    }

    /**
     * Commit a changelog when changelog is decoupled from snapshot.
     *
     * @param changelog the changelog to commit
     */
    public void commitChangelog(Changelog changelog) {
        try {
            changelogManager.commitChangelog(changelog, changelog.id());
            changelogManager.commitLongLivedChangelogLatestHint(changelog.id());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Write the earliest snapshot hint file.
     *
     * @param earliest the earliest snapshot ID to write
     */
    public void writeEarliestHint(long earliest) {
        try {
            snapshotManager.commitEarliestHint(earliest);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
