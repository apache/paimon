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

import java.io.Serializable;

/**
 * A task representing a single snapshot expiration operation.
 *
 * <p>Each task has a specific {@link TaskType} that determines what to delete:
 *
 * <ul>
 *   <li>{@link TaskType#DELETE_DATA_FILES}: Delete data files
 *   <li>{@link TaskType#DELETE_CHANGELOG_FILES}: Delete changelog files
 *   <li>{@link TaskType#DELETE_MANIFESTS}: Delete manifest files
 *   <li>{@link TaskType#DELETE_SNAPSHOT}: Delete snapshot metadata file
 * </ul>
 *
 * <p>This design ensures correct deletion order in the execution layer:
 *
 * <ul>
 *   <li>Phase 1: All data file tasks are executed first
 *   <li>Phase 2a: All manifest tasks are executed
 *   <li>Phase 2b: All snapshot tasks are executed last
 * </ul>
 */
public class SnapshotExpireTask implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The type of deletion operation this task performs. */
    public enum TaskType {
        /** Delete data files only. Used for both expiring snapshots and boundary snapshot. */
        DELETE_DATA_FILES,

        /** Delete changelog files. Separate phase for changelog deletion with different range. */
        DELETE_CHANGELOG_FILES,

        /** Delete manifest files. Second phase for all expiring snapshots. */
        DELETE_MANIFESTS,

        /** Delete snapshot metadata file. Final phase for all expiring snapshots. */
        DELETE_SNAPSHOT
    }

    private final long snapshotId;
    private final TaskType taskType;
    private final boolean changelogDecoupled;

    public SnapshotExpireTask(long snapshotId, TaskType taskType, boolean changelogDecoupled) {
        this.snapshotId = snapshotId;
        this.taskType = taskType;
        this.changelogDecoupled = changelogDecoupled;
    }

    /**
     * Create a task for deleting data files.
     *
     * @param snapshotId the snapshot ID
     * @return a data file deletion task
     */
    public static SnapshotExpireTask forDataFiles(long snapshotId) {
        return new SnapshotExpireTask(snapshotId, TaskType.DELETE_DATA_FILES, false);
    }

    /**
     * Create a task for deleting changelog files.
     *
     * @param snapshotId the snapshot ID
     * @return a changelog file deletion task
     */
    public static SnapshotExpireTask forChangelogFiles(long snapshotId) {
        return new SnapshotExpireTask(snapshotId, TaskType.DELETE_CHANGELOG_FILES, false);
    }

    /**
     * Create a task for deleting manifests.
     *
     * @param snapshotId the snapshot ID
     * @return a manifest deletion task
     */
    public static SnapshotExpireTask forManifests(long snapshotId) {
        return new SnapshotExpireTask(snapshotId, TaskType.DELETE_MANIFESTS, false);
    }

    /**
     * Create a task for deleting snapshot metadata file.
     *
     * @param snapshotId the snapshot ID
     * @param changelogDecoupled whether changelog is decoupled from snapshot
     * @return a snapshot deletion task
     */
    public static SnapshotExpireTask forSnapshot(long snapshotId, boolean changelogDecoupled) {
        return new SnapshotExpireTask(snapshotId, TaskType.DELETE_SNAPSHOT, changelogDecoupled);
    }

    public long snapshotId() {
        return snapshotId;
    }

    public TaskType taskType() {
        return taskType;
    }

    public boolean isChangelogDecoupled() {
        return changelogDecoupled;
    }

    @Override
    public String toString() {
        return "SnapshotExpireTask{"
                + "snapshotId="
                + snapshotId
                + ", taskType="
                + taskType
                + ", changelogDecoupled="
                + changelogDecoupled
                + '}';
    }
}
