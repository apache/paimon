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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The plan for snapshot expiration, containing four groups of tasks organized by deletion phase.
 *
 * <p>The plan organizes tasks into four groups for correct deletion order:
 *
 * <ul>
 *   <li>{@link #dataFileTasks()}: Phase 1a - Delete data files (can be parallelized)
 *   <li>{@link #changelogFileTasks()}: Phase 1b - Delete changelog files (can be parallelized)
 *   <li>{@link #manifestTasks()}: Phase 2a - Delete manifest files (serial)
 *   <li>{@link #snapshotFileTasks()}: Phase 2b - Delete snapshot metadata files (serial)
 * </ul>
 */
public class ExpireSnapshotsPlan implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ExpireSnapshotsPlan EMPTY =
            new ExpireSnapshotsPlan(
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    null,
                    0,
                    0);

    private final List<SnapshotExpireTask> dataFileTasks;
    private final List<SnapshotExpireTask> changelogFileTasks;
    private final List<SnapshotExpireTask> manifestTasks;
    private final List<SnapshotExpireTask> snapshotFileTasks;
    private final ProtectionSet protectionSet;
    private final long beginInclusiveId;
    private final long endExclusiveId;

    public ExpireSnapshotsPlan(
            List<SnapshotExpireTask> dataFileTasks,
            List<SnapshotExpireTask> changelogFileTasks,
            List<SnapshotExpireTask> manifestTasks,
            List<SnapshotExpireTask> snapshotFileTasks,
            ProtectionSet protectionSet,
            long beginInclusiveId,
            long endExclusiveId) {
        this.dataFileTasks = dataFileTasks;
        this.changelogFileTasks = changelogFileTasks;
        this.manifestTasks = manifestTasks;
        this.snapshotFileTasks = snapshotFileTasks;
        this.protectionSet = protectionSet;
        this.beginInclusiveId = beginInclusiveId;
        this.endExclusiveId = endExclusiveId;
    }

    public static ExpireSnapshotsPlan empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return dataFileTasks.isEmpty()
                && changelogFileTasks.isEmpty()
                && manifestTasks.isEmpty()
                && snapshotFileTasks.isEmpty();
    }

    /** Get data file deletion tasks (Phase 1a). These can be executed in parallel. */
    public List<SnapshotExpireTask> dataFileTasks() {
        return dataFileTasks;
    }

    /** Get changelog file deletion tasks (Phase 1b). These can be executed in parallel. */
    public List<SnapshotExpireTask> changelogFileTasks() {
        return changelogFileTasks;
    }

    /** Get manifest deletion tasks (Phase 2a). These should be executed serially. */
    public List<SnapshotExpireTask> manifestTasks() {
        return manifestTasks;
    }

    /** Get snapshot file deletion tasks (Phase 2b). These should be executed serially. */
    public List<SnapshotExpireTask> snapshotFileTasks() {
        return snapshotFileTasks;
    }

    /**
     * Partition tasks into groups by snapshot ID range for parallel mode execution.
     *
     * <p>Each group contains tasks for a contiguous range of snapshots, with dataFileTasks first,
     * then changelogFileTasks. This ensures:
     *
     * <ul>
     *   <li>Same snapshot range tasks are processed by the same worker
     *   <li>Within each worker, data files are deleted before changelog files
     * </ul>
     *
     * <p>For example, with beginInclusiveId=1, endExclusiveId=11, parallelism=3:
     *
     * <ul>
     *   <li>Worker 0 (snapshot 1-4): [dataTask(2,3,4), changelogTask(1,2,3,4)]
     *   <li>Worker 1 (snapshot 5-8): [dataTask(5,6,7,8), changelogTask(5,6,7,8)]
     *   <li>Worker 2 (snapshot 9-11): [dataTask(9,10,11), changelogTask(9,10)]
     * </ul>
     *
     * @param parallelism target parallelism for distribution
     * @return list of task groups, one per worker
     */
    public List<List<SnapshotExpireTask>> partitionTasksBySnapshotRange(int parallelism) {
        if (dataFileTasks.isEmpty() && changelogFileTasks.isEmpty()) {
            return Collections.emptyList();
        }

        // Build maps for quick lookup: snapshotId -> task
        Map<Long, SnapshotExpireTask> dataFileTaskMap = new HashMap<>();
        for (SnapshotExpireTask task : dataFileTasks) {
            dataFileTaskMap.put(task.snapshotId(), task);
        }
        Map<Long, SnapshotExpireTask> changelogFileTaskMap = new HashMap<>();
        for (SnapshotExpireTask task : changelogFileTasks) {
            changelogFileTaskMap.put(task.snapshotId(), task);
        }

        // Calculate snapshot ranges for each worker
        // Total snapshot range is [beginInclusiveId, endExclusiveId]
        long totalSnapshots = endExclusiveId - beginInclusiveId + 1;
        int snapshotsPerWorker = (int) ((totalSnapshots + parallelism - 1) / parallelism);

        List<List<SnapshotExpireTask>> result = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            long rangeStart = beginInclusiveId + (long) i * snapshotsPerWorker;
            long rangeEnd = Math.min(rangeStart + snapshotsPerWorker - 1, endExclusiveId);

            if (rangeStart > endExclusiveId) {
                break;
            }

            List<SnapshotExpireTask> workerTasks = new ArrayList<>();

            // First pass: add all dataFileTasks in this range
            for (long id = rangeStart; id <= rangeEnd; id++) {
                SnapshotExpireTask task = dataFileTaskMap.get(id);
                if (task != null) {
                    workerTasks.add(task);
                }
            }

            // Second pass: add all changelogFileTasks in this range
            for (long id = rangeStart; id <= rangeEnd; id++) {
                SnapshotExpireTask task = changelogFileTaskMap.get(id);
                if (task != null) {
                    workerTasks.add(task);
                }
            }

            if (!workerTasks.isEmpty()) {
                result.add(workerTasks);
            }
        }

        return result;
    }

    public ProtectionSet protectionSet() {
        return protectionSet;
    }

    public long beginInclusiveId() {
        return beginInclusiveId;
    }

    public long endExclusiveId() {
        return endExclusiveId;
    }

    @Override
    public String toString() {
        return "ExpireSnapshotsPlan{"
                + "dataFileTasks="
                + dataFileTasks.size()
                + ", changelogFileTasks="
                + changelogFileTasks.size()
                + ", manifestTasks="
                + manifestTasks.size()
                + ", snapshotFileTasks="
                + snapshotFileTasks.size()
                + ", beginInclusiveId="
                + beginInclusiveId
                + ", endExclusiveId="
                + endExclusiveId
                + ", hasProtectionSet="
                + (protectionSet != null)
                + '}';
    }
}
