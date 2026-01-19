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

import org.apache.paimon.operation.expire.SnapshotExpireTask.TaskType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.operation.expire.SnapshotExpireTask.TaskType.DELETE_CHANGELOG_FILES;
import static org.apache.paimon.operation.expire.SnapshotExpireTask.TaskType.DELETE_DATA_FILES;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExpireSnapshotsPlan}. */
public class ExpireSnapshotsPlanTest {

    @Test
    public void testPartitionTasksBySnapshotRange() {
        // beginInclusiveId=1, endExclusiveId=11, parallelism=3
        List<SnapshotExpireTask> dataFileTasks = new ArrayList<>();
        for (long id = 2; id <= 11; id++) {
            dataFileTasks.add(SnapshotExpireTask.forDataFiles(id));
        }
        List<SnapshotExpireTask> changelogFileTasks = new ArrayList<>();
        for (long id = 1; id <= 10; id++) {
            changelogFileTasks.add(SnapshotExpireTask.forChangelogFiles(id));
        }

        ExpireSnapshotsPlan plan =
                new ExpireSnapshotsPlan(
                        dataFileTasks,
                        changelogFileTasks,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        1,
                        11);

        List<List<SnapshotExpireTask>> result = plan.partitionTasksBySnapshotRange(3);
        assertThat(result).hasSize(3);

        // Worker 0 (snapshot 1-4): [dataTask(2,3,4), changelogTask(1,2,3,4)]
        assertWorker(
                result.get(0),
                new long[] {2, 3, 4, 1, 2, 3, 4},
                new TaskType[] {
                    DELETE_DATA_FILES,
                    DELETE_DATA_FILES,
                    DELETE_DATA_FILES,
                    DELETE_CHANGELOG_FILES,
                    DELETE_CHANGELOG_FILES,
                    DELETE_CHANGELOG_FILES,
                    DELETE_CHANGELOG_FILES
                });

        // Worker 1 (snapshot 5-8): [dataTask(5,6,7,8), changelogTask(5,6,7,8)]
        assertWorker(
                result.get(1),
                new long[] {5, 6, 7, 8, 5, 6, 7, 8},
                new TaskType[] {
                    DELETE_DATA_FILES, DELETE_DATA_FILES, DELETE_DATA_FILES, DELETE_DATA_FILES,
                    DELETE_CHANGELOG_FILES, DELETE_CHANGELOG_FILES, DELETE_CHANGELOG_FILES,
                            DELETE_CHANGELOG_FILES
                });

        // Worker 2 (snapshot 9-11): [dataTask(9,10,11), changelogTask(9,10)]
        assertWorker(
                result.get(2),
                new long[] {9, 10, 11, 9, 10},
                new TaskType[] {
                    DELETE_DATA_FILES,
                    DELETE_DATA_FILES,
                    DELETE_DATA_FILES,
                    DELETE_CHANGELOG_FILES,
                    DELETE_CHANGELOG_FILES
                });
    }

    private void assertWorker(List<SnapshotExpireTask> tasks, long[] ids, TaskType[] types) {
        assertThat(tasks).extracting(SnapshotExpireTask::snapshotId).containsExactly(box(ids));
        assertThat(tasks).extracting(SnapshotExpireTask::taskType).containsExactly(types);
    }

    @Test
    public void testPartitionTasksWithLargeParallelism() {
        // beginInclusiveId=1, endExclusiveId=4, parallelism=10
        List<SnapshotExpireTask> dataFileTasks = new ArrayList<>();
        for (long id = 2; id <= 4; id++) {
            dataFileTasks.add(SnapshotExpireTask.forDataFiles(id));
        }
        List<SnapshotExpireTask> changelogFileTasks = new ArrayList<>();
        for (long id = 1; id <= 3; id++) {
            changelogFileTasks.add(SnapshotExpireTask.forChangelogFiles(id));
        }

        ExpireSnapshotsPlan plan =
                new ExpireSnapshotsPlan(
                        dataFileTasks,
                        changelogFileTasks,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        1,
                        4);

        List<List<SnapshotExpireTask>> result = plan.partitionTasksBySnapshotRange(10);
        // Parallelism is 10. Total snapshot range involved is [1, 4] (inclusive).
        // ID 1: changelog only. ID 2,3: both. ID 4: data only (since data files are (begin, end]).
        // Total 4 snapshots have tasks.
        // snapshotsPerWorker = ceil(4 / 10) = 1.
        // Workers 0, 1, 2, 3 should get tasks.
        assertThat(result).hasSize(4);

        // Worker 0 (snapshot 1): [changelogTask(1)]
        assertWorker(result.get(0), new long[] {1}, new TaskType[] {DELETE_CHANGELOG_FILES});

        // Worker 1 (snapshot 2): [dataTask(2), changelogTask(2)]
        assertWorker(
                result.get(1),
                new long[] {2, 2},
                new TaskType[] {DELETE_DATA_FILES, DELETE_CHANGELOG_FILES});

        // Worker 2 (snapshot 3): [dataTask(3), changelogTask(3)]
        assertWorker(
                result.get(2),
                new long[] {3, 3},
                new TaskType[] {DELETE_DATA_FILES, DELETE_CHANGELOG_FILES});

        // Worker 3 (snapshot 4): [dataTask(4)]
        assertWorker(result.get(3), new long[] {4}, new TaskType[] {DELETE_DATA_FILES});
    }

    @Test
    public void testPartitionTasksWithSingleSnapshot() {
        // beginInclusiveId=1, endExclusiveId=2, parallelism=2
        // Only snapshot 1 needs expire.
        List<SnapshotExpireTask> dataFileTasks =
                Collections.singletonList(SnapshotExpireTask.forDataFiles(2)); // usually id+1
        List<SnapshotExpireTask> changelogFileTasks =
                Collections.singletonList(SnapshotExpireTask.forChangelogFiles(1));

        ExpireSnapshotsPlan plan =
                new ExpireSnapshotsPlan(
                        dataFileTasks,
                        changelogFileTasks,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        1,
                        2);

        List<List<SnapshotExpireTask>> result = plan.partitionTasksBySnapshotRange(2);
        // total = 2-1 = 1? or +1?
        // If range is [1, 2). Snapshots are {1}. Count = 1.
        // snapshotsPerWorker = 1.
        // Worker 0: [1, 1].
        // Worker 1: [2, 2]. > endExclusiveId?

        assertThat(result).hasSize(2);
        assertWorker(result.get(0), new long[] {1}, new TaskType[] {DELETE_CHANGELOG_FILES});
        assertWorker(result.get(1), new long[] {2}, new TaskType[] {DELETE_DATA_FILES});
    }

    private Long[] box(long[] arr) {
        Long[] result = new Long[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = arr[i];
        }
        return result;
    }
}
