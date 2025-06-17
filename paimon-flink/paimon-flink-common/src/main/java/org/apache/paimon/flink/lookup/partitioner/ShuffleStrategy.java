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

package org.apache.paimon.flink.lookup.partitioner;

import java.io.Serializable;
import java.util.Set;

/**
 * Shuffle strategy use to assign bucket to subtask and get all the required buckets for a given
 * task.
 */
public interface ShuffleStrategy extends Serializable {

    /**
     * Calculates and returns the subtask ID that a given bucket should be assigned to.
     *
     * @param bucketId The ID of the bucket.
     * @param joinKeyHash The hash value of the join key.
     * @param numSubtasks The total number of subtasks.
     * @return The subtask ID that the bucket should be assigned to.
     */
    int getTargetSubtaskId(int bucketId, int joinKeyHash, int numSubtasks);

    /**
     * Returns all the bucket IDs required for a given subtask.
     *
     * @param subtaskId The ID of the subtask.
     * @param numSubtasks The total number of subtasks.
     * @return A set containing all the bucket IDs required for the subtask.
     */
    Set<Integer> getRequiredCacheBucketIds(int subtaskId, int numSubtasks);
}
