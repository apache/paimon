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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;

import java.util.List;

/**
 * A {@link TableCommit} for batch processing. Recommended for one-time committing.
 *
 * @since 0.4.0
 */
@Public
public interface BatchTableCommit extends TableCommit {

    /**
     * Create a new commit. One commit may generate up to two snapshots, one for adding new files
     * and the other for compaction. There will be some expiration policies after commit:
     *
     * <p>1. Snapshot expiration may occur according to three options:
     *
     * <ul>
     *   <li>'snapshot.time-retained': The maximum time of completed snapshots to retain.
     *   <li>'snapshot.num-retained.min': The minimum number of completed snapshots to retain.
     *   <li>'snapshot.num-retained.max': The maximum number of completed snapshots to retain.
     * </ul>
     *
     * <p>2. Partition expiration may occur according to 'partition.expiration-time'. The partition
     * check is expensive, so all partitions are not checked every time when invoking this method.
     * The check frequency is controlled by 'partition.expiration-check-interval'. Partition
     * expiration will create an 'OVERWRITE' snapshot.
     *
     * @param commitMessages commit messages from table write
     */
    void commit(List<CommitMessage> commitMessages);

    /**
     * Truncate table, like normal {@link #commit}, files are not immediately deleted, they are only
     * logically deleted and will be deleted after the snapshot expires.
     */
    void truncateTable();
}
