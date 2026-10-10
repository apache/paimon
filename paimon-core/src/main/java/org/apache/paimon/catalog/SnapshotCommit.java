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

package org.apache.paimon.catalog;

import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionStatistics;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Interface to commit snapshot atomically. */
public interface SnapshotCommit extends AutoCloseable {

    /**
     * Begin snapshot preparation and publication, optionally acquiring a commit lock. The returned
     * locked attempt protects head refresh, validation, preparation and publication. An empty
     * result means that another writer currently holds the lock.
     */
    default Optional<CommitAttempt> beginCommit(
            String branch, String commitUser, boolean acquireLock) throws Exception {
        if (acquireLock) {
            throw new UnsupportedOperationException(
                    "This snapshot committer does not support locks.");
        }
        return Optional.of(CommitAttempt.unlocked(this));
    }

    boolean commit(
            @Nullable String baseSnapshotUuid,
            Snapshot snapshot,
            String branch,
            List<PartitionStatistics> statistics)
            throws Exception;
}
