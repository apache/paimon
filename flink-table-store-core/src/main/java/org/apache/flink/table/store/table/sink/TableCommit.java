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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.annotation.Experimental;
import org.apache.flink.table.store.file.catalog.CatalogLock;
import org.apache.flink.table.store.table.Table;

import java.util.List;
import java.util.Set;

/**
 * Commit of {@link Table} to provide {@link CommitMessage} committing.
 *
 * <ol>
 *   <li>Before calling {@link TableCommit#commit}, if user cannot determine if this commit is done
 *       before, user should first call {@link TableCommit#filterCommitted}.
 *   <li>Before committing, it will first check for conflicts by checking if all files to be removed
 *       currently exists, and if modified files have overlapping key ranges with existing files.
 *   <li>After that it use the external {@link CatalogLock} (if provided) or the atomic rename of
 *       the file system to ensure atomicity.
 *   <li>If commit fails due to conflicts or exception it tries its best to clean up and aborts.
 *   <li>If atomic rename fails it tries again after reading the latest snapshot from step 2.
 * </ol>
 *
 * <p>According to the options, the expiration of snapshots and partitions will be completed in
 * commit.
 *
 * @since 0.4.0
 */
@Experimental
public interface TableCommit extends AutoCloseable {

    /**
     * Default ignore empty commit, if this is set to false, when there is no new data, an empty
     * commit will also be created.
     *
     * <p>NOTE: It is recommended to set 'ignoreEmptyCommit' to false in streaming write, in order
     * to better remove duplicate commits (See {@link #filterCommitted}).
     */
    TableCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    /** Filter committed commits. This method is used for failover cases. */
    Set<Long> filterCommitted(Set<Long> commitIdentifiers);

    /**
     * Create a new commit. One commit may generate two snapshots, one for adding new files and the
     * other for compaction.
     */
    void commit(long commitIdentifier, List<CommitMessage> commitMessages);
}
