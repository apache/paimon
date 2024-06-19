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
import java.util.Map;

/**
 * A {@link TableCommit} for stream processing. You can use this class to commit multiple times.
 *
 * @since 0.4.0
 * @see StreamWriteBuilder
 */
@Public
public interface StreamTableCommit extends TableCommit {

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
     * <p>Compared to {@link StreamTableCommit#filterAndCommit}, this method does not check if
     * {@code commitIdentifier} has been committed, so this method might be faster. Please only use
     * this method if you can make sure that the {@code commitIdentifier} hasn't been committed
     * before.
     *
     * @param commitIdentifier Committed transaction ID, can start from 0. If there are multiple
     *     commits, please increment this ID.
     * @param commitMessages commit messages from table write
     * @see StreamTableWrite#prepareCommit
     */
    void commit(long commitIdentifier, List<CommitMessage> commitMessages);

    /**
     * Filter out all {@code List<CommitMessage>} which have been committed and commit the remaining
     * ones.
     *
     * <p>Compared to {@link StreamTableCommit#commit}, this method will first check if a {@code
     * commitIdentifier} has been committed, so this method might be slower. A common usage of this
     * method is to retry the commit process after a failure.
     *
     * @param commitIdentifiersAndMessages a map containing all {@link CommitMessage}s in question.
     *     The key is the {@code commitIdentifier}.
     * @return number of {@code List<CommitMessage>} committed.
     */
    int filterAndCommit(Map<Long, List<CommitMessage>> commitIdentifiersAndMessages);

    default boolean isStreamTableCommit() {
        return true;
    }

    default StreamTableCommit asStreamTableCommit() {
        return this;
    }
}
