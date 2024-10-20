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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.List;
import java.util.Map;

/** Commit operation which provides commit and overwrite. */
public interface FileStoreCommit extends AutoCloseable {

    /** With global lock. */
    FileStoreCommit withLock(Lock lock);

    FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire);

    /** Find out which committables need to be retried when recovering from the failure. */
    List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables);

    /** Commit from manifest committable. */
    void commit(ManifestCommittable committable, Map<String, String> properties);

    /** Commit from manifest committable with checkAppendFiles. */
    void commit(
            ManifestCommittable committable,
            Map<String, String> properties,
            boolean checkAppendFiles);

    /**
     * Overwrite from manifest committable and partition.
     *
     * @param partition A single partition maps each partition key to a partition value. Depending
     *     on the user-defined statement, the partition might not include all partition keys. Also
     *     note that this partition does not necessarily equal to the partitions of the newly added
     *     key-values. This is just the partition to be cleaned up.
     */
    void overwrite(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties);

    /**
     * Drop multiple partitions. The {@link Snapshot.CommitKind} of generated snapshot is {@link
     * Snapshot.CommitKind#OVERWRITE}.
     *
     * @param partitions A list of partition {@link Map}s. NOTE: cannot be empty!
     */
    void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier);

    void truncateTable(long commitIdentifier);

    /** Compact the manifest entries only. */
    void compactManifest();

    /** Abort an unsuccessful commit. The data files will be deleted. */
    void abort(List<CommitMessage> commitMessages);

    /** With metrics to measure commits. */
    FileStoreCommit withMetrics(CommitMetrics metrics);

    /**
     * Commit new statistics. The {@link Snapshot.CommitKind} of generated snapshot is {@link
     * Snapshot.CommitKind#ANALYZE}.
     */
    void commitStatistics(Statistics stats, long commitIdentifier);

    FileStorePathFactory pathFactory();

    FileIO fileIO();

    @Override
    void close();
}
