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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.PartitionBucketMapping;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Restore for write to restore data files by partition and bucket from file system. */
public interface WriteRestore {

    long latestCommittedIdentifier(String user);

    RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex,
            boolean scanSourceIndexPayloads);

    /**
     * Resolves the {@code totalBuckets} for a (partition, bucket) pair given the manifest entries
     * for that bucket and the table's partition-bucket mapping.
     *
     * <ul>
     *   <li>Non-empty bucket: use the value stamped on the existing data files so that
     *       committer-side bucket-count mismatch detection (e.g. rescale-without-overwrite) still
     *       fires.
     *   <li>Empty bucket on a partitioned table: look up the per-partition override in {@code
     *       mapping}; returns {@code null} if the partition uses the table default.
     *   <li>Empty bucket on an unpartitioned table: returns {@code null} so the write path falls
     *       back to {@code numBuckets} and the committer-side check still fires.
     * </ul>
     */
    @Nullable
    static Integer extractTotalBuckets(
            List<ManifestEntry> entries, BinaryRow partition, PartitionBucketMapping mapping) {
        if (!entries.isEmpty()) {
            return entries.get(0).totalBuckets();
        }
        if (partition.getFieldCount() > 0) {
            return mapping.resolveNumBuckets(partition);
        }
        return null;
    }

    /**
     * Extracts the {@link DataFileMeta} list from the given manifest entries, validating that all
     * entries agree on {@code totalBuckets}.
     *
     * @param entries manifest entries for a single (partition, bucket) pair
     * @return the list of data files; empty if {@code entries} is empty
     * @throws RuntimeException if entries carry inconsistent {@code totalBuckets} values, which
     *     indicates a corrupted manifest
     */
    static List<DataFileMeta> extractDataFiles(List<ManifestEntry> entries) {
        Integer totalBuckets = null;
        List<DataFileMeta> dataFiles = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (totalBuckets != null && totalBuckets != entry.totalBuckets()) {
                throw new RuntimeException(
                        String.format(
                                "Bucket data files has different total bucket number, %s vs %s, this should be a bug.",
                                totalBuckets, entry.totalBuckets()));
            }
            totalBuckets = entry.totalBuckets();
            dataFiles.add(entry.file());
        }
        return dataFiles;
    }
}
